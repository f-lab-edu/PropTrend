from datetime import date, timedelta

from sqlalchemy import Date, Interval, cast, func, literal, select, true
from sqlalchemy.ext.asyncio import AsyncSession

from ..exceptions import MarketSnapshotNotReadyError
from ..model.market import MarketPriceMover, MarketSnapshot
from ..model.prop_transaction import PropertyType, RentTransaction, SaleTransaction
from ..schemas.market import (
    DailySummaryGroup,
    DailySummaryResponse,
    PriceMover,
    PriceMoversResponse,
    VolumeSurgeRegion,
    VolumeSurgeRegionsResponse,
)
from ..schemas.prop_transaction import SalePriceTrendPoint, SalePropTransactionResponse
from .prop_transaction import (
    build_sale_response,
    get_complex_ids,
    get_region_names,
)

# 실거래는 계약 후 30일 안에 신고되므로 신고 기한이 지난 날짜만 집계하도록 이만큼 늦춘다.
REPORT_DELAY_MONTHS = 1
VOLUME_SURGE_LIMIT = 5


async def get_daily_summary(session: AsyncSession, *, base_date: date) -> DailySummaryResponse:
    """신고 기한만큼 앞선 계약일 하루의 아파트·단독다가구별 매매 최고가·최저가 거래와 거래건수를 조회한다."""
    # 기준일이 월말이어도 깨지지 않도록 PostgreSQL interval 연산으로 구한다(10/31의 1개월 전은 9/30).
    deal_date = (
        await session.execute(
            select(cast(literal(base_date, Date) - func.make_interval(0, REPORT_DELAY_MONTHS, type_=Interval), Date))
        )
    ).scalar_one()

    rows = {}
    for property_type in (PropertyType.APT, PropertyType.SINGLE_MULTI):
        # 해제된 거래는 실제로 성사되지 않은 가격이라 최고가·최저가와 건수에서 모두 뺀다.
        sale_conditions = [
            SaleTransaction.property_type == property_type,
            SaleTransaction.deal_date == deal_date,
            SaleTransaction.cancel_deal_type.is_distinct_from("O"),
        ]
        # 금액이 같으면 응답이 요청마다 달라지지 않도록 id가 작은 거래를 고른다.
        highest = await session.scalar(
            select(SaleTransaction)
            .where(*sale_conditions)
            .order_by(SaleTransaction.deal_amount.desc(), SaleTransaction.id)
            .limit(1)
        )
        lowest = await session.scalar(
            select(SaleTransaction)
            .where(*sale_conditions)
            .order_by(SaleTransaction.deal_amount, SaleTransaction.id)
            .limit(1)
        )

        sale_count = select(func.count()).select_from(SaleTransaction).where(*sale_conditions).scalar_subquery()
        rent_count = (
            select(func.count())
            .select_from(RentTransaction)
            .where(RentTransaction.property_type == property_type, RentTransaction.deal_date == deal_date)
            .scalar_subquery()
        )
        rows[property_type] = (highest, lowest, await session.scalar(select(sale_count + rent_count)))

    names = await get_region_names(session)
    sales = [sale for highest, lowest, _ in rows.values() for sale in (highest, lowest) if sale is not None]
    complex_ids = await get_complex_ids(session, transactions=sales)
    groups = {
        property_type: DailySummaryGroup(
            highest_sale=None if highest is None else build_sale_response(highest, names, complex_ids),
            lowest_sale=None if lowest is None else build_sale_response(lowest, names, complex_ids),
            transaction_count=transaction_count,
        )
        for property_type, (highest, lowest, transaction_count) in rows.items()
    }
    return DailySummaryResponse(
        deal_date=deal_date,
        apartment=groups[PropertyType.APT],
        single_multi=groups[PropertyType.SINGLE_MULTI],
    )


async def get_price_movers(session: AsyncSession) -> PriceMoversResponse:
    """정기 갱신이 계산해 둔 아파트 거래액 급등·급락 단지를 조회한다."""
    # 기준일과 결과 행을 한 문장으로 읽는다. 두 번 나눠 읽으면 그 사이에 갱신이 커밋돼 기준일과 행이 어긋날 수 있다.
    # 결과가 0건인 날에도 기준일은 받아야 하므로 결과 행을 바깥 조인한다.
    rows = (
        await session.execute(
            select(MarketSnapshot.base_date, MarketPriceMover)
            .outerjoin(MarketPriceMover, true())
            .where(MarketSnapshot.name == MarketPriceMover.__tablename__)
        )
    ).all()
    if not rows:
        raise MarketSnapshotNotReadyError("거래액 급등/급락 단지가 아직 집계되지 않았습니다")

    names = await get_region_names(session)
    movers = [
        PriceMover(
            change_rate=row.change_rate,
            latest_sale=SalePropTransactionResponse(
                id=row.latest_id,
                property_type=PropertyType.APT,
                complex_id=row.complex_id,
                region_name=names.get((row.sido_code, row.sigungu_code)),
                umd_name=row.umd_name,
                jibun=row.jibun,
                deal_date=row.deal_date,
                deal_amount=row.deal_amount,
                dealing_type=row.dealing_type,
                cancel_deal_date=row.cancel_deal_date,
                building_name=row.building_name,
                apartment_dong=row.apartment_dong,
                floor=row.floor,
                build_year=row.build_year,
                exclusive_use_area=row.exclusive_use_area,
            ),
            previous_sale=SalePriceTrendPoint(
                id=row.previous_id,
                deal_date=row.previous_deal_date,
                deal_amount=row.previous_deal_amount,
                floor=row.previous_floor,
            ),
        )
        for _, row in rows
        if row is not None
    ]
    # 결과 테이블은 순서를 보장하지 않으므로 순위 순서로 정렬한다. 변동률이 같으면 최근 거래의 id가 작은 단지가 먼저다.
    return PriceMoversResponse(
        base_date=rows[0].base_date,
        surge=sorted(
            (mover for mover in movers if mover.change_rate > 0),
            key=lambda mover: (-mover.change_rate, mover.latest_sale.id),
        ),
        plunge=sorted(
            (mover for mover in movers if mover.change_rate < 0),
            key=lambda mover: (mover.change_rate, mover.latest_sale.id),
        ),
    )


async def get_volume_surge_regions(session: AsyncSession, *, base_date: date) -> VolumeSurgeRegionsResponse:
    """최근 1개월 매매 건수가 이전 1개월보다 가장 많이 늘어난 시군구를 조회한다."""
    # 기준일이 월말이어도 깨지지 않도록 월 단위 경계는 PostgreSQL interval 연산으로 구한다.
    # 경계는 응답에도 실으므로 집계 전에 날짜로 받아 둔다. 최근 구간의 끝부터 1개월씩 거슬러 올라간다.
    base = literal(base_date, Date)
    months_before = [REPORT_DELAY_MONTHS + offset for offset in range(3)]
    boundaries = [cast(base - func.make_interval(0, months, type_=Interval), Date) for months in months_before]
    recent_end, recent_start, previous_start = (await session.execute(select(*boundaries))).one()

    recent_count = func.count().filter(SaleTransaction.deal_date >= recent_start)
    previous_count = func.count().filter(SaleTransaction.deal_date < recent_start)
    count_change = (recent_count - previous_count).label("count_change")
    rows = await session.execute(
        select(
            SaleTransaction.sido_code,
            SaleTransaction.sigungu_code,
            recent_count.label("recent_count"),
            previous_count.label("previous_count"),
            count_change,
        )
        .where(
            SaleTransaction.deal_date >= previous_start,
            SaleTransaction.deal_date < recent_end,
            # 해제된 거래는 실제로 성사되지 않았으므로 세지 않는다.
            SaleTransaction.cancel_deal_type.is_distinct_from("O"),
        )
        .group_by(SaleTransaction.sido_code, SaleTransaction.sigungu_code)
        .having(recent_count - previous_count > 0)
        # 증가 폭이 같으면 응답이 요청마다 달라지지 않도록 지역코드 순으로 둔다.
        .order_by(count_change.desc(), SaleTransaction.sido_code, SaleTransaction.sigungu_code)
        .limit(VOLUME_SURGE_LIMIT)
    )

    names = await get_region_names(session)
    return VolumeSurgeRegionsResponse(
        base_date=base_date,
        recent_start_date=recent_start,
        recent_end_date=recent_end - timedelta(days=1),
        previous_start_date=previous_start,
        previous_end_date=recent_start - timedelta(days=1),
        regions=[
            VolumeSurgeRegion(
                region_code=row.sido_code + row.sigungu_code,
                region_name=names.get((row.sido_code, row.sigungu_code)),
                recent_count=row.recent_count,
                previous_count=row.previous_count,
                count_change=row.count_change,
            )
            for row in rows
        ],
    )
