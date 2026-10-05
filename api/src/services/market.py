from datetime import date, timedelta

from sqlalchemy import Date, Interval, Numeric, cast, func, literal, select, union_all
from sqlalchemy.ext.asyncio import AsyncSession

from ..model.prop_transaction import PropertyType, RentTransaction, SaleTransaction
from ..schemas.market import (
    DailySummaryResponse,
    PriceMover,
    PriceMoversResponse,
    VolumeSurgeRegion,
    VolumeSurgeRegionsResponse,
)
from ..schemas.prop_transaction import SalePriceTrendPoint
from .prop_transaction import (
    EMPTY_TREND_ON_NULL_KEY_TYPES,
    PRICE_TREND_GROUP_COLUMNS,
    build_sale_response,
    get_complex_ids,
    get_region_names,
)

# 연립다세대·단독다가구는 단지 개념이 모호해 급등/급락 순위에서 뺀다.
PRICE_MOVER_PROPERTY_TYPES = (PropertyType.APT, PropertyType.OFFICETEL)
# 이 기간 안의 거래만 비교에 쓴다.
PRICE_MOVER_LOOKBACK_YEARS = 10
# 최근 거래가 이 기간 안에 있는 단지만 순위에 넣는다. 오래전 거래끼리 비교한 결과가 섞이지 않게 한다.
PRICE_MOVER_RECENT_YEARS = 1
PRICE_MOVER_LIMIT = 5
# 실거래는 계약 후 30일 안에 신고되므로 신고 기한이 지난 구간끼리 비교하도록 이만큼 늦춘다.
VOLUME_SURGE_REPORT_DELAY_MONTHS = 1
VOLUME_SURGE_LIMIT = 5


async def get_daily_summary(session: AsyncSession, deal_date: date) -> DailySummaryResponse:
    """계약일 하루의 매매 최고가·최저가 거래와 전체 거래건수를 조회한다."""
    # 해제된 거래는 실제로 성사되지 않은 가격이라 최고가·최저가와 건수에서 모두 뺀다.
    sale_conditions = [
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
        .where(RentTransaction.deal_date == deal_date)
        .scalar_subquery()
    )
    transaction_count = await session.scalar(select(sale_count + rent_count))

    names = await get_region_names(session)
    complex_ids = await get_complex_ids(session, [sale for sale in (highest, lowest) if sale is not None])
    return DailySummaryResponse(
        deal_date=deal_date,
        highest_sale=None if highest is None else build_sale_response(highest, names, complex_ids),
        lowest_sale=None if lowest is None else build_sale_response(lowest, names, complex_ids),
        transaction_count=transaction_count,
    )


async def get_price_movers(session: AsyncSession, base_date: date) -> PriceMoversResponse:
    """직전 거래 대비 최근 거래 금액 변동률이 가장 큰 단지와 가장 작은 단지를 조회한다."""
    # 기준일이 2/29여도 깨지지 않도록 연 단위 경계는 PostgreSQL interval 연산으로 구한다.
    base = literal(base_date, Date)
    lookback_start = base - func.make_interval(PRICE_MOVER_LOOKBACK_YEARS, type_=Interval)
    recent_start = base - func.make_interval(PRICE_MOVER_RECENT_YEARS, type_=Interval)

    # 유형마다 단지를 묶는 컬럼이 달라 유형별로 윈도를 따로 계산해 합친다.
    ranked_by_type = []
    for property_type in PRICE_MOVER_PROPERTY_TYPES:
        group_columns = [getattr(SaleTransaction, column) for column in PRICE_TREND_GROUP_COLUMNS[property_type]]
        window = {
            "partition_by": [SaleTransaction.sido_code, SaleTransaction.sigungu_code, *group_columns],
            "order_by": [SaleTransaction.deal_date.desc(), SaleTransaction.id.desc()],
        }
        # 해제된 거래는 실제로 성사되지 않은 가격이라 비교에서 뺀다.
        conditions = [
            SaleTransaction.property_type == property_type,
            SaleTransaction.deal_date >= lookback_start,
            SaleTransaction.cancel_deal_type.is_distinct_from("O"),
        ]
        # 아파트는 묶음 컬럼이 NULL이면 같은 단지를 특정할 수 없어 뺀다. 오피스텔은 PARTITION BY가 NULL끼리 묶는다.
        if property_type in EMPTY_TREND_ON_NULL_KEY_TYPES:
            conditions += [column.is_not(None) for column in group_columns]
        ranked_by_type.append(
            select(
                SaleTransaction.id,
                SaleTransaction.deal_date,
                SaleTransaction.deal_amount,
                func.row_number().over(**window).label("rn"),
                func.lead(SaleTransaction.id).over(**window).label("previous_id"),
                func.lead(SaleTransaction.deal_date).over(**window).label("previous_deal_date"),
                func.lead(SaleTransaction.deal_amount).over(**window).label("previous_deal_amount"),
                func.lead(SaleTransaction.floor).over(**window).label("previous_floor"),
            ).where(*conditions)
        )
    ranked = union_all(*ranked_by_type).subquery()

    # 단지의 최근 거래 한 건에 직전 거래가 있으면 거래가 2건 이상인 단지다.
    change_rate = func.round(
        cast(ranked.c.deal_amount - ranked.c.previous_deal_amount, Numeric) * 100 / ranked.c.previous_deal_amount, 2
    ).label("change_rate")
    candidates = (
        select(*ranked.c, change_rate)
        .where(ranked.c.rn == 1, ranked.c.previous_id.is_not(None), ranked.c.deal_date >= recent_start)
        .cte("candidates")
    )
    # 두 순위를 한 문장으로 뽑아 비싼 윈도 계산을 한 번만 한다. 변동률이 같으면 id가 작은 거래를 먼저 둔다.
    surge = (
        select(candidates)
        .where(candidates.c.change_rate > 0)
        .order_by(candidates.c.change_rate.desc(), candidates.c.id)
        .limit(PRICE_MOVER_LIMIT)
    )
    plunge = (
        select(candidates)
        .where(candidates.c.change_rate < 0)
        .order_by(candidates.c.change_rate, candidates.c.id)
        .limit(PRICE_MOVER_LIMIT)
    )
    rows = (await session.execute(union_all(surge, plunge))).all()

    latest_sales = {
        transaction.id: transaction
        for transaction in await session.scalars(
            select(SaleTransaction).where(SaleTransaction.id.in_([row.id for row in rows]))
        )
    }
    names = await get_region_names(session)
    complex_ids = await get_complex_ids(session, list(latest_sales.values()))
    movers = [
        PriceMover(
            change_rate=row.change_rate,
            latest_sale=build_sale_response(latest_sales[row.id], names, complex_ids),
            previous_sale=SalePriceTrendPoint(
                id=row.previous_id,
                deal_date=row.previous_deal_date,
                deal_amount=row.previous_deal_amount,
                floor=row.previous_floor,
            ),
        )
        for row in rows
    ]
    # UNION ALL은 결과 순서를 보장하지 않으므로 순위 순서로 다시 정렬한다.
    return PriceMoversResponse(
        base_date=base_date,
        surge=sorted(
            (mover for mover in movers if mover.change_rate > 0),
            key=lambda mover: (-mover.change_rate, mover.latest_sale.id),
        ),
        plunge=sorted(
            (mover for mover in movers if mover.change_rate < 0),
            key=lambda mover: (mover.change_rate, mover.latest_sale.id),
        ),
    )


async def get_volume_surge_regions(session: AsyncSession, base_date: date) -> VolumeSurgeRegionsResponse:
    """최근 1개월 매매 건수가 이전 1개월보다 가장 많이 늘어난 시군구를 조회한다."""
    # 기준일이 월말이어도 깨지지 않도록 월 단위 경계는 PostgreSQL interval 연산으로 구한다.
    # 경계는 응답에도 실으므로 집계 전에 날짜로 받아 둔다. 최근 구간의 끝부터 1개월씩 거슬러 올라간다.
    base = literal(base_date, Date)
    months_before = [VOLUME_SURGE_REPORT_DELAY_MONTHS + offset for offset in range(3)]
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
