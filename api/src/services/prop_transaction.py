import time

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..exceptions import TransactionNotFoundError
from ..model.prop_transaction import PropertyType, RentTransaction, SaleTransaction
from ..model.raw import LegalDongCodeRawItem
from ..schemas.prop_transaction import (
    PropTransactionQuery,
    RentPriceTrendPoint,
    RentPropTransactionDetailResponse,
    RentPropTransactionResponse,
    SalePriceTrendPoint,
    SalePropTransactionDetailResponse,
    SalePropTransactionResponse,
)

# 법정동코드는 파이프라인이 하루 한 번 통째로 갈아 끼운다. 갱신 후 늦어도 이 시간 안에는 새 이름을 쓴다.
REGION_NAME_TTL_SECONDS = 60 * 60

# 유형·시도·시군구에 더해 같은 매물로 묶는 컬럼.
# 아파트·오피스텔은 단지 안의 평면 타입까지 나누려고 전용면적을 그대로 쓴다.
# 연립다세대는 건물명의 약 20%가 "(지번)"으로 채워져 있고 재건축되면 이름이 바뀌므로 이름 대신 건축년도로 건물을 가른다.
# 단독다가구는 지번이 가려지거나 없고 건물명·전용면적도 없어 같은 매물을 특정할 수 없으므로 추이를 주지 않는다.
PRICE_TREND_GROUP_COLUMNS: dict[PropertyType, tuple[str, ...]] = {
    PropertyType.APT: ("umd_name", "jibun", "building_name", "exclusive_use_area"),
    PropertyType.OFFICETEL: ("umd_name", "jibun", "building_name", "exclusive_use_area"),
    PropertyType.ROW_HOUSE: ("umd_name", "jibun", "build_year"),
}

# 캐시된 지역명 데이터
_region_names: dict[tuple[str, str], str] = {}
_region_names_loaded_at: float | None = None


async def _get_region_names(session: AsyncSession) -> dict[tuple[str, str], str]:
    """(시도코드, 시군구코드) → 지역명("서울특별시 강남구") 매핑. TTL 동안 메모리에 캐시한다."""
    global _region_names, _region_names_loaded_at
    if _region_names_loaded_at is not None and time.monotonic() - _region_names_loaded_at < REGION_NAME_TTL_SECONDS:
        return _region_names

    payload = LegalDongCodeRawItem.payload
    result = await session.execute(
        select(payload["sido_cd"].astext, payload["sgg_cd"].astext, payload["locatadd_nm"].astext)
    )
    names = {(sido_cd, sgg_cd): name for sido_cd, sgg_cd, name in result.tuples()}
    # 파이프라인이 한 번도 돌지 않아 표가 비어 있으면 캐시하지 않고 다음 요청에서 다시 읽는다.
    if names:
        _region_names, _region_names_loaded_at = names, time.monotonic()
    return names


def _build_sale_response_with_region_name(
    transaction: SaleTransaction, names: dict[tuple[str, str], str]
) -> SalePropTransactionResponse:
    """매매 ORM 행을 응답 스키마로 바꾸고 캐시에서 찾은 지역명을 채운다."""
    response = SalePropTransactionResponse.model_validate(transaction)
    response.region_name = names.get((transaction.sido_code, transaction.sigungu_code))
    return response


def _build_rent_response_with_region_name(
    transaction: RentTransaction, names: dict[tuple[str, str], str]
) -> RentPropTransactionResponse:
    """전월세 ORM 행을 응답 스키마로 바꾸고 캐시에서 찾은 지역명을 채운다."""
    response = RentPropTransactionResponse.model_validate(transaction)
    response.region_name = names.get((transaction.sido_code, transaction.sigungu_code))
    return response


async def get_sale_transactions(
    session: AsyncSession, query: PropTransactionQuery
) -> list[SalePropTransactionResponse]:
    """조건에 맞는 매매 실거래 목록을 조회한다."""
    conditions = [
        SaleTransaction.property_type == query.property_type,
        SaleTransaction.sido_code == query.sido_code,
        SaleTransaction.sigungu_code == query.sigungu_code,
        SaleTransaction.deal_date == query.deal_date,
    ]

    # 페이지 경계가 요청마다 달라지지 않도록 id 순으로 고정한다.
    result = await session.execute(
        select(SaleTransaction).where(*conditions).order_by(SaleTransaction.id).limit(query.limit).offset(query.offset)
    )
    names = await _get_region_names(session)
    return [_build_sale_response_with_region_name(transaction, names) for transaction in result.scalars()]


async def get_rent_transactions(
    session: AsyncSession, query: PropTransactionQuery
) -> list[RentPropTransactionResponse]:
    """조건에 맞는 전월세 실거래 목록을 조회한다."""
    conditions = [
        RentTransaction.property_type == query.property_type,
        RentTransaction.sido_code == query.sido_code,
        RentTransaction.sigungu_code == query.sigungu_code,
        RentTransaction.deal_date == query.deal_date,
    ]

    # 페이지 경계가 요청마다 달라지지 않도록 id 순으로 고정한다.
    result = await session.execute(
        select(RentTransaction).where(*conditions).order_by(RentTransaction.id).limit(query.limit).offset(query.offset)
    )
    names = await _get_region_names(session)
    return [_build_rent_response_with_region_name(transaction, names) for transaction in result.scalars()]


async def get_sale_transaction_detail(session: AsyncSession, transaction_id: int) -> SalePropTransactionDetailResponse:
    """매매 실거래 상세 정보를 조회한다."""
    transaction = await session.get(SaleTransaction, transaction_id)
    if transaction is None:
        raise TransactionNotFoundError("실거래를 찾을 수 없습니다")

    names = await _get_region_names(session)
    base_transaction = _build_sale_response_with_region_name(transaction, names)
    group_columns = PRICE_TREND_GROUP_COLUMNS.get(transaction.property_type)
    if group_columns is None:
        return SalePropTransactionDetailResponse(base_transaction=base_transaction, trend=None)

    # 원본에 지번·건축년도가 빈 행이 있어, NULL끼리도 같은 값으로 보도록 IS NOT DISTINCT FROM으로 비교한다.
    conditions = [
        getattr(SaleTransaction, column).is_not_distinct_from(getattr(transaction, column))
        for column in ("property_type", "sido_code", "sigungu_code", *group_columns)
    ]
    # 해제된 거래는 실제로 성사되지 않은 가격이라 추이에서 뺀다.
    result = await session.execute(
        select(SaleTransaction.id, SaleTransaction.deal_date, SaleTransaction.deal_amount, SaleTransaction.floor)
        .where(*conditions, SaleTransaction.cancel_deal_type.is_distinct_from("O"))
        .order_by(SaleTransaction.deal_date, SaleTransaction.id)
    )
    trend = [SalePriceTrendPoint.model_validate(row) for row in result]
    return SalePropTransactionDetailResponse(base_transaction=base_transaction, trend=trend)


async def get_rent_transaction_detail(session: AsyncSession, transaction_id: int) -> RentPropTransactionDetailResponse:
    """전월세 실거래 상세 정보를 조회한다."""
    transaction = await session.get(RentTransaction, transaction_id)
    if transaction is None:
        raise TransactionNotFoundError("실거래를 찾을 수 없습니다")

    names = await _get_region_names(session)
    base_transaction = _build_rent_response_with_region_name(transaction, names)
    group_columns = PRICE_TREND_GROUP_COLUMNS.get(transaction.property_type)
    if group_columns is None:
        return RentPropTransactionDetailResponse(
            base_transaction=base_transaction, jeonse_trend=None, monthly_rent_trend=None
        )

    # 원본에 지번·건축년도가 빈 행이 있어, NULL끼리도 같은 값으로 보도록 IS NOT DISTINCT FROM으로 비교한다.
    conditions = [
        getattr(RentTransaction, column).is_not_distinct_from(getattr(transaction, column))
        for column in ("property_type", "sido_code", "sigungu_code", *group_columns)
    ]
    result = await session.execute(
        select(
            RentTransaction.id,
            RentTransaction.deal_date,
            RentTransaction.deposit,
            RentTransaction.monthly_rent,
            RentTransaction.floor,
        )
        .where(*conditions)
        .order_by(RentTransaction.deal_date, RentTransaction.id)
    )
    # 한 번 읽어 월세가 0인 전세와 나머지 월세로 나눈다.
    jeonse_trend, monthly_rent_trend = [], []
    for row in result:
        point = RentPriceTrendPoint.model_validate(row)
        (jeonse_trend if point.monthly_rent == 0 else monthly_rent_trend).append(point)
    return RentPropTransactionDetailResponse(
        base_transaction=base_transaction, jeonse_trend=jeonse_trend, monthly_rent_trend=monthly_rent_trend
    )
