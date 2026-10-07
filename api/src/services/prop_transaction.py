import time
from collections.abc import Sequence

from sqlalchemy import func, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from ..exceptions import TransactionNotFoundError
from ..jobs.utils import month_range
from ..model.complex import COMPLEX_KEY_COLUMNS, COMPLEX_PROPERTY_TYPES, Complex
from ..model.prop_transaction import PropertyType, RentTransaction, SaleTransaction, TransactionMixin
from ..model.raw import LegalDongCodeRawItem
from ..schemas.prop_transaction import (
    RentPriceTrendPoint,
    RentPropTransactionDetailResponse,
    RentPropTransactionListResponse,
    RentPropTransactionResponse,
    SalePriceTrendPoint,
    SalePropTransactionDetailResponse,
    SalePropTransactionListResponse,
    SalePropTransactionResponse,
)

# 법정동코드는 파이프라인이 하루 한 번 통째로 갈아 끼운다. 갱신 후 늦어도 이 시간 안에는 새 이름을 쓴다.
REGION_NAME_TTL_SECONDS = 60 * 60

# 유형·시도·시군구에 더해 같은 매물로 묶는 컬럼.
# 아파트·오피스텔은 단지 안의 평면 타입까지 나누려고 전용면적을 구간으로 묶지 않고 그대로 쓴다.
# 오피스텔은 단지 일련번호가 없어 지번·건물명으로 건물을 잡고, 같은 자리에 다시 지은 건물은 건축년도로 가른다.
# 아파트는 단지명이 과거 거래까지 소급해 바뀌어 이름으로 묶으면 같은 단지가 갈라지므로 단지 일련번호로 묶는다.
# 연립다세대는 건물명의 약 20%가 "(지번)"으로 채워져 있고 재건축되면 이름이 바뀌므로 이름 대신 건축년도로 건물을 가른다.
# 단독다가구는 지번이 가려지거나 없고 건물명·전용면적도 없어 같은 매물을 특정할 수 없으므로 추이를 주지 않는다.
PRICE_TREND_GROUP_COLUMNS: dict[PropertyType, tuple[str, ...]] = {
    PropertyType.APT: ("apartment_serial_number", "exclusive_use_area"),
    PropertyType.OFFICETEL: ("umd_name", "jibun", "building_name", "build_year", "exclusive_use_area"),
    PropertyType.ROW_HOUSE: ("umd_name", "jibun", "build_year"),
}

# 조회한 거래의 묶음 컬럼이 하나라도 NULL이면 같은 매물을 특정할 수 없다고 보고 빈 추이를 주는 유형.
# 아파트는 재수집 전 매매의 단지 일련번호가, 연립다세대는 지번·건축년도가 빈 행이 있다.
# 오피스텔은 빈 값이 있는 행도 NULL끼리 같은 값으로 보고 묶는다.
EMPTY_TREND_ON_NULL_KEY_TYPES = frozenset({PropertyType.APT, PropertyType.ROW_HOUSE})

# 캐시된 지역명 데이터
_region_names: dict[tuple[str, str], str] = {}
_region_names_loaded_at: float | None = None


async def get_region_names(session: AsyncSession) -> dict[tuple[str, str], str]:
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


async def get_complex_ids(session: AsyncSession, *, transactions: Sequence[TransactionMixin]) -> dict[int, int]:
    """거래 id → 거래가 속한 단지 id. 단지가 없는 거래는 담지 않는다."""
    complex_ids: dict[int, int] = {}
    for property_type in COMPLEX_PROPERTY_TYPES:
        targets = [transaction for transaction in transactions if transaction.property_type == property_type]
        if not targets:
            continue
        key_columns = COMPLEX_KEY_COLUMNS[property_type]
        # 단지 유니크 인덱스를 타도록 아파트는 단지 일련번호로, 오피스텔은 인덱스 선두인 시도·시군구로 좁힌다.
        if property_type == PropertyType.APT:
            condition = Complex.apartment_serial_number.in_(
                list({transaction.apartment_serial_number for transaction in targets})
            )
        else:
            condition = tuple_(Complex.sido_code, Complex.sigungu_code).in_(
                list({(transaction.sido_code, transaction.sigungu_code) for transaction in targets})
            )
        result = await session.execute(
            select(Complex.id, *(getattr(Complex, column) for column in key_columns)).where(
                Complex.property_type == property_type, condition
            )
        )
        # 오피스텔 단지 키에는 NULL이 있을 수 있다. SQL 비교와 달리 파이썬 튜플은 None끼리 같아 그대로 맞춘다.
        ids_by_key = {tuple(row[1:]): row[0] for row in result}
        for transaction in targets:
            complex_id = ids_by_key.get(tuple(getattr(transaction, column) for column in key_columns))
            if complex_id is not None:
                complex_ids[transaction.id] = complex_id
    return complex_ids


def build_sale_response(
    transaction: SaleTransaction, names: dict[tuple[str, str], str], complex_ids: dict[int, int]
) -> SalePropTransactionResponse:
    """매매 ORM 행을 응답 스키마로 바꾸고 캐시에서 찾은 지역명과 단지 id를 채운다."""
    response = SalePropTransactionResponse.model_validate(transaction)
    response.region_name = names.get((transaction.sido_code, transaction.sigungu_code))
    response.complex_id = complex_ids.get(transaction.id)
    return response


def _build_rent_response(
    transaction: RentTransaction, names: dict[tuple[str, str], str], complex_ids: dict[int, int]
) -> RentPropTransactionResponse:
    """전월세 ORM 행을 응답 스키마로 바꾸고 캐시에서 찾은 지역명과 단지 id를 채운다."""
    response = RentPropTransactionResponse.model_validate(transaction)
    response.region_name = names.get((transaction.sido_code, transaction.sigungu_code))
    response.complex_id = complex_ids.get(transaction.id)
    return response


async def get_sale_transactions(
    session: AsyncSession,
    *,
    property_type: PropertyType,
    sido_code: str,
    sigungu_code: str,
    deal_ymd: str,
    limit: int,
    offset: int,
) -> SalePropTransactionListResponse:
    """조건에 맞는 매매 실거래 목록을 조회한다."""
    start, end = month_range(deal_ymd)
    conditions = [
        SaleTransaction.property_type == property_type,
        SaleTransaction.sido_code == sido_code,
        SaleTransaction.sigungu_code == sigungu_code,
        SaleTransaction.deal_date >= start,
        SaleTransaction.deal_date < end,
    ]

    total = (await session.execute(select(func.count()).select_from(SaleTransaction).where(*conditions))).scalar_one()
    # 페이지 경계가 요청마다 달라지지 않도록 id 순으로 고정한다.
    result = await session.execute(
        select(SaleTransaction).where(*conditions).order_by(SaleTransaction.id).limit(limit).offset(offset)
    )
    transactions = list(result.scalars())
    names = await get_region_names(session)
    complex_ids = await get_complex_ids(session, transactions=transactions)
    return SalePropTransactionListResponse(
        items=[build_sale_response(transaction, names, complex_ids) for transaction in transactions],
        total=total,
        limit=limit,
        offset=offset,
    )


async def get_rent_transactions(
    session: AsyncSession,
    *,
    property_type: PropertyType,
    sido_code: str,
    sigungu_code: str,
    deal_ymd: str,
    limit: int,
    offset: int,
) -> RentPropTransactionListResponse:
    """조건에 맞는 전월세 실거래 목록을 조회한다."""
    start, end = month_range(deal_ymd)
    conditions = [
        RentTransaction.property_type == property_type,
        RentTransaction.sido_code == sido_code,
        RentTransaction.sigungu_code == sigungu_code,
        RentTransaction.deal_date >= start,
        RentTransaction.deal_date < end,
    ]

    total = (await session.execute(select(func.count()).select_from(RentTransaction).where(*conditions))).scalar_one()
    # 페이지 경계가 요청마다 달라지지 않도록 id 순으로 고정한다.
    result = await session.execute(
        select(RentTransaction).where(*conditions).order_by(RentTransaction.id).limit(limit).offset(offset)
    )
    transactions = list(result.scalars())
    names = await get_region_names(session)
    complex_ids = await get_complex_ids(session, transactions=transactions)
    return RentPropTransactionListResponse(
        items=[_build_rent_response(transaction, names, complex_ids) for transaction in transactions],
        total=total,
        limit=limit,
        offset=offset,
    )


async def get_sale_transaction_detail(
    session: AsyncSession, *, transaction_id: int
) -> SalePropTransactionDetailResponse:
    """매매 실거래 상세 정보를 조회한다."""
    transaction = await session.get(SaleTransaction, transaction_id)
    if transaction is None:
        raise TransactionNotFoundError("실거래를 찾을 수 없습니다")

    names = await get_region_names(session)
    complex_ids = await get_complex_ids(session, transactions=[transaction])
    base_transaction = build_sale_response(transaction, names, complex_ids)
    group_columns = PRICE_TREND_GROUP_COLUMNS.get(transaction.property_type)
    if group_columns is None:
        return SalePropTransactionDetailResponse(base_transaction=base_transaction, trend=None)
    # 아래 IS NOT DISTINCT FROM이 NULL끼리 묶어 서로 다른 매물이 한 추이로 섞이지 않도록 먼저 끊는다.
    if transaction.property_type in EMPTY_TREND_ON_NULL_KEY_TYPES and any(
        getattr(transaction, column) is None for column in group_columns
    ):
        return SalePropTransactionDetailResponse(base_transaction=base_transaction, trend=[])

    # 오피스텔은 원본에 빈 값이 있는 행도 NULL끼리 같은 값으로 보도록 IS NOT DISTINCT FROM으로 비교한다.
    # 묶음 컬럼이 모두 채워진 거래라면 NULL인 행은 이 비교에서 자연스럽게 빠진다.
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


async def get_rent_transaction_detail(
    session: AsyncSession, *, transaction_id: int
) -> RentPropTransactionDetailResponse:
    """전월세 실거래 상세 정보를 조회한다."""
    transaction = await session.get(RentTransaction, transaction_id)
    if transaction is None:
        raise TransactionNotFoundError("실거래를 찾을 수 없습니다")

    names = await get_region_names(session)
    complex_ids = await get_complex_ids(session, transactions=[transaction])
    base_transaction = _build_rent_response(transaction, names, complex_ids)
    group_columns = PRICE_TREND_GROUP_COLUMNS.get(transaction.property_type)
    if group_columns is None:
        return RentPropTransactionDetailResponse(
            base_transaction=base_transaction, jeonse_trend=None, monthly_rent_trend=None
        )
    # 매매와 같은 규칙이다.
    if transaction.property_type in EMPTY_TREND_ON_NULL_KEY_TYPES and any(
        getattr(transaction, column) is None for column in group_columns
    ):
        return RentPropTransactionDetailResponse(
            base_transaction=base_transaction, jeonse_trend=[], monthly_rent_trend=[]
        )

    # 오피스텔은 원본에 빈 값이 있는 행도 NULL끼리 같은 값으로 보도록 IS NOT DISTINCT FROM으로 비교한다.
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
