import time
from datetime import date

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..model.prop_transaction import PropertyType, RentTransaction, SaleTransaction
from ..model.raw import LegalDongCodeRawItem
from ..schemas.prop_transaction import RentPropTransactionResponse, SalePropTransactionResponse

# 법정동코드는 파이프라인이 하루 한 번 통째로 갈아 끼운다. 갱신 후 늦어도 이 시간 안에는 새 이름을 쓴다.
REGION_NAME_TTL_SECONDS = 60 * 60

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
    session: AsyncSession,
    *,
    property_type: PropertyType,
    sido_code: str | None = None,
    sigungu_code: str | None = None,
    deal_date: date | None = None,
) -> list[SalePropTransactionResponse]:
    """조건에 맞는 매매 실거래 목록을 조회한다."""
    # 지역은 시도 → 시군구 → 계약일 순으로 좁힌다. 상위 조건 없이 하위 조건만 오면 거부한다.
    if sigungu_code is not None and sido_code is None:
        raise ValueError("sigungu_code는 sido_code와 함께 지정해야 한다")
    if deal_date is not None and (sido_code is None or sigungu_code is None):
        raise ValueError("deal_date는 sido_code, sigungu_code와 함께 지정해야 한다")

    conditions = [SaleTransaction.property_type == property_type]
    if sido_code is not None:
        conditions.append(SaleTransaction.sido_code == sido_code)
    if sigungu_code is not None:
        conditions.append(SaleTransaction.sigungu_code == sigungu_code)
    if deal_date is not None:
        conditions.append(SaleTransaction.deal_date == deal_date)

    result = await session.execute(select(SaleTransaction).where(*conditions))
    names = await _get_region_names(session)
    return [_build_sale_response_with_region_name(transaction, names) for transaction in result.scalars()]


async def get_rent_transactions(
    session: AsyncSession,
    *,
    property_type: PropertyType,
    sido_code: str | None = None,
    sigungu_code: str | None = None,
    deal_date: date | None = None,
) -> list[RentPropTransactionResponse]:
    """조건에 맞는 전월세 실거래 목록을 조회한다."""
    # 지역은 시도 → 시군구 → 계약일 순으로 좁힌다. 상위 조건 없이 하위 조건만 오면 거부한다.
    if sigungu_code is not None and sido_code is None:
        raise ValueError("sigungu_code는 sido_code와 함께 지정해야 한다")
    if deal_date is not None and (sido_code is None or sigungu_code is None):
        raise ValueError("deal_date는 sido_code, sigungu_code와 함께 지정해야 한다")

    conditions = [RentTransaction.property_type == property_type]
    if sido_code is not None:
        conditions.append(RentTransaction.sido_code == sido_code)
    if sigungu_code is not None:
        conditions.append(RentTransaction.sigungu_code == sigungu_code)
    if deal_date is not None:
        conditions.append(RentTransaction.deal_date == deal_date)

    result = await session.execute(select(RentTransaction).where(*conditions))
    names = await _get_region_names(session)
    return [_build_rent_response_with_region_name(transaction, names) for transaction in result.scalars()]
