from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from ..dependencies import get_session
from ..schemas.prop_transaction import (
    PropTransactionQuery,
    RentPropTransactionDetailResponse,
    RentPropTransactionResponse,
    SalePropTransactionDetailResponse,
    SalePropTransactionResponse,
)
from ..services.prop_transaction import (
    get_rent_transaction_detail,
    get_rent_transactions,
    get_sale_transaction_detail,
    get_sale_transactions,
)

router = APIRouter()


@router.get(
    "/sales",
    status_code=status.HTTP_200_OK,
    summary="매매 실거래 목록 조회",
    description=(
        "조회 조건에 맞는 매매 실거래 목록을 반환합니다.\n\n"
        "- `property_type`, `sido_code`, `sigungu_code`, `deal_ymd`는 모두 필수입니다.\n"
        "- `deal_ymd`는 계약년월(`YYYYMM`)이며 그 달에 계약한 거래를 모두 반환합니다.\n"
        "- `address`는 법정동코드의 지역명, 읍면동, 지번 중 값이 있는 것만 이어 붙입니다.\n"
        "- `complex_id`는 거래가 속한 단지 id입니다. 아파트·오피스텔만 있으며, 단지 일련번호가 없는 아파트와 "
        "연립다세대·단독다가구는 `null`입니다.\n"
        "- `deal_amount`의 단위는 원입니다.\n"
        "- `cancel_deal_date`는 해제된 거래의 해제일이며, 해제되지 않았으면 `null`입니다.\n"
        "- 부동산 유형에 해당하지 않는 필드는 `null`로 응답합니다.\n"
        "- 결과는 `id` 오름차순이며 `limit`(기본 100, 최대 1000)과 `offset`으로 페이지를 나눕니다."
    ),
)
async def get_sale_prop_transactions(
    query: Annotated[PropTransactionQuery, Query()],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> list[SalePropTransactionResponse]:
    """매매 실거래 목록을 조회한다."""
    async with session.begin():
        return await get_sale_transactions(session, query)


@router.get(
    "/rents",
    status_code=status.HTTP_200_OK,
    summary="전월세 실거래 목록 조회",
    description=(
        "조회 조건에 맞는 전월세 실거래 목록을 반환합니다.\n\n"
        "- `property_type`, `sido_code`, `sigungu_code`, `deal_ymd`는 모두 필수입니다.\n"
        "- `deal_ymd`는 계약년월(`YYYYMM`)이며 그 달에 계약한 거래를 모두 반환합니다.\n"
        "- `address`는 법정동코드의 지역명, 읍면동, 지번 중 값이 있는 것만 이어 붙입니다.\n"
        "- `complex_id`는 거래가 속한 단지 id입니다. 아파트·오피스텔만 있으며, 단지 일련번호가 없는 아파트와 "
        "연립다세대·단독다가구는 `null`입니다.\n"
        "- `deposit`, `monthly_rent`의 단위는 원입니다. `monthly_rent`가 0이면 전세 거래입니다.\n"
        "- 부동산 유형에 해당하지 않는 필드는 `null`로 응답합니다.\n"
        "- 결과는 `id` 오름차순이며 `limit`(기본 100, 최대 1000)과 `offset`으로 페이지를 나눕니다."
    ),
)
async def get_rent_prop_transactions(
    query: Annotated[PropTransactionQuery, Query()],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> list[RentPropTransactionResponse]:
    """전월세 실거래 목록을 조회한다."""
    async with session.begin():
        return await get_rent_transactions(session, query)


@router.get(
    "/sales/{transaction_id}",
    status_code=status.HTTP_200_OK,
    summary="매매 실거래 상세 조회",
    description=(
        "매매 실거래 한 건의 상세 정보를 반환합니다.\n\n"
        "- `base_transaction`은 조회한 거래 정보입니다.\n"
        "- `base_transaction.complex_id`는 거래가 속한 단지 id입니다. 아파트·오피스텔만 있으며, "
        "단지 일련번호가 없는 아파트와 연립다세대·단독다가구는 `null`입니다.\n"
        "- `base_transaction.cancel_deal_date`는 해제된 거래의 해제일이며, 해제되지 않았으면 `null`입니다.\n"
        "- `trend`는 조회한 거래와 같은 그룹으로 묶인 거래들의 실거래가 추이이며 계약일 오름차순입니다.\n"
        "- 같은 그룹은 아파트면 같은 시군구·단지 일련번호·전용면적, "
        "오피스텔이면 같은 시군구·읍면동·지번·건물명·건축년도·전용면적, "
        "연립다세대면 같은 시군구·읍면동·지번·건축년도인 거래입니다. 조회한 거래도 포함합니다.\n"
        "- 아파트·연립다세대는 그룹 기준 값(단지 일련번호, 지번 등)이 비어 있는 거래를 그룹에서 빼고, "
        "조회한 거래의 기준 값이 비어 있으면 추이가 빈 목록입니다.\n"
        "- 해제된 거래는 `trend`에서 뺍니다.\n"
        "- 단독다가구는 같은 매물을 특정할 수 없어 `trend`가 `null`입니다.\n"
        "- `deal_amount`의 단위는 원입니다.\n"
        "- `transaction_id`에 해당하는 매매 거래가 없으면 404를 반환합니다."
    ),
)
async def get_sale_prop_transaction_detail(
    transaction_id: Annotated[int, Path(ge=1)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> SalePropTransactionDetailResponse:
    """매매 실거래 상세 정보를 조회한다."""
    async with session.begin():
        return await get_sale_transaction_detail(session, transaction_id)


@router.get(
    "/rents/{transaction_id}",
    status_code=status.HTTP_200_OK,
    summary="전월세 실거래 상세 조회",
    description=(
        "전월세 실거래 한 건의 상세 정보를 반환합니다.\n\n"
        "- `base_transaction`은 조회한 거래 정보입니다.\n"
        "- `base_transaction.complex_id`는 거래가 속한 단지 id입니다. 아파트·오피스텔만 있으며, "
        "단지 일련번호가 없는 아파트와 연립다세대·단독다가구는 `null`입니다.\n"
        "- `jeonse_trend`, `monthly_rent_trend`는 조회한 거래와 같은 그룹으로 묶인 거래들의 실거래가 추이를 "
        "전세(`monthly_rent`가 0)와 월세로 나눈 것이며 계약일 오름차순입니다.\n"
        "- 같은 그룹은 아파트면 같은 시군구·단지 일련번호·전용면적, "
        "오피스텔이면 같은 시군구·읍면동·지번·건물명·건축년도·전용면적, "
        "연립다세대면 같은 시군구·읍면동·지번·건축년도인 거래입니다. 조회한 거래도 포함합니다.\n"
        "- 아파트·연립다세대는 그룹 기준 값(단지 일련번호, 지번 등)이 비어 있는 거래를 그룹에서 빼고, "
        "조회한 거래의 기준 값이 비어 있으면 추이가 빈 목록입니다.\n"
        "- 단독다가구는 같은 매물을 특정할 수 없어 `jeonse_trend`, `monthly_rent_trend`가 `null`입니다.\n"
        "- `deposit`, `monthly_rent`의 단위는 원입니다.\n"
        "- `transaction_id`에 해당하는 전월세 거래가 없으면 404를 반환합니다."
    ),
)
async def get_rent_prop_transaction_detail(
    transaction_id: Annotated[int, Path(ge=1)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> RentPropTransactionDetailResponse:
    """전월세 실거래 상세 정보를 조회한다."""
    async with session.begin():
        return await get_rent_transaction_detail(session, transaction_id)
