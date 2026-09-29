from typing import Annotated

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from ..dependencies import get_session
from ..schemas.prop_transaction import PropTransactionQuery, RentPropTransactionResponse, SalePropTransactionResponse
from ..services.prop_transaction import get_rent_transactions, get_sale_transactions

router = APIRouter()


@router.get(
    "/sales",
    status_code=status.HTTP_200_OK,
    summary="매매 실거래 목록 조회",
    description=(
        "조회 조건에 맞는 매매 실거래 목록을 반환합니다.\n\n"
        "- 지역 조건은 `sido_code` → `sigungu_code` → `deal_date` 순으로 좁힙니다. "
        "상위 조건 없이 하위 조건만 지정하면 400을 반환합니다.\n"
        "- `address`는 법정동코드의 지역명, 읍면동, 지번 중 값이 있는 것만 이어 붙입니다.\n"
        "- `deal_amount`의 단위는 원입니다.\n"
        "- 부동산 유형에 해당하지 않는 필드는 `null`로 응답합니다."
    ),
)
async def get_sale_prop_transactions(
    query: Annotated[PropTransactionQuery, Query()],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> list[SalePropTransactionResponse]:
    """매매 실거래 목록을 조회한다."""
    async with session.begin():
        return await get_sale_transactions(session, **query.model_dump())


@router.get(
    "/rents",
    status_code=status.HTTP_200_OK,
    summary="전월세 실거래 목록 조회",
    description=(
        "조회 조건에 맞는 전월세 실거래 목록을 반환합니다.\n\n"
        "- 지역 조건은 `sido_code` → `sigungu_code` → `deal_date` 순으로 좁힙니다. "
        "상위 조건 없이 하위 조건만 지정하면 400을 반환합니다.\n"
        "- `address`는 법정동코드의 지역명, 읍면동, 지번 중 값이 있는 것만 이어 붙입니다.\n"
        "- `deposit`, `monthly_rent`의 단위는 원입니다. `monthly_rent`가 0이면 전세 거래입니다.\n"
        "- 부동산 유형에 해당하지 않는 필드는 `null`로 응답합니다."
    ),
)
async def get_rent_prop_transactions(
    query: Annotated[PropTransactionQuery, Query()],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> list[RentPropTransactionResponse]:
    """전월세 실거래 목록을 조회한다."""
    async with session.begin():
        return await get_rent_transactions(session, **query.model_dump())
