from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from ..dependencies import get_session
from ..schemas.prop_transaction import PropTransactionQuery, RentPropTransactionResponse, SalePropTransactionResponse
from ..services.prop_transaction import get_rent_transactions, get_sale_transactions

router = APIRouter()


@router.get("/sales")
async def get_sale_prop_transactions(
    query: Annotated[PropTransactionQuery, Query()],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> list[SalePropTransactionResponse]:
    """매매 실거래 목록을 조회한다."""
    async with session.begin():
        return await get_sale_transactions(session, **query.model_dump())


@router.get("/rents")
async def get_rent_prop_transactions(
    query: Annotated[PropTransactionQuery, Query()],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> list[RentPropTransactionResponse]:
    """전월세 실거래 목록을 조회한다."""
    async with session.begin():
        return await get_rent_transactions(session, **query.model_dump())
