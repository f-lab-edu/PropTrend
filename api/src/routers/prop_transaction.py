from typing import Annotated

from fastapi import APIRouter, Query

from ..schemas.prop_transaction import PropTransactionQuery

router = APIRouter()


@router.get("/sales")
async def get_sale_prop_transactions(query: Annotated[PropTransactionQuery, Query()]):
    pass


@router.get("/rents")
async def get_rent_prop_transactions(query: Annotated[PropTransactionQuery, Query()]):
    pass
