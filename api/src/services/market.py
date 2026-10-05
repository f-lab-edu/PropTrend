from datetime import date

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..model.prop_transaction import RentTransaction, SaleTransaction
from ..schemas.market import DailySummaryResponse
from .prop_transaction import build_sale_response_with_region_name, get_region_names


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
    return DailySummaryResponse(
        deal_date=deal_date,
        highest_sale=None if highest is None else build_sale_response_with_region_name(highest, names),
        lowest_sale=None if lowest is None else build_sale_response_with_region_name(lowest, names),
        transaction_count=transaction_count,
    )
