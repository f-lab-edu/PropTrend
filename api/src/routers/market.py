from datetime import date, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from ..dependencies import get_session, get_today
from ..schemas.market import DailySummaryResponse
from ..services.market import get_daily_summary

router = APIRouter()


@router.get(
    "/daily-summary",
    status_code=status.HTTP_200_OK,
    summary="직전일 실거래 요약",
    description=(
        "호출일(KST) 전날을 계약일로 하는 실거래의 매매 최고가·최저가 거래와 거래건수를 반환합니다.\n\n"
        "- 실거래는 계약 후 30일 안에 신고되므로 현재까지 신고된 거래 기준이며, 이후 값이 늘어날 수 있습니다.\n"
        "- `highest_sale`, `lowest_sale`은 부동산 유형 구분 없이 매매 중에서 고릅니다.\n"
        "- 금액이 같은 거래가 여럿이면 `id`가 작은 거래를 줍니다.\n"
        "- 기준일에 매매가 없으면 `highest_sale`, `lowest_sale`은 `null`입니다.\n"
        "- `transaction_count`는 부동산 유형 구분 없이 매매와 전월세 건수를 합친 값입니다.\n"
        "- 해제된 매매는 최고가·최저가와 거래건수에서 모두 뺍니다.\n"
        "- `deal_amount`의 단위는 원입니다."
    ),
)
async def get_market_daily_summary(
    session: Annotated[AsyncSession, Depends(get_session)],
    today: Annotated[date, Depends(get_today)],
) -> DailySummaryResponse:
    """직전일 실거래 요약을 조회한다."""
    async with session.begin():
        return await get_daily_summary(session, today - timedelta(days=1))
