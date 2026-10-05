from datetime import date

from . import PropTrendCoreModel
from .prop_transaction import SalePropTransactionResponse


class DailySummaryResponse(PropTrendCoreModel):
    """직전일 실거래 요약 응답."""

    # 집계 기준 계약일.
    deal_date: date
    # 기준일 매매가 없으면 null.
    highest_sale: SalePropTransactionResponse | None
    lowest_sale: SalePropTransactionResponse | None
    # 매매와 전월세를 합친 건수.
    transaction_count: int
