from datetime import date

from . import PropTrendCoreModel
from .prop_transaction import SalePriceTrendPoint, SalePropTransactionResponse


class DailySummaryResponse(PropTrendCoreModel):
    """직전일 실거래 요약 응답."""

    # 집계 기준 계약일.
    deal_date: date
    # 기준일 매매가 없으면 null.
    highest_sale: SalePropTransactionResponse | None
    lowest_sale: SalePropTransactionResponse | None
    # 매매와 전월세를 합친 건수.
    transaction_count: int


class PriceMover(PropTrendCoreModel):
    """거래액 급등/급락 단지 하나."""

    # 직전 거래 대비 최근 거래 금액 변동률(%). 소수 셋째 자리에서 반올림.
    change_rate: float
    latest_sale: SalePropTransactionResponse
    previous_sale: SalePriceTrendPoint


class PriceMoversResponse(PropTrendCoreModel):
    """거래액 급등/급락 단지 TOP 5 응답."""

    # 기간 계산의 기준일.
    base_date: date
    surge: list[PriceMover]
    plunge: list[PriceMover]
