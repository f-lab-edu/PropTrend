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


class VolumeSurgeRegion(PropTrendCoreModel):
    """거래량 급등 지역 하나."""

    # 시도코드 2자리 + 시군구코드 3자리.
    region_code: str
    # 법정동코드에 없는 지역이면 null.
    region_name: str | None
    recent_count: int
    previous_count: int
    # recent_count - previous_count.
    count_change: int


class VolumeSurgeRegionsResponse(PropTrendCoreModel):
    """거래량 급등 지역 TOP 5 응답."""

    # 기간 계산의 기준일.
    base_date: date
    # 구간의 시작일과 종료일은 모두 구간에 포함된다.
    recent_start_date: date
    recent_end_date: date
    previous_start_date: date
    previous_end_date: date
    # 거래량이 많이 늘어난 순서.
    regions: list[VolumeSurgeRegion]
