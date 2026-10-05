from datetime import date, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from ..dependencies import get_session, get_today
from ..schemas.market import DailySummaryResponse, PriceMoversResponse, VolumeSurgeRegionsResponse
from ..services.market import get_daily_summary, get_price_movers, get_volume_surge_regions

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
        "- `complex_id`는 거래가 속한 단지 id이며, 단지가 없는 거래(연립다세대·단독다가구 등)는 `null`입니다.\n"
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


@router.get(
    "/price-movers",
    status_code=status.HTTP_200_OK,
    summary="거래액 급등/급락 단지 TOP 5",
    description=(
        "단지별로 최근 매매와 바로 앞 매매의 금액 변동률을 구해 상승률 상위 5개와 하락률 상위 5개를 반환합니다.\n\n"
        "- 기준일은 호출일(KST)이며, 계약일이 기준일로부터 10년 이내인 매매만 비교에 씁니다.\n"
        "- 아파트와 오피스텔만 다루고, 두 유형을 합쳐 한 순위로 매깁니다. 해제된 매매는 뺍니다.\n"
        "- 단지는 전용면적별로 나눠 비교합니다. 아파트는 단지 일련번호 + 전용면적, 오피스텔은 법정동·지번·건물명·"
        "건축년도 + 전용면적으로 묶습니다.\n"
        "- 단지 일련번호가 없는 아파트 매매(상세 자료로 재수집되기 전 과거 거래)는 뺍니다.\n"
        "- 10년 이내 거래가 2건 이상이고, 최근 거래의 계약일이 기준일로부터 1년 이내인 단지만 순위에 넣습니다.\n"
        "- `change_rate`는 `(최근 - 직전) / 직전 × 100`을 소수 둘째 자리까지 반올림한 값(%)입니다.\n"
        "- `surge`는 변동률이 0보다 큰 단지, `plunge`는 0보다 작은 단지만 담으며 5개보다 적을 수 있습니다.\n"
        "- 변동률이 같으면 최근 거래의 `id`가 작은 단지가 먼저 옵니다.\n"
        "- `latest_sale.complex_id`는 최근 거래가 속한 단지 id입니다.\n"
        "- 요청마다 전국 거래를 집계하므로 응답에 수 초가 걸릴 수 있습니다.\n"
        "- `deal_amount`의 단위는 원입니다."
    ),
)
async def get_market_price_movers(
    session: Annotated[AsyncSession, Depends(get_session)],
    today: Annotated[date, Depends(get_today)],
) -> PriceMoversResponse:
    """거래액 급등/급락 단지 TOP 5를 조회한다."""
    async with session.begin():
        return await get_price_movers(session, today)


@router.get(
    "/volume-surge-regions",
    status_code=status.HTTP_200_OK,
    summary="거래량 급등 지역 TOP 5",
    description=(
        "시군구별로 최근 1개월과 이전 1개월의 매매 건수를 비교해 건수가 가장 많이 늘어난 지역 5개를 반환합니다.\n\n"
        "- 실거래는 계약 후 30일 안에 신고되므로 신고 기한이 지난 구간끼리 비교합니다. 기준일(호출일, KST) 기준으로 "
        "최근 구간은 2개월 전 ~ 1개월 전, 이전 구간은 3개월 전 ~ 2개월 전입니다.\n"
        "- 구간의 시작일과 종료일(`*_start_date`, `*_end_date`)은 모두 구간에 포함됩니다.\n"
        "- 부동산 유형 구분 없이 매매만 세고, 해제된 매매는 뺍니다.\n"
        "- `region_code`는 시도코드 2자리와 시군구코드 3자리를 합친 5자리입니다.\n"
        "- `count_change`(`recent_count - previous_count`)가 큰 순서이며, 0보다 큰 지역만 담아 5개보다 적을 수 "
        "있습니다.\n"
        "- `count_change`가 같으면 `region_code`가 작은 지역이 먼저 옵니다.\n"
        "- 법정동코드에 없는 지역이면 `region_name`은 `null`입니다."
    ),
)
async def get_market_volume_surge_regions(
    session: Annotated[AsyncSession, Depends(get_session)],
    today: Annotated[date, Depends(get_today)],
) -> VolumeSurgeRegionsResponse:
    """거래량 급등 지역 TOP 5를 조회한다."""
    async with session.begin():
        return await get_volume_surge_regions(session, today)
