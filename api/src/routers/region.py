from typing import Annotated

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from ..dependencies import get_session
from ..schemas.region import RegionListResponse
from ..services.region import get_regions

router = APIRouter()


@router.get(
    "",
    response_model=RegionListResponse,
    status_code=status.HTTP_200_OK,
    summary="시도·시군구 목록 조회",
    description=(
        "실거래 목록 조회의 `sido_code`, `sigungu_code` 필터에 쓸 지역 목록을 시도별로 묶어 반환합니다.\n\n"
        "- 시도를 `items`에 담고, 시도마다 그 시도에 속한 시군구만 `sigungus`에 담습니다.\n"
        "- 시군구 코드는 시도 안에서만 고유하므로 실거래 조회에는 시도 코드와 함께 넘겨야 합니다.\n"
        "- 일반구를 둔 시(예: 수원시)는 실거래가 구 단위로만 집계되므로 빼고, 구를 `수원시 장안구`처럼 시 이름과 "
        "함께 표기합니다.\n"
        "- 시군구가 없는 세종특별자치시는 시군구명에 시도명을 그대로 씁니다.\n"
        "- 법정동코드 갱신 때마다 목록이 바뀔 수 있습니다. 아직 적재 전이면 `items`가 빈 목록입니다.\n"
        "- 시도와 시군구 모두 코드 오름차순입니다."
    ),
)
async def get_region_list(
    session: Annotated[AsyncSession, Depends(get_session)],
):
    """시도·시군구 목록을 조회한다."""
    async with session.begin():
        return {"items": await get_regions(session)}
