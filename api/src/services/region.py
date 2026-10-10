from itertools import groupby
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..model.region import Region


async def get_regions(session: AsyncSession) -> list[dict[str, Any]]:
    """시도별로 시군구를 묶은 지역 목록을 조회한다."""
    result = await session.scalars(select(Region).order_by(Region.sido_code, Region.sigungu_code))
    sidos: list[dict[str, Any]] = []
    # 시도 코드 순으로 정렬해 읽었으므로 연속한 행만 묶어도 시도당 한 묶음이 된다.
    for sido_code, group in groupby(result, key=lambda region: region.sido_code):
        sigungus = list(group)
        sidos.append(
            {
                "sido_code": sido_code,
                # 적재 시 같은 시도의 이름이 하나임을 검사하므로 첫 행의 이름을 쓴다.
                "sido_name": sigungus[0].sido_name,
                "sigungus": sigungus,
            }
        )
    return sidos
