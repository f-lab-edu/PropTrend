"""시도·시군구 목록 조회 API 테스트."""

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from fastapi import status
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.model import Region

from .conftest import seed_rows

# 세션 범위 엔진의 커넥션을 같은 이벤트 루프에서 재사용한다.
pytestmark = pytest.mark.asyncio(loop_scope="session")

REGIONS_URL = "/api/regions"

# 코드 순 정렬을 확인하려고 일부러 섞어 넣는다.
REGION_ROWS = [
    {"sido_code": "41", "sigungu_code": "113", "sido_name": "경기도", "sigungu_name": "수원시 권선구"},
    {"sido_code": "11", "sigungu_code": "710", "sido_name": "서울특별시", "sigungu_name": "송파구"},
    {"sido_code": "41", "sigungu_code": "111", "sido_name": "경기도", "sigungu_name": "수원시 장안구"},
    {"sido_code": "11", "sigungu_code": "110", "sido_name": "서울특별시", "sigungu_name": "종로구"},
]


class TestGetRegions:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[list[Region]]:
        async with seed_rows(session_factory, Region, REGION_ROWS) as regions:
            yield regions

    # 클래스 범위 시드는 처음 요청하는 테스트에서 만들어지므로, 시드가 없는 테스트를 먼저 둔다.
    async def test_returns_empty_list_before_load(self, client: httpx.AsyncClient) -> None:
        """법정동코드 적재 전이라 지역이 없으면 빈 목록을 준다."""
        response = await client.get(REGIONS_URL)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"items": []}

    @pytest.mark.usefixtures("seed")
    async def test_returns_sigungus_grouped_by_sido(self, client: httpx.AsyncClient) -> None:
        """시도마다 그 시도의 시군구만 묶고, 시도와 시군구 모두 코드 순으로 준다."""
        response = await client.get(REGIONS_URL)

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {
            "items": [
                {
                    "sido_code": "11",
                    "sido_name": "서울특별시",
                    "sigungus": [
                        {"sigungu_code": "110", "sigungu_name": "종로구"},
                        {"sigungu_code": "710", "sigungu_name": "송파구"},
                    ],
                },
                {
                    "sido_code": "41",
                    "sido_name": "경기도",
                    "sigungus": [
                        {"sigungu_code": "111", "sigungu_name": "수원시 장안구"},
                        {"sigungu_code": "113", "sigungu_name": "수원시 권선구"},
                    ],
                },
            ]
        }
