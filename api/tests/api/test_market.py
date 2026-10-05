"""시장 지표 API 테스트."""

from collections.abc import AsyncIterator
from datetime import date

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.dependencies import get_today
from src.main import app
from src.model import LegalDongCodeRawItem, PropertyType, RentTransaction, SaleTransaction

from .conftest import seed_rows
from .test_prop_transaction import RENT_ROW as BASE_RENT_ROW
from .test_prop_transaction import SALE_ROW as BASE_SALE_ROW

# 세션 범위 엔진의 커넥션을 같은 이벤트 루프에서 재사용한다.
pytestmark = pytest.mark.asyncio(loop_scope="session")

DAILY_SUMMARY_URL = "/api/market/daily-summary"

LEGAL_DONG_CODE_ROWS = [
    {"payload": {"sido_cd": "11", "sgg_cd": "680", "locatadd_nm": "서울특별시 강남구"}},
    {"payload": {"sido_cd": "26", "sgg_cd": "350", "locatadd_nm": "부산광역시 해운대구"}},
]

# 스위트는 이 행에서 필요한 값만 바꿔 시드를 만든다. 계약일은 기준일(2026-10-04)이다.
SALE_ROW = BASE_SALE_ROW | {"deal_date": date(2026, 10, 4), "deal_amount": 3_000_000_000}
RENT_ROW = BASE_RENT_ROW | {"deal_date": date(2026, 10, 4)}


class TestGetDailySummary:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, SaleTransaction]]:
        sale_rows = {
            "highest": SALE_ROW,
            # 최고가와 금액이 같지만 나중에 넣어 id가 크다.
            "highest_tie": SALE_ROW | {"jibun": "123-5", "floor": 15},
            "middle": SALE_ROW | {"property_type": PropertyType.OFFICETEL, "deal_amount": 500_000_000},
            "lowest": SALE_ROW
            | {
                "property_type": PropertyType.SINGLE_MULTI,
                "house_type": "단독",
                "sido_code": "26",
                "sigungu_code": "350",
                "umd_name": "우동",
                "jibun": "1**",
                "building_name": None,
                "apartment_dong": None,
                "deal_amount": 11_000_000,
                "exclusive_use_area": None,
                "floor": None,
                "build_year": None,
                "total_floor_area": 120.5,
                "plottage_area": 200.0,
            },
            "cancelled_high": SALE_ROW | {"deal_amount": 9_000_000_000, "cancel_deal_type": "O"},
            "cancelled_low": SALE_ROW | {"deal_amount": 1_000_000, "cancel_deal_type": "O"},
            "day_before": SALE_ROW | {"deal_date": date(2026, 10, 3), "deal_amount": 8_000_000_000},
            "today": SALE_ROW | {"deal_date": date(2026, 10, 5), "deal_amount": 2_000_000},
        }
        rent_rows = [
            RENT_ROW,
            RENT_ROW | {"property_type": PropertyType.ROW_HOUSE, "deposit": 10_000_000, "monthly_rent": 500_000},
            RENT_ROW | {"deal_date": date(2026, 10, 3)},
            RENT_ROW | {"deal_date": date(2026, 10, 5)},
            # 매매 없이 전월세만 있는 날.
            RENT_ROW | {"deal_date": date(2026, 10, 2)},
            RENT_ROW | {"deal_date": date(2026, 10, 2), "monthly_rent": 1_000_000},
        ]
        async with (
            seed_rows(session_factory, LegalDongCodeRawItem, LEGAL_DONG_CODE_ROWS),
            seed_rows(session_factory, SaleTransaction, list(sale_rows.values())) as sales,
            seed_rows(session_factory, RentTransaction, rent_rows),
        ):
            yield dict(zip(sale_rows, sales, strict=True))

    async def test_returns_summary_of_previous_day(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """호출일 전날 계약된 매매의 최고가·최저가 거래와 매매·전월세 합계 건수를 해제 거래를 빼고 준다."""
        app.dependency_overrides[get_today] = lambda: date(2026, 10, 5)

        response = await client.get(DAILY_SUMMARY_URL)

        assert response.status_code == 200
        assert response.json() == {
            "deal_date": "2026-10-04",
            "highest_sale": {
                "id": seed["highest"].id,
                "property_type": "APT",
                "deal_date": "2026-10-04",
                "deal_amount": 3_000_000_000,
                "dealing_type": "중개거래",
                "house_type": None,
                "building_name": "역삼래미안",
                "apartment_dong": "101",
                "floor": 10,
                "build_year": 2005,
                "exclusive_use_area": 84.97,
                "total_floor_area": None,
                "plottage_area": None,
                "land_area": None,
                "address": "서울특별시 강남구 역삼동 123-4",
            },
            "lowest_sale": {
                "id": seed["lowest"].id,
                "property_type": "SINGLE_MULTI",
                "deal_date": "2026-10-04",
                "deal_amount": 11_000_000,
                "dealing_type": "중개거래",
                "house_type": "단독",
                "building_name": None,
                "apartment_dong": None,
                "floor": None,
                "build_year": None,
                "exclusive_use_area": None,
                "total_floor_area": 120.5,
                "plottage_area": 200.0,
                "land_area": None,
                "address": "부산광역시 해운대구 우동 1**",
            },
            # 해제되지 않은 매매 4건 + 전월세 2건
            "transaction_count": 6,
        }

    async def test_returns_null_sales_when_no_sale_on_previous_day(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """전날 매매가 없으면 최고가·최저가는 null이고 건수는 전월세만 센다."""
        app.dependency_overrides[get_today] = lambda: date(2026, 10, 3)

        response = await client.get(DAILY_SUMMARY_URL)

        assert response.status_code == 200
        assert response.json() == {
            "deal_date": "2026-10-02",
            "highest_sale": None,
            "lowest_sale": None,
            "transaction_count": 2,
        }

    async def test_returns_zero_count_when_no_transaction_on_previous_day(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """전날 거래가 하나도 없으면 최고가·최저가는 null이고 건수는 0이다."""
        app.dependency_overrides[get_today] = lambda: date(2026, 10, 2)

        response = await client.get(DAILY_SUMMARY_URL)

        assert response.status_code == 200
        assert response.json() == {
            "deal_date": "2026-10-01",
            "highest_sale": None,
            "lowest_sale": None,
            "transaction_count": 0,
        }
