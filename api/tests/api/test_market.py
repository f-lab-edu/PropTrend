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
PRICE_MOVERS_URL = "/api/market/price-movers"

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


# 급등/급락 스위트의 기준일. 비교 기간은 2016-10-05부터, 최근 거래는 2025-10-05부터 인정한다.
PRICE_MOVERS_BASE_DATE = date(2026, 10, 5)
APT_ROW = BASE_SALE_ROW | {"deal_amount": 1_000_000_000}
OFFICETEL_ROW = BASE_SALE_ROW | {
    "property_type": PropertyType.OFFICETEL,
    "jibun": "200-1",
    "building_name": "역삼오피스텔",
    "apartment_serial_number": None,
    "apartment_dong": None,
    "exclusive_use_area": 30.5,
}


class TestGetPriceMovers:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, SaleTransaction]]:
        # 묶음마다 직전 거래(_prev)와 최근 거래(_latest)를 둔다.
        prev, latest = {"deal_date": date(2025, 1, 10)}, {"deal_date": date(2026, 9, 1)}
        # 순위에서 빠져야 하는 묶음은 빠지지 않으면 1위가 되도록 10배로 뛴 금액을 준다.
        jump_prev, jump_latest = prev | {"deal_amount": 100_000_000}, latest | {"deal_amount": 1_000_000_000}
        row_house = BASE_SALE_ROW | {
            "property_type": PropertyType.ROW_HOUSE,
            "house_type": "연립",
            "apartment_serial_number": None,
            "apartment_dong": None,
        }
        single_multi = row_house | {"property_type": PropertyType.SINGLE_MULTI, "house_type": "단독"}
        no_serial = APT_ROW | {"apartment_serial_number": None, "building_name": "일련번호없음"}
        sale_rows = {
            # 급등: +50, +40, +30, +20(id 작음), +20(id 큼), +10 → 5위까지만 나온다.
            "s1_prev": APT_ROW | {"apartment_serial_number": "11680-2001"} | prev,
            "s1_latest": APT_ROW | {"apartment_serial_number": "11680-2001", "deal_amount": 1_500_000_000} | latest,
            "s2_prev": OFFICETEL_ROW | {"deal_amount": 200_000_000} | prev,
            "s2_latest": OFFICETEL_ROW | {"deal_amount": 280_000_000, "floor": 7} | latest,
            "s3_prev": APT_ROW | {"apartment_serial_number": "11680-2003"} | prev,
            "s3_latest": APT_ROW | {"apartment_serial_number": "11680-2003", "deal_amount": 1_300_000_000} | latest,
            "s4_prev": APT_ROW | {"apartment_serial_number": "11680-2004"} | prev,
            "s4_latest": APT_ROW | {"apartment_serial_number": "11680-2004", "deal_amount": 1_200_000_000} | latest,
            "s5_prev": APT_ROW | {"apartment_serial_number": "11680-2005", "deal_amount": 500_000_000} | prev,
            "s5_latest": APT_ROW | {"apartment_serial_number": "11680-2005", "deal_amount": 600_000_000} | latest,
            "s6_prev": APT_ROW | {"apartment_serial_number": "11680-2006"} | prev,
            "s6_latest": APT_ROW | {"apartment_serial_number": "11680-2006", "deal_amount": 1_100_000_000} | latest,
            # 급락: -33.33(반올림), -20(해제 거래 건너뜀), -10(오피스텔 NULL 키), -5(s1과 같은 단지, 다른 면적).
            "p1_prev": OFFICETEL_ROW | {"jibun": "300-1", "deal_amount": 300_000_000} | prev,
            "p1_latest": OFFICETEL_ROW | {"jibun": "300-1", "deal_amount": 200_000_000} | latest,
            "p2_prev": APT_ROW | {"apartment_serial_number": "11680-2102"} | prev,
            "p2_latest": APT_ROW
            | {"apartment_serial_number": "11680-2102", "deal_amount": 800_000_000, "deal_date": date(2026, 3, 1)},
            "p2_cancelled": APT_ROW
            | {"apartment_serial_number": "11680-2102", "deal_amount": 3_000_000_000, "cancel_deal_type": "O"}
            | latest,
            "p3_prev": OFFICETEL_ROW | {"jibun": "400-1", "building_name": None, "deal_amount": 400_000_000} | prev,
            "p3_latest": OFFICETEL_ROW | {"jibun": "400-1", "building_name": None, "deal_amount": 360_000_000} | latest,
            "p4_prev": APT_ROW
            | {"apartment_serial_number": "11680-2001", "exclusive_use_area": 59.9, "deal_amount": 600_000_000}
            | {"deal_date": date(2025, 2, 10)},
            "p4_latest": APT_ROW
            | {"apartment_serial_number": "11680-2001", "exclusive_use_area": 59.9, "deal_amount": 570_000_000}
            | {"deal_date": date(2026, 8, 1)},
            # 순위에서 빠지는 묶음.
            "single": APT_ROW | {"apartment_serial_number": "11680-2301"} | jump_latest,
            "too_old_prev": APT_ROW
            | {"apartment_serial_number": "11680-2302", "deal_amount": 100_000_000, "deal_date": date(2016, 10, 4)},
            "too_old_latest": APT_ROW | {"apartment_serial_number": "11680-2302"} | jump_latest,
            "stale_prev": APT_ROW | {"apartment_serial_number": "11680-2303"} | jump_prev,
            "stale_latest": APT_ROW
            | {"apartment_serial_number": "11680-2303", "deal_amount": 1_000_000_000, "deal_date": date(2025, 10, 4)},
            "unchanged_prev": APT_ROW | {"apartment_serial_number": "11680-2304"} | prev,
            "unchanged_latest": APT_ROW | {"apartment_serial_number": "11680-2304"} | latest,
            "row_house_prev": row_house | jump_prev,
            "row_house_latest": row_house | jump_latest,
            "single_multi_prev": single_multi | jump_prev,
            "single_multi_latest": single_multi | jump_latest,
            "no_serial_prev": no_serial | jump_prev,
            "no_serial_latest": no_serial | jump_latest,
        }
        async with (
            seed_rows(session_factory, LegalDongCodeRawItem, LEGAL_DONG_CODE_ROWS),
            seed_rows(session_factory, SaleTransaction, list(sale_rows.values())) as sales,
        ):
            yield dict(zip(sale_rows, sales, strict=True))

    async def test_returns_top_surge_and_plunge(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """아파트·오피스텔을 합쳐 변동률 순으로 급등·급락 각 5개까지 주고, 동률이면 최근 거래 id 순이다."""
        app.dependency_overrides[get_today] = lambda: PRICE_MOVERS_BASE_DATE

        response = await client.get(PRICE_MOVERS_URL)

        assert response.status_code == 200
        body = response.json()
        assert body["base_date"] == "2026-10-05"
        assert body["surge"][0] == {
            "change_rate": 50.0,
            "latest_sale": {
                "id": seed["s1_latest"].id,
                "property_type": "APT",
                "deal_date": "2026-09-01",
                "deal_amount": 1_500_000_000,
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
            "previous_sale": {
                "id": seed["s1_prev"].id,
                "deal_date": "2025-01-10",
                "deal_amount": 1_000_000_000,
                "floor": 10,
            },
        }
        assert [
            (mover["latest_sale"]["id"], mover["previous_sale"]["id"], mover["change_rate"]) for mover in body["surge"]
        ] == [
            (seed["s1_latest"].id, seed["s1_prev"].id, 50.0),
            (seed["s2_latest"].id, seed["s2_prev"].id, 40.0),
            (seed["s3_latest"].id, seed["s3_prev"].id, 30.0),
            (seed["s4_latest"].id, seed["s4_prev"].id, 20.0),
            (seed["s5_latest"].id, seed["s5_prev"].id, 20.0),
        ]
        assert [
            (mover["latest_sale"]["id"], mover["previous_sale"]["id"], mover["change_rate"]) for mover in body["plunge"]
        ] == [
            (seed["p1_latest"].id, seed["p1_prev"].id, -33.33),
            (seed["p2_latest"].id, seed["p2_prev"].id, -20.0),
            (seed["p3_latest"].id, seed["p3_prev"].id, -10.0),
            (seed["p4_latest"].id, seed["p4_prev"].id, -5.0),
        ]

    async def test_excludes_ineligible_transactions(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """거래가 1건이거나 기간 밖이거나 대상 유형이 아니거나 단지 일련번호가 없거나 변동이 없는 묶음은 빠진다."""
        app.dependency_overrides[get_today] = lambda: PRICE_MOVERS_BASE_DATE

        response = await client.get(PRICE_MOVERS_URL)

        assert response.status_code == 200
        body = response.json()
        ranked_ids = {mover["latest_sale"]["id"] for mover in body["surge"] + body["plunge"]}
        excluded = ("single", "too_old_latest", "stale_latest", "unchanged_latest", "p2_cancelled")
        excluded += ("row_house_latest", "single_multi_latest", "no_serial_latest")
        assert ranked_ids.isdisjoint(seed[name].id for name in excluded)

    async def test_groups_officetel_with_null_keys(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """건물명이 NULL인 오피스텔도 NULL끼리 한 묶음으로 비교한다."""
        app.dependency_overrides[get_today] = lambda: PRICE_MOVERS_BASE_DATE

        response = await client.get(PRICE_MOVERS_URL)

        assert response.status_code == 200
        null_key_mover = next(
            mover for mover in response.json()["plunge"] if mover["latest_sale"]["id"] == seed["p3_latest"].id
        )
        assert null_key_mover["latest_sale"]["property_type"] == "OFFICETEL"
        assert null_key_mover["latest_sale"]["building_name"] is None
        assert null_key_mover["previous_sale"]["id"] == seed["p3_prev"].id

    async def test_returns_empty_lists_when_no_recent_transaction(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """기준일로부터 1년 안에 거래가 있는 묶음이 없으면 급등·급락 모두 빈 목록이다."""
        app.dependency_overrides[get_today] = lambda: date(2028, 1, 1)

        response = await client.get(PRICE_MOVERS_URL)

        assert response.status_code == 200
        assert response.json() == {"base_date": "2028-01-01", "surge": [], "plunge": []}
