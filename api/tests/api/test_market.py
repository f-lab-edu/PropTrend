"""시장 지표 API 테스트."""

from collections.abc import AsyncIterator
from datetime import UTC, date, datetime

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.dependencies import get_today
from src.main import app
from src.model import (
    Complex,
    LegalDongCodeRawItem,
    MarketPriceMover,
    MarketSnapshot,
    PropertyType,
    RentTransaction,
    SaleTransaction,
)

from .conftest import seed_rows
from .test_prop_transaction import APARTMENT_COMPLEX_ROW, seed_complexes
from .test_prop_transaction import RENT_ROW as BASE_RENT_ROW
from .test_prop_transaction import SALE_ROW as BASE_SALE_ROW

# 세션 범위 엔진의 커넥션을 같은 이벤트 루프에서 재사용한다.
pytestmark = pytest.mark.asyncio(loop_scope="session")

DAILY_SUMMARY_URL = "/api/market/daily-summary"
PRICE_MOVERS_URL = "/api/market/price-movers"
VOLUME_SURGE_REGIONS_URL = "/api/market/volume-surge-regions"

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
        single_multi = SALE_ROW | {
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
        }
        sale_rows = {
            "apartment_highest": SALE_ROW,
            # 최고가와 금액이 같지만 나중에 넣어 id가 크다.
            "apartment_highest_tie": SALE_ROW | {"jibun": "123-5", "floor": 15},
            "apartment_lowest": SALE_ROW | {"deal_amount": 900_000_000, "floor": 3},
            "single_multi_highest": single_multi
            | {"jibun": "2**", "deal_amount": 800_000_000, "total_floor_area": 250.0, "plottage_area": 300.0},
            "single_multi_lowest": single_multi,
            # 집계 대상이 아닌 유형은 금액이 가장 크거나 작아도 고르지 않는다.
            "officetel": SALE_ROW | {"property_type": PropertyType.OFFICETEL, "deal_amount": 10_000_000_000},
            "row_house": SALE_ROW | {"property_type": PropertyType.ROW_HOUSE, "deal_amount": 5_000_000},
            "cancelled_high": SALE_ROW | {"deal_amount": 9_000_000_000, "cancel_deal_type": "O"},
            "cancelled_low": single_multi | {"deal_amount": 1_000_000, "cancel_deal_type": "O"},
            "day_before": SALE_ROW | {"deal_date": date(2026, 10, 3), "deal_amount": 8_000_000_000},
            "day_after": SALE_ROW | {"deal_date": date(2026, 10, 5), "deal_amount": 2_000_000},
        }
        rent_rows = [
            RENT_ROW,
            RENT_ROW | {"property_type": PropertyType.SINGLE_MULTI, "deposit": 200_000_000},
            RENT_ROW | {"property_type": PropertyType.OFFICETEL, "deposit": 300_000_000},
            RENT_ROW | {"property_type": PropertyType.ROW_HOUSE, "deposit": 10_000_000, "monthly_rent": 500_000},
            RENT_ROW | {"deal_date": date(2026, 10, 3)},
            RENT_ROW | {"deal_date": date(2026, 10, 5)},
            # 매매 없이 전월세만 있는 날.
            RENT_ROW | {"deal_date": date(2026, 10, 2)},
            RENT_ROW | {"deal_date": date(2026, 10, 2), "monthly_rent": 1_000_000},
            RENT_ROW | {"deal_date": date(2026, 10, 2), "property_type": PropertyType.SINGLE_MULTI},
        ]
        async with (
            seed_rows(session_factory, LegalDongCodeRawItem, LEGAL_DONG_CODE_ROWS),
            seed_rows(session_factory, SaleTransaction, list(sale_rows.values())) as sales,
            seed_rows(session_factory, RentTransaction, rent_rows),
        ):
            yield dict(zip(sale_rows, sales, strict=True))

    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def complexes(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Complex]]:
        async with seed_complexes(session_factory, {"apartment": APARTMENT_COMPLEX_ROW}) as complexes:
            yield complexes

    async def test_returns_summary_of_month_ago(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction], complexes: dict[str, Complex]
    ) -> None:
        """호출일 1개월 전 계약된 거래를 아파트·단독다가구로 나눠 매매 최고가·최저가와 매매·전월세 합계 건수를 준다.

        오피스텔·연립다세대와 해제된 매매는 뺀다.
        """
        app.dependency_overrides[get_today] = lambda: date(2026, 11, 4)

        response = await client.get(DAILY_SUMMARY_URL)

        assert response.status_code == 200
        apartment_sale = {
            "id": seed["apartment_highest"].id,
            "property_type": "APT",
            "complex_id": complexes["apartment"].id,
            "deal_date": "2026-10-04",
            "deal_amount": 3_000_000_000,
            "dealing_type": "중개거래",
            "cancel_deal_date": None,
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
        }
        single_multi_sale = {
            "id": seed["single_multi_lowest"].id,
            "property_type": "SINGLE_MULTI",
            "complex_id": None,
            "deal_date": "2026-10-04",
            "deal_amount": 11_000_000,
            "dealing_type": "중개거래",
            "cancel_deal_date": None,
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
        }
        assert response.json() == {
            "deal_date": "2026-10-04",
            "apartment": {
                "highest_sale": apartment_sale,
                "lowest_sale": apartment_sale
                | {"id": seed["apartment_lowest"].id, "deal_amount": 900_000_000, "floor": 3},
                # 해제되지 않은 매매 3건 + 전월세 1건
                "transaction_count": 4,
            },
            "single_multi": {
                "highest_sale": single_multi_sale
                | {
                    "id": seed["single_multi_highest"].id,
                    "deal_amount": 800_000_000,
                    "total_floor_area": 250.0,
                    "plottage_area": 300.0,
                    "address": "부산광역시 해운대구 우동 2**",
                },
                "lowest_sale": single_multi_sale,
                # 해제되지 않은 매매 2건 + 전월세 1건
                "transaction_count": 3,
            },
        }

    async def test_returns_null_sales_when_no_sale_on_month_ago(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """1개월 전 날짜에 매매가 없으면 최고가·최저가는 null이고 건수는 유형별 전월세만 센다."""
        app.dependency_overrides[get_today] = lambda: date(2026, 11, 2)

        response = await client.get(DAILY_SUMMARY_URL)

        assert response.status_code == 200
        assert response.json() == {
            "deal_date": "2026-10-02",
            "apartment": {"highest_sale": None, "lowest_sale": None, "transaction_count": 2},
            "single_multi": {"highest_sale": None, "lowest_sale": None, "transaction_count": 1},
        }

    async def test_returns_zero_count_when_no_transaction_on_month_ago(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """1개월 전 날짜에 거래가 하나도 없으면 최고가·최저가는 null이고 건수는 0이다."""
        app.dependency_overrides[get_today] = lambda: date(2026, 11, 1)

        response = await client.get(DAILY_SUMMARY_URL)

        assert response.status_code == 200
        assert response.json() == {
            "deal_date": "2026-10-01",
            "apartment": {"highest_sale": None, "lowest_sale": None, "transaction_count": 0},
            "single_multi": {"highest_sale": None, "lowest_sale": None, "transaction_count": 0},
        }

    async def test_clamps_deal_date_to_month_end(self, client: httpx.AsyncClient) -> None:
        """전월에 같은 날이 없으면 전월 말일을 기준일로 삼는다."""
        app.dependency_overrides[get_today] = lambda: date(2026, 10, 31)

        response = await client.get(DAILY_SUMMARY_URL)

        assert response.status_code == 200
        assert response.json() == {
            "deal_date": "2026-09-30",
            "apartment": {"highest_sale": None, "lowest_sale": None, "transaction_count": 0},
            "single_multi": {"highest_sale": None, "lowest_sale": None, "transaction_count": 0},
        }


# 정기 갱신이 계산해 둔 결과 행. 스위트는 이 행에서 순위에 쓰는 값만 바꿔 시드를 만든다.
PRICE_MOVER_ROW = {
    "change_rate": 50.0,
    "latest_id": 101,
    "sido_code": "11",
    "sigungu_code": "680",
    "umd_name": "역삼동",
    "jibun": "123-4",
    "building_name": "역삼래미안",
    "apartment_dong": "101",
    "floor": 10,
    "build_year": 2005,
    "exclusive_use_area": 84.97,
    "deal_date": date(2026, 9, 1),
    "deal_amount": 1_500_000_000,
    "dealing_type": "중개거래",
    "cancel_deal_date": None,
    "complex_id": 7,
    "previous_id": 11,
    "previous_deal_date": date(2025, 1, 10),
    "previous_deal_amount": 1_000_000_000,
    "previous_floor": 9,
}
PRICE_MOVERS_SNAPSHOT_ROW = {
    "name": MarketPriceMover.__tablename__,
    "base_date": date(2026, 10, 5),
    "refreshed_at": datetime(2026, 10, 4, 19, 30, tzinfo=UTC),
}


class TestGetPriceMovers:
    async def test_returns_saved_movers_in_rank_order(
        self, client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        """계산해 둔 결과를 급등·급락으로 나눠 변동률 순으로 주고, 동률이면 최근 거래 id 순이다."""
        # 결과 테이블은 순서를 보장하지 않으므로 순위와 다른 순서로 넣는다.
        rows = [
            PRICE_MOVER_ROW | {"change_rate": 20.0, "latest_id": 300},
            PRICE_MOVER_ROW | {"change_rate": -5.0, "latest_id": 400},
            PRICE_MOVER_ROW,
            PRICE_MOVER_ROW | {"change_rate": -33.33, "latest_id": 500, "complex_id": None},
            PRICE_MOVER_ROW | {"change_rate": 20.0, "latest_id": 200},
        ]
        async with (
            seed_rows(session_factory, LegalDongCodeRawItem, LEGAL_DONG_CODE_ROWS),
            seed_rows(session_factory, MarketSnapshot, [PRICE_MOVERS_SNAPSHOT_ROW]),
            seed_rows(session_factory, MarketPriceMover, rows),
        ):
            response = await client.get(PRICE_MOVERS_URL)

        assert response.status_code == 200
        body = response.json()
        assert body["base_date"] == "2026-10-05"
        assert body["surge"][0] == {
            "change_rate": 50.0,
            "latest_sale": {
                "id": 101,
                "property_type": "APT",
                "complex_id": 7,
                "deal_date": "2026-09-01",
                "deal_amount": 1_500_000_000,
                "dealing_type": "중개거래",
                "cancel_deal_date": None,
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
            "previous_sale": {"id": 11, "deal_date": "2025-01-10", "deal_amount": 1_000_000_000, "floor": 9},
        }
        assert [(mover["latest_sale"]["id"], mover["change_rate"]) for mover in body["surge"]] == [
            (101, 50.0),
            (200, 20.0),
            (300, 20.0),
        ]
        assert [(mover["latest_sale"]["id"], mover["change_rate"]) for mover in body["plunge"]] == [
            (500, -33.33),
            (400, -5.0),
        ]
        assert body["plunge"][0]["latest_sale"]["complex_id"] is None

    async def test_returns_empty_lists_when_no_saved_mover(
        self, client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
    ) -> None:
        """계산 결과가 0건이어도 기준일과 함께 빈 급등·급락 목록을 준다."""
        async with seed_rows(session_factory, MarketSnapshot, [PRICE_MOVERS_SNAPSHOT_ROW]):
            response = await client.get(PRICE_MOVERS_URL)

        assert response.status_code == 200
        assert response.json() == {"base_date": "2026-10-05", "surge": [], "plunge": []}

    async def test_returns_503_before_first_refresh(self, client: httpx.AsyncClient) -> None:
        """정기 갱신이 아직 한 번도 계산하지 않았으면 503이다."""
        response = await client.get(PRICE_MOVERS_URL)

        assert response.status_code == 503
        assert response.json()["message"] == "거래액 급등/급락 단지가 아직 집계되지 않았습니다"


# 거래량 급등 스위트의 기준일. 최근 구간은 2026-08-05~09-04, 이전 구간은 2026-07-05~08-04다.
VOLUME_SURGE_BASE_DATE = date(2026, 10, 5)
VOLUME_SURGE_LEGAL_DONG_CODE_ROWS = LEGAL_DONG_CODE_ROWS + [
    {"payload": {"sido_cd": "11", "sgg_cd": "650", "locatadd_nm": "서울특별시 서초구"}},
    {"payload": {"sido_cd": "11", "sgg_cd": "710", "locatadd_nm": "서울특별시 송파구"}},
    {"payload": {"sido_cd": "41", "sgg_cd": "135", "locatadd_nm": "경기도 성남시 분당구"}},
    {"payload": {"sido_cd": "41", "sgg_cd": "465", "locatadd_nm": "경기도 용인시 수지구"}},
]


class TestGetVolumeSurgeRegions:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[None]:
        gangnam = SALE_ROW | {"sido_code": "11", "sigungu_code": "680"}
        seocho = SALE_ROW | {"sido_code": "11", "sigungu_code": "650"}
        songpa = SALE_ROW | {"sido_code": "11", "sigungu_code": "710"}
        haeundae = SALE_ROW | {"property_type": PropertyType.OFFICETEL, "sido_code": "26", "sigungu_code": "350"}
        bundang = SALE_ROW | {"sido_code": "41", "sigungu_code": "135"}
        suji = SALE_ROW | {"sido_code": "41", "sigungu_code": "465"}
        # 법정동코드에 없는 지역.
        unnamed = SALE_ROW | {"sido_code": "50", "sigungu_code": "999"}
        recent, previous = {"deal_date": date(2026, 8, 20)}, {"deal_date": date(2026, 7, 20)}
        # 기준일을 2026-11-05로 옮기면 최근 구간이 되는 날짜. 2026-10-05 기준으로는 신고 기한 전이라 세지 않는다.
        unreported = {"deal_date": date(2026, 9, 20)}
        sale_rows = [
            # 강남구 최근 7건, 이전 2건 → +5. 구간 경계일을 포함한다.
            *[gangnam | recent] * 5,
            gangnam | {"deal_date": date(2026, 8, 5)},
            gangnam | {"deal_date": date(2026, 9, 4)},
            gangnam | {"deal_date": date(2026, 7, 5)},
            gangnam | {"deal_date": date(2026, 8, 4)},
            # 강남구에서 세지 않는 매매: 구간 밖, 해제.
            gangnam | {"deal_date": date(2026, 9, 5)},
            gangnam | {"deal_date": date(2026, 7, 4)},
            gangnam | recent | {"cancel_deal_type": "O"},
            # 서초구 최근 4건, 이전 0건 → +4.
            *[seocho | recent] * 4,
            # 해운대구 최근 4건, 이전 2건 → +2.
            *[haeundae | recent] * 4,
            *[haeundae | previous] * 2,
            # 이름 없는 지역 최근 3건, 이전 1건 → +2. 해운대구와 동률이라 지역코드 순으로 뒤에 온다.
            *[unnamed | recent] * 3,
            unnamed | previous,
            # 분당구 최근 2건, 이전 1건 → +1.
            *[bundang | recent] * 2,
            bundang | previous,
            # 수지구 최근 1건, 이전 0건 → +1. 분당구와 동률이고 지역코드가 커서 6위로 빠진다.
            suji | recent,
            # 2026-11-05 기준 최근 구간: 강남구 1건(위 09-05) → -6, 서초구 4건 → 0, 송파구 2건 → +2, 해운대구 5건 → +1.
            *[seocho | unreported] * 4,
            *[songpa | unreported] * 2,
            *[haeundae | unreported] * 5,
        ]
        # 전월세는 거래량에 넣지 않는다.
        rent_rows = [RENT_ROW | {"sido_code": "11", "sigungu_code": "680"} | recent] * 10
        async with (
            seed_rows(session_factory, LegalDongCodeRawItem, VOLUME_SURGE_LEGAL_DONG_CODE_ROWS),
            seed_rows(session_factory, SaleTransaction, sale_rows),
            seed_rows(session_factory, RentTransaction, rent_rows),
        ):
            yield

    async def test_returns_top_regions_by_count_increase(self, client: httpx.AsyncClient, seed: None) -> None:
        """매매 건수 증가 폭 순으로 5개 지역을 주고, 동률이면 지역코드 순이다."""
        app.dependency_overrides[get_today] = lambda: VOLUME_SURGE_BASE_DATE

        response = await client.get(VOLUME_SURGE_REGIONS_URL)

        assert response.status_code == 200
        assert response.json() == {
            "base_date": "2026-10-05",
            "recent_start_date": "2026-08-05",
            "recent_end_date": "2026-09-04",
            "previous_start_date": "2026-07-05",
            "previous_end_date": "2026-08-04",
            "regions": [
                {
                    "region_code": "11680",
                    "region_name": "서울특별시 강남구",
                    "recent_count": 7,
                    "previous_count": 2,
                    "count_change": 5,
                },
                {
                    "region_code": "11650",
                    "region_name": "서울특별시 서초구",
                    "recent_count": 4,
                    "previous_count": 0,
                    "count_change": 4,
                },
                {
                    "region_code": "26350",
                    "region_name": "부산광역시 해운대구",
                    "recent_count": 4,
                    "previous_count": 2,
                    "count_change": 2,
                },
                {
                    "region_code": "50999",
                    "region_name": None,
                    "recent_count": 3,
                    "previous_count": 1,
                    "count_change": 2,
                },
                {
                    "region_code": "41135",
                    "region_name": "경기도 성남시 분당구",
                    "recent_count": 2,
                    "previous_count": 1,
                    "count_change": 1,
                },
            ],
        }

    async def test_counts_only_eligible_sales_in_periods(self, client: httpx.AsyncClient, seed: None) -> None:
        """구간 경계일은 세고, 구간 밖 매매·해제된 매매·전월세는 세지 않는다."""
        app.dependency_overrides[get_today] = lambda: VOLUME_SURGE_BASE_DATE

        response = await client.get(VOLUME_SURGE_REGIONS_URL)

        assert response.status_code == 200
        gangnam = next(region for region in response.json()["regions"] if region["region_code"] == "11680")
        assert (gangnam["recent_count"], gangnam["previous_count"]) == (7, 2)

    async def test_excludes_regions_without_increase(self, client: httpx.AsyncClient, seed: None) -> None:
        """건수가 줄었거나 그대로인 지역은 빠져 5개보다 적을 수 있다."""
        app.dependency_overrides[get_today] = lambda: date(2026, 11, 5)

        response = await client.get(VOLUME_SURGE_REGIONS_URL)

        assert response.status_code == 200
        body = response.json()
        assert (body["recent_start_date"], body["recent_end_date"]) == ("2026-09-05", "2026-10-04")
        assert [(region["region_code"], region["count_change"]) for region in body["regions"]] == [
            ("11710", 2),
            ("26350", 1),
        ]

    async def test_returns_empty_list_when_no_increase(self, client: httpx.AsyncClient, seed: None) -> None:
        """증가한 지역이 없으면 빈 목록이고, 월말 기준일의 구간은 각 달의 말일로 맞춰진다."""
        app.dependency_overrides[get_today] = lambda: date(2028, 3, 31)

        response = await client.get(VOLUME_SURGE_REGIONS_URL)

        assert response.status_code == 200
        assert response.json() == {
            "base_date": "2028-03-31",
            "recent_start_date": "2028-01-31",
            "recent_end_date": "2028-02-28",
            "previous_start_date": "2027-12-31",
            "previous_end_date": "2028-01-30",
            "regions": [],
        }
