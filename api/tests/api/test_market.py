"""시장 지표 API 테스트."""

from collections.abc import AsyncIterator
from datetime import date

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.dependencies import get_today
from src.main import app
from src.model import Complex, LegalDongCodeRawItem, PropertyType, RentTransaction, SaleTransaction

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
            "day_after": SALE_ROW | {"deal_date": date(2026, 10, 5), "deal_amount": 2_000_000},
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

    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def complexes(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Complex]]:
        async with seed_complexes(session_factory, {"apartment": APARTMENT_COMPLEX_ROW}) as complexes:
            yield complexes

    async def test_returns_summary_of_month_ago(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction], complexes: dict[str, Complex]
    ) -> None:
        """호출일 1개월 전 계약된 매매의 최고가·최저가 거래와 매매·전월세 합계 건수를 해제 거래를 빼고 준다."""
        app.dependency_overrides[get_today] = lambda: date(2026, 11, 4)

        response = await client.get(DAILY_SUMMARY_URL)

        assert response.status_code == 200
        assert response.json() == {
            "deal_date": "2026-10-04",
            "highest_sale": {
                "id": seed["highest"].id,
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
            },
            "lowest_sale": {
                "id": seed["lowest"].id,
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
            },
            # 해제되지 않은 매매 4건 + 전월세 2건
            "transaction_count": 6,
        }

    async def test_returns_null_sales_when_no_sale_on_month_ago(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """1개월 전 날짜에 매매가 없으면 최고가·최저가는 null이고 건수는 전월세만 센다."""
        app.dependency_overrides[get_today] = lambda: date(2026, 11, 2)

        response = await client.get(DAILY_SUMMARY_URL)

        assert response.status_code == 200
        assert response.json() == {
            "deal_date": "2026-10-02",
            "highest_sale": None,
            "lowest_sale": None,
            "transaction_count": 2,
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
            "highest_sale": None,
            "lowest_sale": None,
            "transaction_count": 0,
        }

    async def test_clamps_deal_date_to_month_end(self, client: httpx.AsyncClient) -> None:
        """전월에 같은 날이 없으면 전월 말일을 기준일로 삼는다."""
        app.dependency_overrides[get_today] = lambda: date(2026, 10, 31)

        response = await client.get(DAILY_SUMMARY_URL)

        assert response.status_code == 200
        assert response.json() == {
            "deal_date": "2026-09-30",
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
            "s2_prev": APT_ROW | {"apartment_serial_number": "11680-2002", "deal_amount": 200_000_000} | prev,
            "s2_latest": APT_ROW
            | {"apartment_serial_number": "11680-2002", "deal_amount": 280_000_000, "floor": 7}
            | latest,
            "s3_prev": APT_ROW | {"apartment_serial_number": "11680-2003"} | prev,
            "s3_latest": APT_ROW | {"apartment_serial_number": "11680-2003", "deal_amount": 1_300_000_000} | latest,
            "s4_prev": APT_ROW | {"apartment_serial_number": "11680-2004"} | prev,
            "s4_latest": APT_ROW | {"apartment_serial_number": "11680-2004", "deal_amount": 1_200_000_000} | latest,
            "s5_prev": APT_ROW | {"apartment_serial_number": "11680-2005", "deal_amount": 500_000_000} | prev,
            "s5_latest": APT_ROW | {"apartment_serial_number": "11680-2005", "deal_amount": 600_000_000} | latest,
            "s6_prev": APT_ROW | {"apartment_serial_number": "11680-2006"} | prev,
            "s6_latest": APT_ROW | {"apartment_serial_number": "11680-2006", "deal_amount": 1_100_000_000} | latest,
            # 급락: -33.33(반올림), -20(해제 거래 건너뜀), -10, -5(s1과 같은 단지, 다른 면적).
            "p1_prev": APT_ROW | {"apartment_serial_number": "11680-2101", "deal_amount": 300_000_000} | prev,
            "p1_latest": APT_ROW | {"apartment_serial_number": "11680-2101", "deal_amount": 200_000_000} | latest,
            "p2_prev": APT_ROW | {"apartment_serial_number": "11680-2102"} | prev,
            "p2_latest": APT_ROW
            | {"apartment_serial_number": "11680-2102", "deal_amount": 800_000_000, "deal_date": date(2026, 3, 1)},
            "p2_cancelled": APT_ROW
            | {"apartment_serial_number": "11680-2102", "deal_amount": 3_000_000_000, "cancel_deal_type": "O"}
            | latest,
            "p3_prev": APT_ROW | {"apartment_serial_number": "11680-2103", "deal_amount": 400_000_000} | prev,
            "p3_latest": APT_ROW | {"apartment_serial_number": "11680-2103", "deal_amount": 360_000_000} | latest,
            "p4_prev": APT_ROW
            | {"apartment_serial_number": "11680-2001", "exclusive_use_area": 59.9, "deal_amount": 600_000_000}
            | {"deal_date": date(2025, 2, 10)},
            "p4_latest": APT_ROW
            | {"apartment_serial_number": "11680-2001", "exclusive_use_area": 59.9, "deal_amount": 570_000_000}
            | {"deal_date": date(2026, 8, 1)},
            # 순위에서 빠지는 묶음.
            "single": APT_ROW | {"apartment_serial_number": "11680-2301"} | jump_latest,
            "too_old_prev": APT_ROW
            | {"apartment_serial_number": "11680-2302", "deal_amount": 100_000_000, "deal_date": date(2021, 10, 4)},
            "too_old_latest": APT_ROW | {"apartment_serial_number": "11680-2302"} | jump_latest,
            "stale_prev": APT_ROW | {"apartment_serial_number": "11680-2303"} | jump_prev,
            "stale_latest": APT_ROW
            | {"apartment_serial_number": "11680-2303", "deal_amount": 1_000_000_000, "deal_date": date(2025, 10, 4)},
            "unchanged_prev": APT_ROW | {"apartment_serial_number": "11680-2304"} | prev,
            "unchanged_latest": APT_ROW | {"apartment_serial_number": "11680-2304"} | latest,
            "officetel_prev": OFFICETEL_ROW | jump_prev,
            "officetel_latest": OFFICETEL_ROW | jump_latest,
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

    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def complexes(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Complex]]:
        async with seed_complexes(
            session_factory, {"s1": APARTMENT_COMPLEX_ROW | {"apartment_serial_number": "11680-2001"}}
        ) as complexes:
            yield complexes

    async def test_returns_top_surge_and_plunge(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction], complexes: dict[str, Complex]
    ) -> None:
        """아파트 단지를 변동률 순으로 급등·급락 각 5개까지 주고, 동률이면 최근 거래 id 순이다."""
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
                "complex_id": complexes["s1"].id,
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
        excluded += ("officetel_latest", "row_house_latest", "single_multi_latest", "no_serial_latest")
        assert ranked_ids.isdisjoint(seed[name].id for name in excluded)

    async def test_returns_empty_lists_when_no_recent_transaction(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """기준일로부터 1년 안에 거래가 있는 묶음이 없으면 급등·급락 모두 빈 목록이다."""
        app.dependency_overrides[get_today] = lambda: date(2028, 1, 1)

        response = await client.get(PRICE_MOVERS_URL)

        assert response.status_code == 200
        assert response.json() == {"base_date": "2028-01-01", "surge": [], "plunge": []}


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
