"""실거래 목록 조회 API 테스트."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import date
from typing import Any

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.model import Complex, LegalDongCodeRawItem, PropertyType, RentTransaction, SaleTransaction

from .conftest import seed_rows

# 세션 범위 엔진의 커넥션을 같은 이벤트 루프에서 재사용한다.
pytestmark = pytest.mark.asyncio(loop_scope="session")

SALES_URL = "/api/prop-transactions/sales"
RENTS_URL = "/api/prop-transactions/rents"

# 두 스위트가 함께 쓰는 시군구 단위 법정동코드.
LEGAL_DONG_CODE_ROWS = [
    {"payload": {"sido_cd": "11", "sgg_cd": "680", "locatadd_nm": "서울특별시 강남구"}},
    {"payload": {"sido_cd": "11", "sgg_cd": "650", "locatadd_nm": "서울특별시 서초구"}},
    {"payload": {"sido_cd": "26", "sgg_cd": "350", "locatadd_nm": "부산광역시 해운대구"}},
]

# 스위트는 이 행에서 필요한 값만 바꿔 시드를 만든다.
SALE_ROW = {
    "property_type": PropertyType.APT,
    "sido_code": "11",
    "sigungu_code": "680",
    "umd_name": "역삼동",
    "jibun": "123-4",
    "building_name": "역삼래미안",
    "apartment_serial_number": "11680-1001",
    "apartment_dong": "101",
    "deal_date": date(2026, 2, 27),
    "deal_amount": 1_500_000_000,
    "dealing_type": "중개거래",
    "exclusive_use_area": 84.97,
    "floor": 10,
    "build_year": 2005,
}

RENT_ROW = {
    "property_type": PropertyType.APT,
    "sido_code": "11",
    "sigungu_code": "680",
    "umd_name": "역삼동",
    "jibun": "123-4",
    "building_name": "역삼래미안",
    "apartment_serial_number": "11680-1001",
    "deal_date": date(2026, 2, 27),
    "deposit": 800_000_000,
    "monthly_rent": 0,
    "contract_term": "26.03~28.03",
    "contract_type": "신규",
    "exclusive_use_area": 84.97,
    "floor": 10,
    "build_year": 2005,
}

# SALE_ROW·RENT_ROW가 속한 아파트 단지. 스위트는 이 행에서 필요한 값만 바꿔 단지 시드를 만든다.
APARTMENT_COMPLEX_ROW = {
    "property_type": PropertyType.APT,
    "sido_code": "11",
    "sigungu_code": "680",
    "umd_name": "역삼동",
    "jibun": "123-4",
    "building_name": "역삼래미안",
    "build_year": 2005,
    "apartment_serial_number": "11680-1001",
}


@asynccontextmanager
async def seed_complexes(
    session_factory: async_sessionmaker[AsyncSession], rows: dict[str, dict[str, Any]]
) -> AsyncIterator[dict[str, Complex]]:
    """이름 붙인 단지 시드를 만들어 이름으로 돌려준다."""
    async with seed_rows(session_factory, Complex, list(rows.values())) as complexes:
        yield dict(zip(rows, complexes, strict=True))


# 필수 조회 조건. 시드의 target 행과 맞는다.
REQUIRED_PARAMS = {"property_type": "APT", "sido_code": "11", "sigungu_code": "680", "deal_ymd": "202602"}

INVALID_PARAMS = [
    *(
        pytest.param(
            {key: value for key, value in REQUIRED_PARAMS.items() if key != field},
            field,
            "missing",
            id=f"missing_{field}",
        )
        for field in REQUIRED_PARAMS
    ),
    pytest.param(REQUIRED_PARAMS | {"property_type": "VILLA"}, "property_type", "enum", id="property_type"),
    pytest.param(REQUIRED_PARAMS | {"sido_code": "1"}, "sido_code", "string_pattern_mismatch", id="sido_code"),
    pytest.param(
        REQUIRED_PARAMS | {"sigungu_code": "68"}, "sigungu_code", "string_pattern_mismatch", id="sigungu_code"
    ),
    pytest.param(
        REQUIRED_PARAMS | {"deal_ymd": "2026-02"}, "deal_ymd", "string_pattern_mismatch", id="deal_ymd_format"
    ),
    pytest.param(REQUIRED_PARAMS | {"deal_ymd": "202613"}, "deal_ymd", "string_pattern_mismatch", id="deal_ymd_month"),
    pytest.param(REQUIRED_PARAMS | {"limit": 0}, "limit", "greater_than_equal", id="limit_too_small"),
    pytest.param(REQUIRED_PARAMS | {"limit": 1001}, "limit", "less_than_equal", id="limit_too_large"),
    pytest.param(REQUIRED_PARAMS | {"offset": -1}, "offset", "greater_than_equal", id="offset_negative"),
]


class TestGetSalePropTransactions:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, SaleTransaction]]:
        rows = {
            "target": SALE_ROW,
            "same_month": SALE_ROW
            | {"jibun": "123-5", "deal_amount": 1_200_000_000, "floor": 3, "deal_date": date(2026, 2, 1)},
            "previous_month": SALE_ROW | {"deal_date": date(2026, 1, 31)},
            "next_month": SALE_ROW | {"deal_date": date(2026, 3, 1)},
            "cancelled": SALE_ROW | {"jibun": "123-6", "cancel_deal_type": "O", "cancel_deal_date": date(2026, 3, 5)},
            "other_sigungu": SALE_ROW | {"sigungu_code": "650", "umd_name": "서초동", "jibun": "1-1"},
            "other_sido": SALE_ROW | {"sido_code": "26", "sigungu_code": "350", "umd_name": "우동", "jibun": "1-1"},
            "officetel": SALE_ROW | {"property_type": PropertyType.OFFICETEL, "apartment_dong": None},
            "unknown_region": SALE_ROW | {"sigungu_code": "999", "umd_name": "개포동", "jibun": "1-1"},
        }
        async with (
            seed_rows(session_factory, LegalDongCodeRawItem, LEGAL_DONG_CODE_ROWS),
            seed_rows(session_factory, SaleTransaction, list(rows.values())) as sales,
        ):
            yield dict(zip(rows, sales, strict=True))

    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def complexes(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Complex]]:
        async with seed_complexes(session_factory, {"apartment": APARTMENT_COMPLEX_ROW}) as complexes:
            yield complexes

    async def test_returns_transactions_matching_all_conditions(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction], complexes: dict[str, Complex]
    ) -> None:
        """유형·시도·시군구·계약년월이 모두 맞는 매매만 지역명 붙은 주소와 함께 주고, 해제 거래는 해제일도 준다."""
        response = await client.get(SALES_URL, params=REQUIRED_PARAMS)

        assert response.status_code == 200
        assert sorted(response.json(), key=lambda item: item["id"]) == [
            {
                "id": seed["target"].id,
                "property_type": "APT",
                "complex_id": complexes["apartment"].id,
                "deal_date": "2026-02-27",
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
            {
                "id": seed["same_month"].id,
                "property_type": "APT",
                "complex_id": complexes["apartment"].id,
                "deal_date": "2026-02-01",
                "deal_amount": 1_200_000_000,
                "dealing_type": "중개거래",
                "cancel_deal_date": None,
                "house_type": None,
                "building_name": "역삼래미안",
                "apartment_dong": "101",
                "floor": 3,
                "build_year": 2005,
                "exclusive_use_area": 84.97,
                "total_floor_area": None,
                "plottage_area": None,
                "land_area": None,
                "address": "서울특별시 강남구 역삼동 123-5",
            },
            {
                "id": seed["cancelled"].id,
                "property_type": "APT",
                "complex_id": complexes["apartment"].id,
                "deal_date": "2026-02-27",
                "deal_amount": 1_500_000_000,
                "dealing_type": "중개거래",
                "cancel_deal_date": "2026-03-05",
                "house_type": None,
                "building_name": "역삼래미안",
                "apartment_dong": "101",
                "floor": 10,
                "build_year": 2005,
                "exclusive_use_area": 84.97,
                "total_floor_area": None,
                "plottage_area": None,
                "land_area": None,
                "address": "서울특별시 강남구 역삼동 123-6",
            },
        ]

    async def test_paginates_by_limit_and_offset(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """limit·offset만큼 잘라 id 오름차순으로 준다."""
        ids = sorted(seed[name].id for name in ("target", "same_month", "cancelled"))
        params = REQUIRED_PARAMS | {"limit": 1, "offset": 1}

        response = await client.get(SALES_URL, params=params)

        assert response.status_code == 200
        assert [item["id"] for item in response.json()] == ids[1:2]

    async def test_omits_region_name_when_legal_dong_code_missing(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """법정동코드에 없는 시군구면 주소에서 지역명을 빼고 읍면동·지번만 준다."""
        params = REQUIRED_PARAMS | {"sigungu_code": "999"}

        response = await client.get(SALES_URL, params=params)

        assert response.status_code == 200
        assert [(item["id"], item["address"]) for item in response.json()] == [
            (seed["unknown_region"].id, "개포동 1-1")
        ]

    @pytest.mark.parametrize(("params", "field", "error_type"), INVALID_PARAMS)
    async def test_rejects_invalid_query(
        self, client: httpx.AsyncClient, params: dict[str, Any], field: str, error_type: str
    ) -> None:
        """조회 조건의 값 형식이 잘못되면 422와 함께 실패한 필드와 오류 종류를 준다."""
        response = await client.get(SALES_URL, params=params)

        body = response.json()
        assert response.status_code == 422
        assert body["message"] == "요청 값이 올바르지 않습니다"
        assert [(error["loc"], error["type"]) for error in body["errors"]] == [(["query", field], error_type)]
        assert body["trace_id"] == response.headers["X-Trace-ID"]


class TestGetRentPropTransactions:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, RentTransaction]]:
        rows = {
            "target": RENT_ROW,
            "monthly": RENT_ROW | {"deposit": 50_000_000, "monthly_rent": 1_500_000, "deal_date": date(2026, 2, 1)},
            "previous_month": RENT_ROW | {"deal_date": date(2026, 1, 31)},
            "next_month": RENT_ROW | {"deal_date": date(2026, 3, 1)},
            "other_sigungu": RENT_ROW | {"sigungu_code": "650", "umd_name": "서초동", "jibun": "1-1"},
            "other_sido": RENT_ROW | {"sido_code": "26", "sigungu_code": "350", "umd_name": "우동", "jibun": "1-1"},
            "officetel": RENT_ROW | {"property_type": PropertyType.OFFICETEL},
            "unknown_region": RENT_ROW | {"sigungu_code": "999", "umd_name": "개포동", "jibun": "1-1"},
        }
        async with (
            seed_rows(session_factory, LegalDongCodeRawItem, LEGAL_DONG_CODE_ROWS),
            seed_rows(session_factory, RentTransaction, list(rows.values())) as rents,
        ):
            yield dict(zip(rows, rents, strict=True))

    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def complexes(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Complex]]:
        async with seed_complexes(session_factory, {"apartment": APARTMENT_COMPLEX_ROW}) as complexes:
            yield complexes

    async def test_returns_transactions_matching_all_conditions(
        self, client: httpx.AsyncClient, seed: dict[str, RentTransaction], complexes: dict[str, Complex]
    ) -> None:
        """유형·시도·시군구·계약년월이 모두 맞는 전세·월세를 지역명을 붙인 주소와 함께 준다."""
        response = await client.get(RENTS_URL, params=REQUIRED_PARAMS)

        assert response.status_code == 200
        assert sorted(response.json(), key=lambda item: item["id"]) == [
            {
                "id": seed["target"].id,
                "property_type": "APT",
                "complex_id": complexes["apartment"].id,
                "deal_date": "2026-02-27",
                "deposit": 800_000_000,
                "monthly_rent": 0,
                "contract_term": "26.03~28.03",
                "contract_type": "신규",
                "house_type": None,
                "building_name": "역삼래미안",
                "floor": 10,
                "build_year": 2005,
                "exclusive_use_area": 84.97,
                "total_floor_area": None,
                "address": "서울특별시 강남구 역삼동 123-4",
            },
            {
                "id": seed["monthly"].id,
                "property_type": "APT",
                "complex_id": complexes["apartment"].id,
                "deal_date": "2026-02-01",
                "deposit": 50_000_000,
                "monthly_rent": 1_500_000,
                "contract_term": "26.03~28.03",
                "contract_type": "신규",
                "house_type": None,
                "building_name": "역삼래미안",
                "floor": 10,
                "build_year": 2005,
                "exclusive_use_area": 84.97,
                "total_floor_area": None,
                "address": "서울특별시 강남구 역삼동 123-4",
            },
        ]

    async def test_paginates_by_limit_and_offset(
        self, client: httpx.AsyncClient, seed: dict[str, RentTransaction]
    ) -> None:
        """limit·offset만큼 잘라 id 오름차순으로 준다."""
        ids = sorted(seed[name].id for name in ("target", "monthly"))
        params = REQUIRED_PARAMS | {"limit": 1, "offset": 1}

        response = await client.get(RENTS_URL, params=params)

        assert response.status_code == 200
        assert [item["id"] for item in response.json()] == ids[1:2]

    async def test_omits_region_name_when_legal_dong_code_missing(
        self, client: httpx.AsyncClient, seed: dict[str, RentTransaction]
    ) -> None:
        """법정동코드에 없는 시군구면 주소에서 지역명을 빼고 읍면동·지번만 준다."""
        params = REQUIRED_PARAMS | {"sigungu_code": "999"}

        response = await client.get(RENTS_URL, params=params)

        assert response.status_code == 200
        assert [(item["id"], item["address"]) for item in response.json()] == [
            (seed["unknown_region"].id, "개포동 1-1")
        ]

    @pytest.mark.parametrize(("params", "field", "error_type"), INVALID_PARAMS)
    async def test_rejects_invalid_query(
        self, client: httpx.AsyncClient, params: dict[str, Any], field: str, error_type: str
    ) -> None:
        """조회 조건의 값 형식이 잘못되면 422와 함께 실패한 필드와 오류 종류를 준다."""
        response = await client.get(RENTS_URL, params=params)

        body = response.json()
        assert response.status_code == 422
        assert body["message"] == "요청 값이 올바르지 않습니다"
        assert [(error["loc"], error["type"]) for error in body["errors"]] == [(["query", field], error_type)]
        assert body["trace_id"] == response.headers["X-Trace-ID"]


# 상세 조회 스위트가 함께 쓰는 연립다세대·단독다가구 시드. 매매·전월세 공통 컬럼만 바꾼다.
ROW_HOUSE_OVERRIDES = {
    "property_type": PropertyType.ROW_HOUSE,
    "house_type": "다세대",
    "jibun": "55-1",
    "building_name": "(55-1)",
    "apartment_serial_number": None,
    "exclusive_use_area": 40.12,
    "build_year": 2015,
}
OFFICETEL_OVERRIDES = {
    "property_type": PropertyType.OFFICETEL,
    "jibun": "200",
    "building_name": "역삼오피스텔",
    "apartment_serial_number": None,
    "exclusive_use_area": 30.12,
    "build_year": 2018,
}
# 상세 조회 스위트의 단지 시드. 건물명이 없는 오피스텔로 NULL이 섞인 단지 키도 이어지는지 본다.
OFFICETEL_COMPLEX_ROW = APARTMENT_COMPLEX_ROW | {
    key: value for key, value in OFFICETEL_OVERRIDES.items() if key != "exclusive_use_area"
}
DETAIL_COMPLEX_ROWS = {
    "apartment": APARTMENT_COMPLEX_ROW,
    "officetel": OFFICETEL_COMPLEX_ROW,
    "officetel_no_name": OFFICETEL_COMPLEX_ROW | {"building_name": None},
}

# 단지가 있는 거래와 없는 거래. 기대값이 None이면 complex_id가 null이다.
COMPLEX_LINKS = [
    pytest.param("officetel", "officetel", id="officetel"),
    pytest.param("officetel_no_name", "officetel_no_name", id="officetel_null_key"),
    pytest.param("officetel_rebuilt", None, id="officetel_without_complex"),
    pytest.param("missing_serial_number", None, id="apartment_without_serial_number"),
    pytest.param("row_house", None, id="row_house"),
    pytest.param("single_multi", None, id="single_multi"),
]

SINGLE_MULTI_OVERRIDES = {
    "property_type": PropertyType.SINGLE_MULTI,
    "house_type": "다가구",
    "jibun": None,
    "building_name": None,
    "apartment_serial_number": None,
    "exclusive_use_area": None,
    "floor": None,
    "total_floor_area": 36.0,
}

INVALID_TRANSACTION_IDS = [
    pytest.param(0, "greater_than_equal", id="zero"),
    pytest.param("abc", "int_parsing", id="not_int"),
]


class TestGetSalePropTransactionDetail:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, SaleTransaction]]:
        row_house = SALE_ROW | ROW_HOUSE_OVERRIDES | {"apartment_dong": None}
        officetel = SALE_ROW | OFFICETEL_OVERRIDES | {"apartment_dong": None}
        rows = {
            "target": SALE_ROW,
            "earlier": SALE_ROW | {"deal_date": date(2025, 3, 10), "deal_amount": 1_300_000_000, "floor": 5},
            "cancelled": SALE_ROW | {"deal_date": date(2026, 1, 10), "cancel_deal_type": "O"},
            "other_area": SALE_ROW | {"exclusive_use_area": 59.99},
            "other_building": SALE_ROW | {"apartment_serial_number": "11680-2002"},
            "renamed": SALE_ROW | {"building_name": "역삼래미안포레", "deal_date": date(2025, 9, 1), "floor": 7},
            "missing_serial_number": SALE_ROW | {"apartment_serial_number": None, "deal_date": date(2025, 6, 1)},
            "row_house": row_house,
            "row_house_renamed": row_house
            | {"building_name": "역삼빌라", "exclusive_use_area": 59.5, "deal_date": date(2024, 5, 1)},
            "row_house_rebuilt": row_house | {"build_year": 1990, "deal_date": date(2010, 5, 1)},
            "row_house_missing_build_year": row_house | {"build_year": None, "deal_date": date(2023, 5, 1)},
            "row_house_missing_jibun": row_house | {"jibun": None, "deal_date": date(2022, 5, 1)},
            "officetel": officetel,
            "officetel_earlier": officetel | {"deal_date": date(2025, 4, 1), "deal_amount": 300_000_000, "floor": 3},
            "officetel_rebuilt": officetel | {"build_year": 1995, "deal_date": date(2012, 5, 1)},
            "officetel_other_area": officetel | {"exclusive_use_area": 30.05},
            "officetel_no_name": officetel | {"building_name": None},
            "single_multi": SALE_ROW
            | SINGLE_MULTI_OVERRIDES
            | {"jibun": "1**", "apartment_dong": None, "build_year": 1990, "plottage_area": 150.0},
        }
        async with (
            seed_rows(session_factory, LegalDongCodeRawItem, LEGAL_DONG_CODE_ROWS),
            seed_rows(session_factory, SaleTransaction, list(rows.values())) as sales,
        ):
            yield dict(zip(rows, sales, strict=True))

    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def complexes(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Complex]]:
        async with seed_complexes(session_factory, DETAIL_COMPLEX_ROWS) as complexes:
            yield complexes

    @pytest.mark.parametrize(("key", "complex_key"), COMPLEX_LINKS)
    async def test_links_complex_by_complex_key(
        self,
        client: httpx.AsyncClient,
        seed: dict[str, SaleTransaction],
        complexes: dict[str, Complex],
        key: str,
        complex_key: str | None,
    ) -> None:
        """아파트는 단지 일련번호, 오피스텔은 NULL을 포함한 단지 키로 단지를 찾고, 단지가 없으면 null을 준다."""
        response = await client.get(f"{SALES_URL}/{seed[key].id}")

        assert response.status_code == 200
        expected = None if complex_key is None else complexes[complex_key].id
        assert response.json()["base_transaction"]["complex_id"] == expected

    async def test_returns_apartment_trend_of_same_serial_number_and_area(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction], complexes: dict[str, Complex]
    ) -> None:
        """아파트는 단지명이 바뀌어도 같은 단지 일련번호·전용면적의 해제되지 않은 거래를 계약일 순으로 담는다."""
        response = await client.get(f"{SALES_URL}/{seed['target'].id}")

        assert response.status_code == 200
        assert response.json() == {
            "base_transaction": {
                "id": seed["target"].id,
                "property_type": "APT",
                "complex_id": complexes["apartment"].id,
                "deal_date": "2026-02-27",
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
            "trend": [
                {"id": seed["earlier"].id, "deal_date": "2025-03-10", "deal_amount": 1_300_000_000, "floor": 5},
                {"id": seed["renamed"].id, "deal_date": "2025-09-01", "deal_amount": 1_500_000_000, "floor": 7},
                {"id": seed["target"].id, "deal_date": "2026-02-27", "deal_amount": 1_500_000_000, "floor": 10},
            ],
        }

    async def test_returns_empty_trend_when_apartment_serial_number_missing(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """단지 일련번호가 없는 아파트는 같은 단지를 찾을 수 없어 추이를 빈 목록으로 준다."""
        response = await client.get(f"{SALES_URL}/{seed['missing_serial_number'].id}")

        body = response.json()
        assert response.status_code == 200
        assert body["base_transaction"]["id"] == seed["missing_serial_number"].id
        assert body["trend"] == []

    async def test_groups_officetel_by_build_year_and_exact_area(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """오피스텔은 같은 지번·건물명·건축년도이면서 전용면적이 정확히 같은 거래만 묶는다."""
        response = await client.get(f"{SALES_URL}/{seed['officetel'].id}")

        assert response.status_code == 200
        assert [point["id"] for point in response.json()["trend"]] == [
            seed["officetel_earlier"].id,
            seed["officetel"].id,
        ]

    async def test_groups_row_house_by_build_year_instead_of_name(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """연립다세대는 이름·면적이 달라도 같은 지번·건축년도면 같은 건물로 묶는다."""
        response = await client.get(f"{SALES_URL}/{seed['row_house'].id}")

        assert response.status_code == 200
        assert [point["id"] for point in response.json()["trend"]] == [
            seed["row_house_renamed"].id,
            seed["row_house"].id,
        ]

    @pytest.mark.parametrize("key", ["row_house_missing_build_year", "row_house_missing_jibun"])
    async def test_returns_empty_trend_when_row_house_key_missing(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction], key: str
    ) -> None:
        """연립다세대는 지번이나 건축년도가 없으면 같은 건물을 특정할 수 없어 추이를 빈 목록으로 준다."""
        response = await client.get(f"{SALES_URL}/{seed[key].id}")

        body = response.json()
        assert response.status_code == 200
        assert body["base_transaction"]["id"] == seed[key].id
        assert body["trend"] == []

    async def test_returns_null_trend_for_single_multi(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """단독다가구는 같은 매물을 특정할 수 없어 추이를 null로 준다."""
        response = await client.get(f"{SALES_URL}/{seed['single_multi'].id}")

        body = response.json()
        assert response.status_code == 200
        assert body["base_transaction"]["id"] == seed["single_multi"].id
        assert body["trend"] is None

    async def test_returns_404_when_transaction_missing(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """없는 id면 404 공통 오류 응답을 준다."""
        missing_id = max(transaction.id for transaction in seed.values()) + 1

        response = await client.get(f"{SALES_URL}/{missing_id}")

        assert response.status_code == 404
        assert response.json() == {
            "message": "실거래를 찾을 수 없습니다",
            "errors": [],
            "trace_id": response.headers["X-Trace-ID"],
        }

    @pytest.mark.parametrize(("transaction_id", "error_type"), INVALID_TRANSACTION_IDS)
    async def test_rejects_invalid_transaction_id(
        self, client: httpx.AsyncClient, transaction_id: int | str, error_type: str
    ) -> None:
        """id가 1 이상의 정수가 아니면 422와 함께 실패한 경로와 오류 종류를 준다."""
        response = await client.get(f"{SALES_URL}/{transaction_id}")

        body = response.json()
        assert response.status_code == 422
        assert [(error["loc"], error["type"]) for error in body["errors"]] == [(["path", "transaction_id"], error_type)]


class TestGetRentPropTransactionDetail:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, RentTransaction]]:
        row_house = RENT_ROW | ROW_HOUSE_OVERRIDES
        officetel = RENT_ROW | OFFICETEL_OVERRIDES
        rows = {
            "target": RENT_ROW,
            "earlier_jeonse": RENT_ROW | {"deal_date": date(2024, 3, 1), "deposit": 700_000_000, "floor": 5},
            "monthly": RENT_ROW | {"deal_date": date(2025, 6, 1), "deposit": 50_000_000, "monthly_rent": 1_500_000},
            "other_area": RENT_ROW | {"exclusive_use_area": 59.99},
            "other_building": RENT_ROW | {"apartment_serial_number": "11680-2002"},
            "renamed": RENT_ROW | {"building_name": "역삼래미안포레", "deal_date": date(2025, 9, 1), "floor": 7},
            "missing_serial_number": RENT_ROW | {"apartment_serial_number": None, "deal_date": date(2025, 6, 1)},
            "row_house": row_house,
            "row_house_renamed": row_house | {"building_name": "역삼빌라", "deal_date": date(2024, 5, 1)},
            "row_house_rebuilt": row_house | {"build_year": 1990, "deal_date": date(2012, 5, 1)},
            "row_house_missing_build_year": row_house | {"build_year": None, "deal_date": date(2023, 5, 1)},
            "row_house_missing_jibun": row_house | {"jibun": None, "deal_date": date(2022, 5, 1)},
            "officetel": officetel,
            "officetel_earlier": officetel | {"deal_date": date(2025, 4, 1), "deposit": 250_000_000, "floor": 3},
            "officetel_rebuilt": officetel | {"build_year": 1995, "deal_date": date(2012, 5, 1)},
            "officetel_other_area": officetel | {"exclusive_use_area": 30.05},
            "officetel_no_name": officetel | {"building_name": None},
            "single_multi": RENT_ROW | SINGLE_MULTI_OVERRIDES | {"build_year": 1991},
        }
        async with (
            seed_rows(session_factory, LegalDongCodeRawItem, LEGAL_DONG_CODE_ROWS),
            seed_rows(session_factory, RentTransaction, list(rows.values())) as rents,
        ):
            yield dict(zip(rows, rents, strict=True))

    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def complexes(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Complex]]:
        async with seed_complexes(session_factory, DETAIL_COMPLEX_ROWS) as complexes:
            yield complexes

    @pytest.mark.parametrize(("key", "complex_key"), COMPLEX_LINKS)
    async def test_links_complex_by_complex_key(
        self,
        client: httpx.AsyncClient,
        seed: dict[str, RentTransaction],
        complexes: dict[str, Complex],
        key: str,
        complex_key: str | None,
    ) -> None:
        """아파트는 단지 일련번호, 오피스텔은 NULL을 포함한 단지 키로 단지를 찾고, 단지가 없으면 null을 준다."""
        response = await client.get(f"{RENTS_URL}/{seed[key].id}")

        assert response.status_code == 200
        expected = None if complex_key is None else complexes[complex_key].id
        assert response.json()["base_transaction"]["complex_id"] == expected

    async def test_returns_apartment_trend_split_into_jeonse_and_monthly_rent(
        self, client: httpx.AsyncClient, seed: dict[str, RentTransaction], complexes: dict[str, Complex]
    ) -> None:
        """아파트는 단지명이 바뀌어도 같은 단지 일련번호·전용면적의 거래를 전세와 월세로 나눠 계약일 순으로 담는다."""
        response = await client.get(f"{RENTS_URL}/{seed['target'].id}")

        assert response.status_code == 200
        assert response.json() == {
            "base_transaction": {
                "id": seed["target"].id,
                "property_type": "APT",
                "complex_id": complexes["apartment"].id,
                "deal_date": "2026-02-27",
                "deposit": 800_000_000,
                "monthly_rent": 0,
                "contract_term": "26.03~28.03",
                "contract_type": "신규",
                "house_type": None,
                "building_name": "역삼래미안",
                "floor": 10,
                "build_year": 2005,
                "exclusive_use_area": 84.97,
                "total_floor_area": None,
                "address": "서울특별시 강남구 역삼동 123-4",
            },
            "jeonse_trend": [
                {
                    "id": seed["earlier_jeonse"].id,
                    "deal_date": "2024-03-01",
                    "deposit": 700_000_000,
                    "monthly_rent": 0,
                    "floor": 5,
                },
                {
                    "id": seed["renamed"].id,
                    "deal_date": "2025-09-01",
                    "deposit": 800_000_000,
                    "monthly_rent": 0,
                    "floor": 7,
                },
                {
                    "id": seed["target"].id,
                    "deal_date": "2026-02-27",
                    "deposit": 800_000_000,
                    "monthly_rent": 0,
                    "floor": 10,
                },
            ],
            "monthly_rent_trend": [
                {
                    "id": seed["monthly"].id,
                    "deal_date": "2025-06-01",
                    "deposit": 50_000_000,
                    "monthly_rent": 1_500_000,
                    "floor": 10,
                },
            ],
        }

    async def test_groups_officetel_by_build_year_and_exact_area(
        self, client: httpx.AsyncClient, seed: dict[str, RentTransaction]
    ) -> None:
        """오피스텔은 같은 지번·건물명·건축년도이면서 전용면적이 정확히 같은 거래만 묶는다."""
        response = await client.get(f"{RENTS_URL}/{seed['officetel'].id}")

        body = response.json()
        assert response.status_code == 200
        assert [point["id"] for point in body["jeonse_trend"]] == [seed["officetel_earlier"].id, seed["officetel"].id]
        assert body["monthly_rent_trend"] == []

    async def test_groups_row_house_by_build_year_instead_of_name(
        self, client: httpx.AsyncClient, seed: dict[str, RentTransaction]
    ) -> None:
        """연립다세대는 이름이 달라도 같은 지번·건축년도면 같은 건물로 묶는다."""
        response = await client.get(f"{RENTS_URL}/{seed['row_house'].id}")

        body = response.json()
        assert response.status_code == 200
        assert [point["id"] for point in body["jeonse_trend"]] == [seed["row_house_renamed"].id, seed["row_house"].id]
        assert body["monthly_rent_trend"] == []

    async def test_returns_empty_trends_when_apartment_serial_number_missing(
        self, client: httpx.AsyncClient, seed: dict[str, RentTransaction]
    ) -> None:
        """단지 일련번호가 없는 아파트는 같은 단지를 찾을 수 없어 전세·월세 추이를 모두 빈 목록으로 준다."""
        response = await client.get(f"{RENTS_URL}/{seed['missing_serial_number'].id}")

        body = response.json()
        assert response.status_code == 200
        assert body["base_transaction"]["id"] == seed["missing_serial_number"].id
        assert body["jeonse_trend"] == []
        assert body["monthly_rent_trend"] == []

    @pytest.mark.parametrize("key", ["row_house_missing_build_year", "row_house_missing_jibun"])
    async def test_returns_empty_trend_when_row_house_key_missing(
        self, client: httpx.AsyncClient, seed: dict[str, RentTransaction], key: str
    ) -> None:
        """연립다세대는 지번이나 건축년도가 없으면 같은 건물을 특정할 수 없어 전세·월세 추이를 모두 빈 목록으로 준다."""
        response = await client.get(f"{RENTS_URL}/{seed[key].id}")

        body = response.json()
        assert response.status_code == 200
        assert body["base_transaction"]["id"] == seed[key].id
        assert body["jeonse_trend"] == []
        assert body["monthly_rent_trend"] == []

    async def test_returns_null_trend_for_single_multi(
        self, client: httpx.AsyncClient, seed: dict[str, RentTransaction]
    ) -> None:
        """단독다가구는 같은 매물을 특정할 수 없어 전세·월세 추이를 모두 null로 준다."""
        response = await client.get(f"{RENTS_URL}/{seed['single_multi'].id}")

        body = response.json()
        assert response.status_code == 200
        assert body["base_transaction"]["id"] == seed["single_multi"].id
        assert body["jeonse_trend"] is None
        assert body["monthly_rent_trend"] is None

    async def test_returns_404_when_transaction_missing(
        self, client: httpx.AsyncClient, seed: dict[str, RentTransaction]
    ) -> None:
        """없는 id면 404 공통 오류 응답을 준다."""
        missing_id = max(transaction.id for transaction in seed.values()) + 1

        response = await client.get(f"{RENTS_URL}/{missing_id}")

        assert response.status_code == 404
        assert response.json() == {
            "message": "실거래를 찾을 수 없습니다",
            "errors": [],
            "trace_id": response.headers["X-Trace-ID"],
        }

    @pytest.mark.parametrize(("transaction_id", "error_type"), INVALID_TRANSACTION_IDS)
    async def test_rejects_invalid_transaction_id(
        self, client: httpx.AsyncClient, transaction_id: int | str, error_type: str
    ) -> None:
        """id가 1 이상의 정수가 아니면 422와 함께 실패한 경로와 오류 종류를 준다."""
        response = await client.get(f"{RENTS_URL}/{transaction_id}")

        body = response.json()
        assert response.status_code == 422
        assert [(error["loc"], error["type"]) for error in body["errors"]] == [(["path", "transaction_id"], error_type)]
