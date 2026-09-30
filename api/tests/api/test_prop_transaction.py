"""실거래 목록 조회 API 테스트."""

from collections.abc import AsyncIterator
from datetime import date
from typing import Any

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.model import LegalDongCodeRawItem, PropertyType, RentTransaction, SaleTransaction

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
    "deal_date": date(2026, 2, 27),
    "deposit": 800_000_000,
    "monthly_rent": 0,
    "contract_term": "26.03~28.03",
    "contract_type": "신규",
    "exclusive_use_area": 84.97,
    "floor": 10,
    "build_year": 2005,
}

INVALID_PARAMS = [
    pytest.param({}, "property_type", "missing", id="missing_property_type"),
    pytest.param({"property_type": "VILLA"}, "property_type", "enum", id="property_type"),
    pytest.param({"property_type": "APT", "sido_code": "1"}, "sido_code", "string_pattern_mismatch", id="sido_code"),
    pytest.param(
        {"property_type": "APT", "sido_code": "11", "sigungu_code": "68"},
        "sigungu_code",
        "string_pattern_mismatch",
        id="sigungu_code",
    ),
    pytest.param(
        {"property_type": "APT", "sido_code": "11", "sigungu_code": "680", "deal_date": "2026-13-01"},
        "deal_date",
        "date_from_datetime_parsing",
        id="deal_date",
    ),
    pytest.param({"property_type": "APT", "limit": 0}, "limit", "greater_than_equal", id="limit_too_small"),
    pytest.param({"property_type": "APT", "limit": 1001}, "limit", "less_than_equal", id="limit_too_large"),
    pytest.param({"property_type": "APT", "offset": -1}, "offset", "greater_than_equal", id="offset_negative"),
]

DEAL_DATE_WITHOUT_REGION_PARAMS = [
    pytest.param({"property_type": "APT", "deal_date": "2026-02-27"}, id="no_region"),
    pytest.param({"property_type": "APT", "sido_code": "11", "deal_date": "2026-02-27"}, id="sido_only"),
]


class TestGetSalePropTransactions:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, SaleTransaction]]:
        rows = {
            "target": SALE_ROW,
            "other_date": SALE_ROW | {"deal_date": date(2026, 2, 28)},
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

    async def test_returns_transactions_matching_all_conditions(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """유형·시도·시군구·계약일이 모두 맞는 매매만 지역명을 붙인 주소와 함께 준다."""
        params = {"property_type": "APT", "sido_code": "11", "sigungu_code": "680", "deal_date": "2026-02-27"}

        response = await client.get(SALES_URL, params=params)

        assert response.status_code == 200
        assert response.json() == [
            {
                "id": seed["target"].id,
                "property_type": "APT",
                "deal_date": "2026-02-27",
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
            }
        ]

    async def test_returns_all_sigungu_when_only_sido_given(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """시도만 주면 시군구·계약일과 상관없이 그 시도의 같은 유형 매매를 모두 준다."""
        response = await client.get(SALES_URL, params={"property_type": "APT", "sido_code": "11"})

        assert response.status_code == 200
        assert {item["id"] for item in response.json()} == {
            seed[name].id for name in ("target", "other_date", "other_sigungu", "unknown_region")
        }

    async def test_paginates_by_limit_and_offset(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """limit·offset만큼 잘라 id 오름차순으로 준다."""
        ids = sorted(seed[name].id for name in ("target", "other_date", "other_sigungu", "unknown_region"))
        params = {"property_type": "APT", "sido_code": "11", "limit": 2, "offset": 1}

        response = await client.get(SALES_URL, params=params)

        assert response.status_code == 200
        assert [item["id"] for item in response.json()] == ids[1:3]

    async def test_omits_region_name_when_legal_dong_code_missing(
        self, client: httpx.AsyncClient, seed: dict[str, SaleTransaction]
    ) -> None:
        """법정동코드에 없는 시군구면 주소에서 지역명을 빼고 읍면동·지번만 준다."""
        params = {"property_type": "APT", "sido_code": "11", "sigungu_code": "999"}

        response = await client.get(SALES_URL, params=params)

        assert response.status_code == 200
        assert [(item["id"], item["address"]) for item in response.json()] == [
            (seed["unknown_region"].id, "개포동 1-1")
        ]

    async def test_rejects_missing_api_key(self, client: httpx.AsyncClient) -> None:
        """X-API-KEY 헤더가 없으면 401 공통 오류 응답을 준다."""
        request = client.build_request("GET", SALES_URL, params={"property_type": "APT"})
        del request.headers["X-API-KEY"]

        response = await client.send(request)

        assert response.status_code == 401
        assert response.json() == {
            "message": "API 키가 올바르지 않습니다",
            "errors": [],
            "trace_id": response.headers["X-Trace-ID"],
        }

    async def test_rejects_sigungu_without_sido(self, client: httpx.AsyncClient) -> None:
        """sido_code 없이 sigungu_code만 오면 400을 준다."""
        response = await client.get(SALES_URL, params={"property_type": "APT", "sigungu_code": "680"})

        assert response.status_code == 400
        assert response.json() == {
            "message": "sigungu_code는 sido_code와 함께 지정해야 한다",
            "errors": [],
            "trace_id": response.headers["X-Trace-ID"],
        }

    @pytest.mark.parametrize("params", DEAL_DATE_WITHOUT_REGION_PARAMS)
    async def test_rejects_deal_date_without_sido_and_sigungu(
        self, client: httpx.AsyncClient, params: dict[str, str]
    ) -> None:
        """deal_date가 sido_code나 sigungu_code 없이 오면 400을 준다."""
        response = await client.get(SALES_URL, params=params)

        assert response.status_code == 400
        assert response.json() == {
            "message": "deal_date는 sido_code, sigungu_code와 함께 지정해야 한다",
            "errors": [],
            "trace_id": response.headers["X-Trace-ID"],
        }

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
            "monthly": RENT_ROW | {"deposit": 50_000_000, "monthly_rent": 1_500_000},
            "other_date": RENT_ROW | {"deal_date": date(2026, 2, 28)},
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

    async def test_returns_transactions_matching_all_conditions(
        self, client: httpx.AsyncClient, seed: dict[str, RentTransaction]
    ) -> None:
        """유형·시도·시군구·계약일이 모두 맞는 전세·월세를 지역명을 붙인 주소와 함께 준다."""
        params = {"property_type": "APT", "sido_code": "11", "sigungu_code": "680", "deal_date": "2026-02-27"}

        response = await client.get(RENTS_URL, params=params)

        assert response.status_code == 200
        assert sorted(response.json(), key=lambda item: item["id"]) == [
            {
                "id": seed["target"].id,
                "property_type": "APT",
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
                "deal_date": "2026-02-27",
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

    async def test_returns_all_sigungu_when_only_sido_given(
        self, client: httpx.AsyncClient, seed: dict[str, RentTransaction]
    ) -> None:
        """시도만 주면 시군구·계약일과 상관없이 그 시도의 같은 유형 전월세를 모두 준다."""
        response = await client.get(RENTS_URL, params={"property_type": "APT", "sido_code": "11"})

        assert response.status_code == 200
        assert {item["id"] for item in response.json()} == {
            seed[name].id for name in ("target", "monthly", "other_date", "other_sigungu", "unknown_region")
        }

    async def test_paginates_by_limit_and_offset(
        self, client: httpx.AsyncClient, seed: dict[str, RentTransaction]
    ) -> None:
        """limit·offset만큼 잘라 id 오름차순으로 준다."""
        ids = sorted(seed[name].id for name in ("target", "monthly", "other_date", "other_sigungu", "unknown_region"))
        params = {"property_type": "APT", "sido_code": "11", "limit": 2, "offset": 1}

        response = await client.get(RENTS_URL, params=params)

        assert response.status_code == 200
        assert [item["id"] for item in response.json()] == ids[1:3]

    async def test_omits_region_name_when_legal_dong_code_missing(
        self, client: httpx.AsyncClient, seed: dict[str, RentTransaction]
    ) -> None:
        """법정동코드에 없는 시군구면 주소에서 지역명을 빼고 읍면동·지번만 준다."""
        params = {"property_type": "APT", "sido_code": "11", "sigungu_code": "999"}

        response = await client.get(RENTS_URL, params=params)

        assert response.status_code == 200
        assert [(item["id"], item["address"]) for item in response.json()] == [
            (seed["unknown_region"].id, "개포동 1-1")
        ]

    async def test_rejects_missing_api_key(self, client: httpx.AsyncClient) -> None:
        """X-API-KEY 헤더가 없으면 401 공통 오류 응답을 준다."""
        request = client.build_request("GET", RENTS_URL, params={"property_type": "APT"})
        del request.headers["X-API-KEY"]

        response = await client.send(request)

        assert response.status_code == 401
        assert response.json() == {
            "message": "API 키가 올바르지 않습니다",
            "errors": [],
            "trace_id": response.headers["X-Trace-ID"],
        }

    async def test_rejects_sigungu_without_sido(self, client: httpx.AsyncClient) -> None:
        """sido_code 없이 sigungu_code만 오면 400을 준다."""
        response = await client.get(RENTS_URL, params={"property_type": "APT", "sigungu_code": "680"})

        assert response.status_code == 400
        assert response.json() == {
            "message": "sigungu_code는 sido_code와 함께 지정해야 한다",
            "errors": [],
            "trace_id": response.headers["X-Trace-ID"],
        }

    @pytest.mark.parametrize("params", DEAL_DATE_WITHOUT_REGION_PARAMS)
    async def test_rejects_deal_date_without_sido_and_sigungu(
        self, client: httpx.AsyncClient, params: dict[str, str]
    ) -> None:
        """deal_date가 sido_code나 sigungu_code 없이 오면 400을 준다."""
        response = await client.get(RENTS_URL, params=params)

        assert response.status_code == 400
        assert response.json() == {
            "message": "deal_date는 sido_code, sigungu_code와 함께 지정해야 한다",
            "errors": [],
            "trace_id": response.headers["X-Trace-ID"],
        }

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
