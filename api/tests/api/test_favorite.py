"""단지 즐겨찾기 API 테스트."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.model import Complex, FavoriteComplex, LegalDongCodeRawItem, PropertyType, User, UserSession
from src.services.user import SESSION_TTL

from .conftest import seed_rows
from .test_user import USER_ROW, session_cookie_header, session_row

# 세션 범위 엔진의 커넥션을 같은 이벤트 루프에서 재사용한다.
pytestmark = pytest.mark.asyncio(loop_scope="session")

FAVORITE_COMPLEXES_URL = "/api/favorites/complexes"

TOKEN = "favorite-valid"  # noqa: S105 - 테스트 시드 세션의 토큰

APARTMENT_ROW = {
    "property_type": PropertyType.APT,
    "sido_code": "11",
    "sigungu_code": "110",
    "umd_name": "청운동",
    "jibun": "1",
    "building_name": "청운현대",
    "build_year": 2000,
    "apartment_serial_number": "11110-1",
}
OFFICETEL_ROW = APARTMENT_ROW | {
    "property_type": PropertyType.OFFICETEL,
    "building_name": "청운오피스텔",
    "apartment_serial_number": None,
}

UNAUTHENTICATED_HEADERS = [
    pytest.param({}, id="no_cookie"),
    pytest.param(session_cookie_header("unknown"), id="unknown_token"),
]


def assert_unauthenticated(response: httpx.Response) -> None:
    """401 공통 오류 응답인지 확인한다."""
    assert response.status_code == 401
    assert response.json() == {
        "message": "로그인이 필요합니다",
        "errors": [],
        "trace_id": response.headers["X-Trace-ID"],
    }


@asynccontextmanager
async def seed_users_and_complexes(session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Any]]:
    """로그인한 대상 사용자, 다른 사용자, 아파트·오피스텔 단지를 넣고 이름으로 돌려준다."""
    # 즐겨찾기는 사용자나 단지가 지워질 때 FK의 ON DELETE CASCADE로 함께 지워진다.
    user_rows = {
        "target": USER_ROW | {"email": "favorite@example.com"},
        "other": USER_ROW | {"email": "favorite-other@example.com"},
    }
    complex_rows = {"apartment": APARTMENT_ROW, "officetel": OFFICETEL_ROW}
    async with (
        seed_rows(session_factory, User, list(user_rows.values())) as users,
        seed_rows(session_factory, UserSession, [session_row(users[0], TOKEN, SESSION_TTL)]),
        seed_rows(session_factory, Complex, list(complex_rows.values())) as complexes,
    ):
        yield dict(zip(user_rows, users, strict=True)) | dict(zip(complex_rows, complexes, strict=True))


def mixed_favorite_rows(seed: dict[str, Any]) -> list[dict[str, Any]]:
    """대상 사용자의 아파트·오피스텔 즐겨찾기와 다른 사용자의 아파트 즐겨찾기."""
    user, other, apartment, officetel = seed["target"], seed["other"], seed["apartment"], seed["officetel"]
    return [
        {"user_id": user.id, "complex_id": apartment.id},
        {"user_id": user.id, "complex_id": officetel.id},
        {"user_id": other.id, "complex_id": apartment.id},
    ]


async def favorite_complex_ids(session_factory: async_sessionmaker[AsyncSession], user: User) -> list[int]:
    """사용자가 즐겨찾기한 단지 id 목록."""
    async with session_factory() as session, session.begin():
        result = await session.scalars(
            select(FavoriteComplex.complex_id)
            .where(FavoriteComplex.user_id == user.id)
            .order_by(FavoriteComplex.complex_id)
        )
        return list(result)


class TestAddFavoriteComplex:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Any]]:
        async with seed_users_and_complexes(session_factory) as seeded:
            yield seeded

    async def test_adds_favorite(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        seed: dict[str, Any],
    ) -> None:
        """즐겨찾기하지 않은 단지면 추가하고 201을 준다."""
        user, apartment = seed["target"], seed["apartment"]
        try:
            response = await client.post(
                FAVORITE_COMPLEXES_URL, json={"complex_id": apartment.id}, headers=session_cookie_header(TOKEN)
            )
            favorites = await favorite_complex_ids(session_factory, user)
        finally:
            async with session_factory() as session, session.begin():
                await session.execute(delete(FavoriteComplex).where(FavoriteComplex.user_id == user.id))

        assert response.status_code == 201
        assert response.json() == {"complex_id": apartment.id}
        assert favorites == [apartment.id]

    async def test_keeps_existing_favorite(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        seed: dict[str, Any],
    ) -> None:
        """이미 즐겨찾기한 단지여도 제거하지 않고 201을 준다. 다른 즐겨찾기도 그대로 둔다."""
        user, other, apartment, officetel = seed["target"], seed["other"], seed["apartment"], seed["officetel"]
        async with seed_rows(session_factory, FavoriteComplex, mixed_favorite_rows(seed)):
            response = await client.post(
                FAVORITE_COMPLEXES_URL, json={"complex_id": apartment.id}, headers=session_cookie_header(TOKEN)
            )
            favorites = await favorite_complex_ids(session_factory, user)
            other_favorites = await favorite_complex_ids(session_factory, other)

        assert response.status_code == 201
        assert response.json() == {"complex_id": apartment.id}
        assert favorites == sorted([apartment.id, officetel.id])
        assert other_favorites == [apartment.id]

    @pytest.mark.parametrize("headers", UNAUTHENTICATED_HEADERS)
    async def test_rejects_unauthenticated(
        self, client: httpx.AsyncClient, seed: dict[str, Any], headers: dict[str, str]
    ) -> None:
        """쿠키가 없거나 세션이 무효하면 401 공통 오류 응답을 준다."""
        response = await client.post(FAVORITE_COMPLEXES_URL, json={"complex_id": seed["apartment"].id}, headers=headers)

        assert_unauthenticated(response)

    async def test_rejects_missing_complex(self, client: httpx.AsyncClient, seed: dict[str, Any]) -> None:
        """없는 단지 id면 404 공통 오류 응답을 준다."""
        missing_id = max(seed["apartment"].id, seed["officetel"].id) + 1

        response = await client.post(
            FAVORITE_COMPLEXES_URL, json={"complex_id": missing_id}, headers=session_cookie_header(TOKEN)
        )

        assert response.status_code == 404
        assert response.json() == {
            "message": "단지를 찾을 수 없습니다",
            "errors": [],
            "trace_id": response.headers["X-Trace-ID"],
        }

    @pytest.mark.parametrize(
        ("body", "error_type"),
        [
            pytest.param({"complex_id": 0}, "greater_than_equal", id="zero"),
            pytest.param({"complex_id": "abc"}, "int_parsing", id="not_int"),
            pytest.param({}, "missing", id="missing"),
        ],
    )
    async def test_rejects_invalid_body(
        self, client: httpx.AsyncClient, seed: dict[str, Any], body: dict[str, Any], error_type: str
    ) -> None:
        """complex_id가 없거나 1 이상의 정수가 아니면 422와 함께 실패한 필드와 오류 종류를 준다."""
        response = await client.post(FAVORITE_COMPLEXES_URL, json=body, headers=session_cookie_header(TOKEN))

        errors = response.json()["errors"]
        assert response.status_code == 422
        assert [(error["loc"], error["type"]) for error in errors] == [(["body", "complex_id"], error_type)]


class TestRemoveFavoriteComplex:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Any]]:
        async with seed_users_and_complexes(session_factory) as seeded:
            yield seeded

    async def test_removes_favorite(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        seed: dict[str, Any],
    ) -> None:
        """대상 단지 즐겨찾기만 제거하고 204를 준다. 다른 단지와 다른 사용자의 즐겨찾기는 남긴다."""
        user, other, apartment, officetel = seed["target"], seed["other"], seed["apartment"], seed["officetel"]
        async with seed_rows(session_factory, FavoriteComplex, mixed_favorite_rows(seed)):
            response = await client.delete(
                f"{FAVORITE_COMPLEXES_URL}/{apartment.id}", headers=session_cookie_header(TOKEN)
            )
            favorites = await favorite_complex_ids(session_factory, user)
            other_favorites = await favorite_complex_ids(session_factory, other)

        assert response.status_code == 204
        assert response.content == b""
        assert favorites == [officetel.id]
        assert other_favorites == [apartment.id]

    @pytest.mark.parametrize("missing", [False, True], ids=["not_favorite", "missing_complex"])
    async def test_ignores_absent_favorite(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        seed: dict[str, Any],
        missing: bool,
    ) -> None:
        """즐겨찾기하지 않은 단지나 없는 단지여도 204를 주고 다른 즐겨찾기는 남긴다."""
        user, apartment, officetel = seed["target"], seed["apartment"], seed["officetel"]
        complex_id = max(apartment.id, officetel.id) + 1 if missing else officetel.id
        async with seed_rows(session_factory, FavoriteComplex, [{"user_id": user.id, "complex_id": apartment.id}]):
            response = await client.delete(
                f"{FAVORITE_COMPLEXES_URL}/{complex_id}", headers=session_cookie_header(TOKEN)
            )
            favorites = await favorite_complex_ids(session_factory, user)

        assert response.status_code == 204
        assert favorites == [apartment.id]

    @pytest.mark.parametrize("headers", UNAUTHENTICATED_HEADERS)
    async def test_rejects_unauthenticated(
        self, client: httpx.AsyncClient, seed: dict[str, Any], headers: dict[str, str]
    ) -> None:
        """쿠키가 없거나 세션이 무효하면 401 공통 오류 응답을 준다."""
        response = await client.delete(f"{FAVORITE_COMPLEXES_URL}/{seed['apartment'].id}", headers=headers)

        assert_unauthenticated(response)

    @pytest.mark.parametrize(
        ("complex_id", "error_type"),
        [
            pytest.param("0", "greater_than_equal", id="zero"),
            pytest.param("abc", "int_parsing", id="not_int"),
        ],
    )
    async def test_rejects_invalid_complex_id(
        self, client: httpx.AsyncClient, seed: dict[str, Any], complex_id: str, error_type: str
    ) -> None:
        """complex_id가 1 이상의 정수가 아니면 422와 함께 실패한 필드와 오류 종류를 준다."""
        response = await client.delete(f"{FAVORITE_COMPLEXES_URL}/{complex_id}", headers=session_cookie_header(TOKEN))

        errors = response.json()["errors"]
        assert response.status_code == 422
        assert [(error["loc"], error["type"]) for error in errors] == [(["path", "complex_id"], error_type)]


class TestListFavoriteComplexes:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Any]]:
        legal_dong_code_rows = [{"payload": {"sido_cd": "11", "sgg_cd": "110", "locatadd_nm": "서울특별시 종로구"}}]
        async with (
            seed_users_and_complexes(session_factory) as seeded,
            seed_rows(session_factory, LegalDongCodeRawItem, legal_dong_code_rows),
        ):
            yield seeded

    async def test_lists_favorites(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        seed: dict[str, Any],
    ) -> None:
        """내 즐겨찾기만 최근에 추가한 순으로 주고 주소를 지역명부터 이어 붙인다."""
        user, other, apartment, officetel = seed["target"], seed["other"], seed["apartment"], seed["officetel"]
        apartment_at = datetime(2026, 1, 1, tzinfo=UTC)
        officetel_at = datetime(2026, 2, 1, tzinfo=UTC)
        rows = [
            {"user_id": user.id, "complex_id": apartment.id, "created_at": apartment_at},
            {"user_id": user.id, "complex_id": officetel.id, "created_at": officetel_at},
            {"user_id": other.id, "complex_id": apartment.id},
        ]
        async with seed_rows(session_factory, FavoriteComplex, rows):
            response = await client.get(FAVORITE_COMPLEXES_URL, headers=session_cookie_header(TOKEN))

        items = response.json()["items"]
        assert response.status_code == 200
        assert [datetime.fromisoformat(item.pop("favorited_at")) for item in items] == [officetel_at, apartment_at]
        assert items == [
            {
                "complex_id": officetel.id,
                "property_type": "OFFICETEL",
                "building_name": "청운오피스텔",
                "build_year": 2000,
                "address": "서울특별시 종로구 청운동 1",
            },
            {
                "complex_id": apartment.id,
                "property_type": "APT",
                "building_name": "청운현대",
                "build_year": 2000,
                "address": "서울특별시 종로구 청운동 1",
            },
        ]

    async def test_returns_empty_list(self, client: httpx.AsyncClient, seed: dict[str, Any]) -> None:
        """즐겨찾기한 단지가 없으면 빈 목록을 준다."""
        response = await client.get(FAVORITE_COMPLEXES_URL, headers=session_cookie_header(TOKEN))

        assert response.status_code == 200
        assert response.json() == {"items": []}

    @pytest.mark.parametrize("headers", UNAUTHENTICATED_HEADERS)
    async def test_rejects_unauthenticated(
        self, client: httpx.AsyncClient, seed: dict[str, Any], headers: dict[str, str]
    ) -> None:
        """쿠키가 없거나 세션이 무효하면 401 공통 오류 응답을 준다."""
        response = await client.get(FAVORITE_COMPLEXES_URL, headers=headers)

        assert_unauthenticated(response)
