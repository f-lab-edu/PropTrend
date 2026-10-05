"""단지 즐겨찾기 토글 API 테스트."""

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.model import Complex, FavoriteComplex, PropertyType, User, UserSession
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


async def favorite_complex_ids(session_factory: async_sessionmaker[AsyncSession], user: User) -> list[int]:
    """사용자가 즐겨찾기한 단지 id 목록."""
    async with session_factory() as session, session.begin():
        result = await session.scalars(
            select(FavoriteComplex.complex_id)
            .where(FavoriteComplex.user_id == user.id)
            .order_by(FavoriteComplex.complex_id)
        )
        return list(result)


class TestToggleFavoriteComplex:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Any]]:
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

    async def test_adds_favorite(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        seed: dict[str, Any],
    ) -> None:
        """즐겨찾기하지 않은 단지면 추가하고 is_favorite=true를 준다."""
        user, apartment = seed["target"], seed["apartment"]
        try:
            response = await client.post(
                FAVORITE_COMPLEXES_URL, json={"complex_id": apartment.id}, headers=session_cookie_header(TOKEN)
            )
            favorites = await favorite_complex_ids(session_factory, user)
        finally:
            async with session_factory() as session, session.begin():
                await session.execute(delete(FavoriteComplex).where(FavoriteComplex.user_id == user.id))

        assert response.status_code == 200
        assert response.json() == {"complex_id": apartment.id, "is_favorite": True}
        assert favorites == [apartment.id]

    async def test_removes_favorite(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        seed: dict[str, Any],
    ) -> None:
        """이미 즐겨찾기한 단지면 제거하고 is_favorite=false를 준다. 다른 즐겨찾기는 남긴다."""
        user, other, apartment, officetel = seed["target"], seed["other"], seed["apartment"], seed["officetel"]
        rows = [
            {"user_id": user.id, "complex_id": apartment.id},
            {"user_id": user.id, "complex_id": officetel.id},
            {"user_id": other.id, "complex_id": apartment.id},
        ]
        async with seed_rows(session_factory, FavoriteComplex, rows):
            response = await client.post(
                FAVORITE_COMPLEXES_URL, json={"complex_id": apartment.id}, headers=session_cookie_header(TOKEN)
            )
            favorites = await favorite_complex_ids(session_factory, user)
            other_favorites = await favorite_complex_ids(session_factory, other)

        assert response.status_code == 200
        assert response.json() == {"complex_id": apartment.id, "is_favorite": False}
        assert favorites == [officetel.id]
        assert other_favorites == [apartment.id]

    @pytest.mark.parametrize(
        "headers",
        [
            pytest.param({}, id="no_cookie"),
            pytest.param(session_cookie_header("unknown"), id="unknown_token"),
        ],
    )
    async def test_rejects_unauthenticated(
        self, client: httpx.AsyncClient, seed: dict[str, Any], headers: dict[str, str]
    ) -> None:
        """쿠키가 없거나 세션이 무효하면 401 공통 오류 응답을 준다."""
        response = await client.post(FAVORITE_COMPLEXES_URL, json={"complex_id": seed["apartment"].id}, headers=headers)

        assert response.status_code == 401
        assert response.json() == {
            "message": "로그인이 필요합니다",
            "errors": [],
            "trace_id": response.headers["X-Trace-ID"],
        }

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
