"""로그인·로그아웃·내 정보 조회 API 테스트."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from http.cookies import SimpleCookie
from typing import Any

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.model import User, UserSession
from src.schemas.user import UserResponse
from src.services.user import SESSION_TTL, hash_session_token, password_hasher

from .conftest import seed_rows

# 세션 범위 엔진의 커넥션을 같은 이벤트 루프에서 재사용한다.
pytestmark = pytest.mark.asyncio(loop_scope="session")

LOGIN_URL = "/api/users/login"
LOGOUT_URL = "/api/users/logout"
ME_URL = "/api/users/me"

PASSWORD = "correct-password"  # noqa: S105 - 테스트 시드 사용자의 비밀번호
# 해싱이 수십 ms 걸리므로 모듈에서 한 번만 만든다.
USER_ROW = {"nickname": "테스터", "password": password_hasher.hash(PASSWORD)}

UNAUTHENTICATED_MESSAGE = "로그인이 필요합니다"


def session_cookie_header(token: str) -> dict[str, str]:
    """세션 쿠키를 실은 요청 헤더를 만든다."""
    # 쿠키가 Secure라 http://test 클라이언트의 쿠키 저장소는 보내지 않으므로 헤더로 직접 싣는다.
    return {"Cookie": f"session_id={token}"}


def session_row(user: User, token: str, expires_in: timedelta) -> dict[str, Any]:
    """사용자의 세션 시드 행을 만든다."""
    return {"token_hash": hash_session_token(token), "user_id": user.id, "expires_at": datetime.now(UTC) + expires_in}


class TestLogin:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, User]]:
        # 로그인으로 생긴 세션은 사용자가 지워질 때 FK의 ON DELETE CASCADE로 함께 지워진다.
        rows = {"target": USER_ROW | {"email": "login@example.com"}}
        async with seed_rows(session_factory, User, list(rows.values())) as users:
            yield dict(zip(rows, users, strict=True))

    async def test_issues_session_cookie(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        seed: dict[str, User],
    ) -> None:
        """이메일·비밀번호가 맞으면 본문 없이 200과 HttpOnly 세션 쿠키를 주고, 해시로 세션을 저장한다."""
        user = seed["target"]
        async with seed_rows(session_factory, UserSession, [session_row(user, "expired", -timedelta(seconds=1))]):
            response = await client.post(LOGIN_URL, json={"email": user.email, "password": PASSWORD})

            async with session_factory() as session, session.begin():
                sessions = list(await session.scalars(select(UserSession).where(UserSession.user_id == user.id)))

        assert response.status_code == 200
        assert response.content == b""
        cookie = SimpleCookie(response.headers["set-cookie"])["session_id"]
        assert cookie["httponly"] is True
        assert cookie["secure"] is True
        assert cookie["samesite"] == "lax"
        assert cookie["path"] == "/"
        assert cookie["max-age"] == str(int(SESSION_TTL.total_seconds()))
        # 로그인하면서 같은 사용자의 만료된 세션은 지우고 새 세션 하나만 남긴다.
        assert [row.token_hash for row in sessions] == [hash_session_token(cookie.value)]
        assert abs(sessions[0].expires_at - (datetime.now(UTC) + SESSION_TTL)) < timedelta(minutes=1)

    async def test_rejects_wrong_password(self, client: httpx.AsyncClient, seed: dict[str, User]) -> None:
        """비밀번호가 틀리면 쿠키 없이 401 공통 오류 응답을 준다."""
        response = await client.post(LOGIN_URL, json={"email": seed["target"].email, "password": "wrong-password"})

        assert response.status_code == 401
        assert "set-cookie" not in response.headers
        assert response.json() == {
            "message": "이메일 또는 비밀번호가 올바르지 않습니다",
            "errors": [],
            "trace_id": response.headers["X-Trace-ID"],
        }

    async def test_rejects_unknown_email(self, client: httpx.AsyncClient) -> None:
        """가입되지 않은 이메일이면 비밀번호가 틀렸을 때와 같은 401을 준다."""
        response = await client.post(LOGIN_URL, json={"email": "nobody@example.com", "password": PASSWORD})

        assert response.status_code == 401
        assert "set-cookie" not in response.headers
        assert response.json()["message"] == "이메일 또는 비밀번호가 올바르지 않습니다"

    @pytest.mark.parametrize(
        ("body", "field", "error_type"),
        [
            pytest.param({"password": PASSWORD}, "email", "missing", id="missing_email"),
            pytest.param({"email": "login@example.com"}, "password", "missing", id="missing_password"),
            pytest.param({"email": "not-an-email", "password": PASSWORD}, "email", "value_error", id="invalid_email"),
        ],
    )
    async def test_rejects_invalid_body(
        self, client: httpx.AsyncClient, body: dict[str, str], field: str, error_type: str
    ) -> None:
        """요청 본문 형식이 잘못되면 422와 함께 실패한 필드와 오류 종류를 준다."""
        response = await client.post(LOGIN_URL, json=body)

        result = response.json()
        assert response.status_code == 422
        assert result["message"] == "요청 값이 올바르지 않습니다"
        assert [(error["loc"], error["type"]) for error in result["errors"]] == [(["body", field], error_type)]


class TestLogout:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, User]]:
        rows = {"target": USER_ROW | {"email": "logout@example.com"}}
        async with seed_rows(session_factory, User, list(rows.values())) as users:
            yield dict(zip(rows, users, strict=True))

    async def test_deletes_session_and_cookie(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        seed: dict[str, User],
    ) -> None:
        """세션 행을 지우고, 같은 속성으로 만료시킨 쿠키를 내려 브라우저에서도 지운다."""
        async with seed_rows(session_factory, UserSession, [session_row(seed["target"], "logout", SESSION_TTL)]):
            response = await client.post(LOGOUT_URL, headers=session_cookie_header("logout"))

            async with session_factory() as session, session.begin():
                remaining = await session.get(UserSession, hash_session_token("logout"))

        assert response.status_code == 204
        assert remaining is None
        cookie = SimpleCookie(response.headers["set-cookie"])["session_id"]
        assert cookie.value == ""
        assert cookie["max-age"] == "0"
        assert (cookie["httponly"], cookie["secure"], cookie["samesite"], cookie["path"]) == (True, True, "lax", "/")

    @pytest.mark.parametrize(
        "headers",
        [
            pytest.param({}, id="no_cookie"),
            pytest.param(session_cookie_header("unknown"), id="unknown_token"),
        ],
    )
    async def test_succeeds_without_valid_session(self, client: httpx.AsyncClient, headers: dict[str, str]) -> None:
        """쿠키가 없거나 없는 세션이어도 같은 204를 준다."""
        response = await client.post(LOGOUT_URL, headers=headers)

        assert response.status_code == 204
        assert SimpleCookie(response.headers["set-cookie"])["session_id"]["max-age"] == "0"


class TestGetMe:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, User]]:
        rows = {"target": USER_ROW | {"email": "me@example.com"}}
        async with seed_rows(session_factory, User, list(rows.values())) as users:
            user = users[0]
            sessions = [session_row(user, "valid", SESSION_TTL), session_row(user, "expired", -timedelta(seconds=1))]
            async with seed_rows(session_factory, UserSession, sessions):
                yield dict(zip(rows, users, strict=True))

    async def test_returns_current_user(self, client: httpx.AsyncClient, seed: dict[str, User]) -> None:
        """유효한 세션 쿠키면 비밀번호를 뺀 사용자 정보를 준다."""
        user = seed["target"]

        response = await client.get(ME_URL, headers=session_cookie_header("valid"))

        assert response.status_code == 200
        assert response.json() == UserResponse.model_validate(user).model_dump(mode="json")

    @pytest.mark.parametrize(
        "headers",
        [
            pytest.param({}, id="no_cookie"),
            pytest.param(session_cookie_header("expired"), id="expired_session"),
            pytest.param(session_cookie_header("unknown"), id="unknown_token"),
        ],
    )
    async def test_rejects_unauthenticated(
        self, client: httpx.AsyncClient, seed: dict[str, User], headers: dict[str, str]
    ) -> None:
        """쿠키가 없거나 세션이 만료·무효하면 401 공통 오류 응답을 준다."""
        response = await client.get(ME_URL, headers=headers)

        assert response.status_code == 401
        assert response.json() == {
            "message": UNAUTHENTICATED_MESSAGE,
            "errors": [],
            "trace_id": response.headers["X-Trace-ID"],
        }
