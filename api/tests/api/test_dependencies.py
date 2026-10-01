"""X-API-KEY 헤더 검증 의존성 테스트."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from pydantic import SecretStr

from src.config import Settings, get_settings
from src.dependencies import get_session
from src.main import app

API_KEY = "test-api-key"
URL = "/api/prop-transactions/sales"


class _EmptyResult:
    """조회 결과가 없는 `session.execute` 반환값."""

    def scalars(self) -> list[Any]:
        return []

    def tuples(self) -> list[Any]:
        return []


class _EmptySession:
    """DB 없이 빈 결과를 돌려주는 가짜 AsyncSession."""

    @asynccontextmanager
    async def begin(self) -> AsyncIterator[None]:
        yield

    async def execute(self, statement: Any) -> _EmptyResult:
        return _EmptyResult()


class TestVerifyApiKey:
    @pytest.fixture(autouse=True)
    def setup(self) -> Any:
        app.dependency_overrides[get_session] = _EmptySession
        app.dependency_overrides[get_settings] = lambda: Settings(api_key=SecretStr(API_KEY))
        yield
        app.dependency_overrides.clear()

    @pytest.fixture
    async def client(self) -> AsyncIterator[httpx.AsyncClient]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client

    async def test_passes_with_valid_key(self, client: httpx.AsyncClient) -> None:
        response = await client.get(URL, params={"property_type": "APT"}, headers={"X-API-KEY": API_KEY})

        assert response.status_code == 200
        assert response.json() == []

    async def test_rejects_missing_key(self, client: httpx.AsyncClient) -> None:
        response = await client.get(URL, params={"property_type": "APT"})

        assert response.status_code == 401
        assert response.json() == {
            "message": "API 키가 올바르지 않습니다",
            "errors": [],
            "trace_id": response.headers.get("X-Trace-ID"),
        }

    async def test_rejects_wrong_key(self, client: httpx.AsyncClient) -> None:
        response = await client.get(URL, params={"property_type": "APT"}, headers={"X-API-KEY": "wrong"})

        assert response.status_code == 401
        assert response.json() == {
            "message": "API 키가 올바르지 않습니다",
            "errors": [],
            "trace_id": response.headers.get("X-Trace-ID"),
        }

    async def test_health_check_needs_no_key(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/health")

        assert response.status_code == 200
