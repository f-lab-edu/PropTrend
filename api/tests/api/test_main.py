"""앱 수준 엔드포인트 테스트."""

from collections.abc import AsyncIterator

import httpx
import pytest

from src.main import app


class TestHealthCheck:
    @pytest.fixture
    async def client(self) -> AsyncIterator[httpx.AsyncClient]:
        # DB를 거치지 않는 엔드포인트라 테스트 DB 없이 앱만 띄운다.
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client

    async def test_returns_ok(self, client: httpx.AsyncClient) -> None:
        """인증 없이 200과 상태 ok를 준다."""
        response = await client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestPublicPage:
    @pytest.fixture
    async def client(self) -> AsyncIterator[httpx.AsyncClient]:
        # 정적 파일만 서빙하므로 테스트 DB 없이 앱만 띄운다.
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client

    async def test_returns_index_html(self, client: httpx.AsyncClient) -> None:
        """루트 경로에서 public/index.html을 준다."""
        response = await client.get("/")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/html")

    async def test_returns_not_found_for_unknown_path(self, client: httpx.AsyncClient) -> None:
        """없는 경로는 공통 오류 응답 형식의 404를 준다."""
        response = await client.get("/not-exists.html")

        assert response.status_code == 404
        assert response.json()["message"] == "Not Found"
