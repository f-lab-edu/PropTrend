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
