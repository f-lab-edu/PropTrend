"""갱신 파이프라인 테스트가 함께 쓰는 빌더와 가짜 객체."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, cast

import httpx
import pytest
from sqlalchemy import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

from src.jobs import collector

SERVICE_KEY = "test-service-key"

# 핸들러를 받아 MockTransport를 설치하고, 그 뒤로 나간 요청 목록을 돌려주는 함수.
ApiHandler = Callable[[httpx.Request], httpx.Response]
InstallApi = Callable[[ApiHandler], list[httpx.Request]]

# 전처리기의 _common이 요구하는 최소 필드. 개별 테스트는 오버라이드로 결측·이상값을 만든다.
_BASE_PAYLOAD = {
    "sggCd": "11110",
    "umdNm": "청운동",
    "dealYear": "2026",
    "dealMonth": "2",
    "dealDay": "27",
}


def rtms_payload(**overrides: Any) -> dict[str, Any]:
    """유효한 최소 payload를 만든다. 값이 None인 오버라이드는 키 자체를 지운다."""
    payload = _BASE_PAYLOAD | overrides
    return {key: value for key, value in payload.items() if value is not None}


def sale_payload(**overrides: Any) -> dict[str, Any]:
    """매매 전처리기가 요구하는 필수 필드까지 채운 payload."""
    return rtms_payload(**({"dealAmount": "36,900"} | overrides))


def rent_payload(**overrides: Any) -> dict[str, Any]:
    """전월세 전처리기가 요구하는 필수 필드까지 채운 payload."""
    return rtms_payload(**({"deposit": "30,000", "monthlyRent": "0"} | overrides))


def bronze_rows(*payloads: dict[str, Any], start_id: int = 1) -> list[RowMapping]:
    """수집기가 돌려주는 `id`/`payload` 매핑 모양으로 감싼다."""
    # 전처리기는 키로 읽기만 하므로 dict로 RowMapping을 대신한다.
    rows = [{"id": start_id + index, "payload": payload} for index, payload in enumerate(payloads)]
    return cast(list[RowMapping], rows)


class FakeResult:
    """`session.execute`의 반환값 중 적재·정리·조회가 실제로 읽는 부분만 흉내낸다."""

    def __init__(self, rowcount: int = 0, rows: Sequence[Any] = ()) -> None:
        self.rowcount = rowcount
        self._rows = list(rows)

    def mappings(self) -> FakeResult:
        return self

    def all(self) -> list[Any]:
        return self._rows


class FakeSession(AsyncSession):
    """실행된 문장을 모아두는 가짜 AsyncSession. 파이프라인이 세션에 쓰는 API는 execute뿐이다."""

    # 상속은 AsyncSession 자리에 넘기기 위한 것이다. 부모 __init__을 부르지 않아 엔진 없이 만들어진다.
    def __init__(self, rowcount: int = 0, rows: Sequence[Any] = ()) -> None:
        self.statements: list[Any] = []
        self._rowcount = rowcount
        self._rows = rows

    async def execute(self, statement: Any, params: Any = None) -> FakeResult:
        self.statements.append((statement, params))
        return FakeResult(self._rowcount, self._rows)


@pytest.fixture
def mock_api(monkeypatch: pytest.MonkeyPatch) -> InstallApi:
    """수집기가 내부에서 만드는 AsyncClient에 MockTransport를 끼우고, 나간 요청을 모아 돌려준다."""
    monkeypatch.setenv("DATA_GO_KR_SERVICE_KEY", SERVICE_KEY)

    def install(handler: ApiHandler) -> list[httpx.Request]:
        requests: list[httpx.Request] = []

        def record(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return handler(request)

        transport = httpx.MockTransport(record)
        # 교체 전의 클래스를 붙잡아 둔다. httpx.AsyncClient로 다시 부르면 자기 자신을 부른다.
        real_client = collector.httpx.AsyncClient

        def build_client(**kwargs: Any) -> httpx.AsyncClient:
            return real_client(transport=transport, **kwargs)

        monkeypatch.setattr(collector.httpx, "AsyncClient", build_client)
        return requests

    return install
