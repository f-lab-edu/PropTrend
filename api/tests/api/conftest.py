"""API 테스트가 함께 쓰는 클라이언트, 테스트 DB 세션, 시드 데이터 삽입 함수."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import delete, insert, inspect, tuple_
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from src.dependencies import get_session
from src.main import app
from src.model import Base
from src.services import prop_transaction as prop_transaction_service


@asynccontextmanager
async def seed_rows[ModelT: Base](
    session_factory: async_sessionmaker[AsyncSession], model: type[ModelT], rows: list[dict[str, Any]]
) -> AsyncIterator[list[ModelT]]:
    """모델 테이블에 행을 넣어 넘긴 순서대로 돌려주고, 블록을 벗어나면 넣은 행만 지운다."""
    async with session_factory() as session, session.begin():
        result = await session.scalars(insert(model).returning(model, sort_by_parameter_order=True), rows)
        inserted = list(result)
    try:
        yield inserted
    finally:
        # 복합 기본키 테이블(raw_load_progress 등)도 지울 수 있게 기본키 튜플로 비교한다.
        mapper = inspect(model)
        keys = [mapper.primary_key_from_instance(row) for row in inserted]
        async with session_factory() as session, session.begin():
            await session.execute(delete(model).where(tuple_(*mapper.primary_key).in_(keys)))


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def session_factory(test_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """운영 코드와 같은 설정으로 테스트 DB 세션을 만드는 팩토리."""
    return async_sessionmaker(test_engine, expire_on_commit=False, autobegin=False)


@pytest_asyncio.fixture(loop_scope="session")
async def client(
    monkeypatch: pytest.MonkeyPatch, session_factory: async_sessionmaker[AsyncSession]
) -> AsyncIterator[httpx.AsyncClient]:
    """테스트 DB 세션을 주입한 클라이언트."""

    async def get_test_session() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    # 지역명 캐시가 앞 테스트의 시드를 들고 있지 않도록 매번 비운다.
    monkeypatch.setattr(prop_transaction_service, "_region_names_loaded_at", None)
    app.dependency_overrides[get_session] = get_test_session
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()
