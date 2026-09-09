"""PostgreSQL 비동기 세션 구성.

엔진/세션 팩토리는 첫 사용 시점에 만든다. main.py에서 load_dotenv()가 끝난 뒤에
DATABASE_URL을 읽기 위해서다.
"""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

DEFAULT_DATABASE_URL = (
    "postgresql+asyncpg://postgres:postgres@localhost:5432/prop_trend"
)

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
        _engine = create_async_engine(url, pool_pre_ping=True)
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(get_engine(), expire_on_commit=False)
    return _session_factory


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """세션을 열어 넘기고, 블록이 정상 종료되면 commit한다.

    수집 잡처럼 요청 흐름 밖에서 세션이 필요한 곳에서 쓴다.

        async with session_scope() as session:
            await RawDataLoader(session, RawLegalDongCode).load(rows)
    """
    async with get_session_factory()() as session:
        async with session.begin():
            yield session


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI 의존성 주입용 세션. 트랜잭션은 사용하는 쪽에서 관리한다."""
    async with get_session_factory()() as session:
        yield session


async def dispose_engine() -> None:
    """앱 종료 시 커넥션 풀을 정리한다."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
