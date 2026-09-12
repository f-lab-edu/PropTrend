"""PostgreSQL 비동기 세션 구성"""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .model import Base

DEFAULT_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/prop_trend"

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
    """데이터베이스 세션 제공 함수"""
    async with get_session_factory()() as session, session.begin():
        yield session


async def get_session() -> AsyncIterator[AsyncSession]:
    """데이터베이스 세션 의존성 주입 함수"""
    async with get_session_factory()() as session:
        yield session


async def create_tables() -> None:
    """모델 메타데이터에 정의된 테이블 중 없는 것을 만든다."""
    async with get_engine().begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def dispose_engine() -> None:
    """앱 종료 시 커넥션 풀을 정리한다."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
