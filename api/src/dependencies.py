"""라우터가 공유하는 의존성"""

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession

from .db import get_session_factory


async def get_session() -> AsyncIterator[AsyncSession]:
    """데이터베이스 세션 의존성 주입 함수"""
    async with get_session_factory()() as session:
        yield session
