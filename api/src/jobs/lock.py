"""갱신 프로세스가 동시에 두 개 뜨는 것을 막는 PostgreSQL advisory lock."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy import func, select

from ..db import get_engine

# "prop"의 ASCII. 이 프로젝트의 갱신 실행 전체가 이 키 하나를 두고 겨룬다.
LOCK_KEY = 0x70726F70


@asynccontextmanager
async def advisory_lock(key: int = LOCK_KEY) -> AsyncGenerator[bool]:
    """락을 잡았는지 알려준다. 잡았다면 블록이 끝날 때까지 전용 커넥션이 붙들고 있는다."""
    async with get_engine().connect() as conn:
        await conn.execution_options(isolation_level="AUTOCOMMIT")
        acquired = bool(await conn.scalar(select(func.pg_try_advisory_lock(key))))
        try:
            yield acquired
        finally:
            if acquired:
                await conn.scalar(select(func.pg_advisory_unlock(key)))
