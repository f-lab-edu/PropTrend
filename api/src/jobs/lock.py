"""갱신 프로세스끼리 같은 범위를 동시에 건드리지 않게 하는 PostgreSQL advisory lock."""

import zlib
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy import func, select

from ..db import get_engine

# "prop"의 ASCII. 두 키 형식의 첫 키로 이 프로젝트의 락 공간을 다른 advisory lock 사용처와 가른다.
LOCK_NAMESPACE = 0x70726F70
# 법정동코드·지역 교체. 실거래 범위는 PipelineSpec.api_id를 그대로 쓴다.
LEGAL_DONG_SCOPE = "legal_dong"


def scope_key(scope: str) -> int:
    """범위 이름을 int4 키로 바꾼다."""
    # 파이썬 hash()는 프로세스마다 시드가 달라 다른 프로세스와 같은 키를 얻지 못한다.
    return zlib.crc32(scope.encode()) & 0x7FFFFFFF


@asynccontextmanager
async def advisory_lock(*scopes: str) -> AsyncGenerator[bool]:
    """모든 범위를 잡았는지 알려준다. 잡은 범위는 블록이 끝날 때까지 전용 커넥션이 붙들고 있는다."""
    if not scopes:
        # 범위 없이 True를 돌려주면 아무것도 막지 않는 락이 조용히 통과한다.
        raise ValueError("잡을 락 범위가 없다")

    async with get_engine().connect() as conn:
        await conn.execution_options(isolation_level="AUTOCOMMIT")
        held: list[int] = []
        try:
            # 같은 세션의 advisory lock은 중첩되므로, 범위가 겹치면 푸는 횟수가 모자라지 않게 중복을 없앤다.
            # try_lock은 기다리지 않아 순서와 무관하게 교착이 없고, 정렬은 pg_locks를 읽기 쉽게 하려는 것이다.
            for scope in sorted(set(scopes)):
                key = scope_key(scope)
                if not await conn.scalar(select(func.pg_try_advisory_lock(LOCK_NAMESPACE, key))):
                    break
                held.append(key)
            yield len(held) == len(set(scopes))
        finally:
            # 일부만 잡았어도 푼다. 커넥션은 닫히지 않고 풀로 돌아가므로 여기서 풀지 않으면 락이 남는다.
            for key in held:
                await conn.scalar(select(func.pg_advisory_unlock(LOCK_NAMESPACE, key)))
