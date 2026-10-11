"""범위별 advisory lock의 배타성과 해제. 실제 Postgres에 커넥션을 둘 붙여야 드러난다."""

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from src.jobs import lock as lock_module
from src.jobs.lock import LEGAL_DONG_SCOPE, LOCK_NAMESPACE, advisory_lock, scope_key
from src.jobs.pipeline import PIPELINES

# 세션 범위 엔진과 이벤트 루프를 맞춘다.
session_loop = pytest.mark.asyncio(loop_scope="session")


@pytest_asyncio.fixture(loop_scope="session")
async def other(monkeypatch: pytest.MonkeyPatch, test_engine: AsyncEngine) -> AsyncIterator[AsyncConnection]:
    """advisory_lock과 다른 세션을 열고, advisory_lock도 같은 테스트 DB를 쓰게 한다."""
    monkeypatch.setattr(lock_module, "get_engine", lambda: test_engine)
    # 같은 세션의 advisory lock은 중첩돼 배타성이 보이지 않으므로 커넥션을 따로 둔다.
    async with test_engine.connect() as conn:
        await conn.execution_options(isolation_level="AUTOCOMMIT")
        yield conn
        # 풀로 돌아간 커넥션에 락이 남으면 다음 테스트가 엉뚱하게 실패한다.
        await conn.scalar(select(func.pg_advisory_unlock_all()))


async def try_lock(conn: AsyncConnection, scope: str) -> bool:
    return bool(await conn.scalar(select(func.pg_try_advisory_lock(LOCK_NAMESPACE, scope_key(scope)))))


def test_scope_keys_are_distinct() -> None:
    scopes = [LEGAL_DONG_SCOPE, *(spec.api_id for spec in PIPELINES)]

    # crc32가 겹치면 서로 무관한 두 범위가 조용히 서로를 막는다.
    assert len({scope_key(scope) for scope in scopes}) == len(scopes)


async def test_requires_at_least_one_scope() -> None:
    with pytest.raises(ValueError, match="범위가 없다"):
        async with advisory_lock():
            pass


@session_loop
async def test_held_scope_blocks_other_session_until_block_ends(other: AsyncConnection) -> None:
    async with advisory_lock("apart_sale") as acquired:
        assert acquired
        assert not await try_lock(other, "apart_sale")

    # 커넥션은 닫히지 않고 풀로 돌아간다. 명시적으로 풀지 않으면 여기서 잡히지 않는다.
    assert await try_lock(other, "apart_sale")


@session_loop
async def test_different_scopes_do_not_block_each_other(other: AsyncConnection) -> None:
    assert await try_lock(other, "apart_sale")

    async with advisory_lock("apart_rent", LEGAL_DONG_SCOPE) as acquired:
        assert acquired


@session_loop
async def test_partial_acquire_reports_false_and_releases_what_it_held(other: AsyncConnection) -> None:
    assert await try_lock(other, "apart_sale")

    # 정렬 순서상 apart_rent를 먼저 잡고 apart_sale에서 막힌다.
    async with advisory_lock("apart_sale", "apart_rent") as acquired:
        assert not acquired

    assert await try_lock(other, "apart_rent")


@session_loop
async def test_duplicate_scopes_are_released_completely(other: AsyncConnection) -> None:
    async with advisory_lock("apart_sale", "apart_sale") as acquired:
        assert acquired

    # 같은 세션에서 두 번 잡으면 두 번 풀어야 한다. 한 번만 풀면 락이 남는다.
    assert await try_lock(other, "apart_sale")
