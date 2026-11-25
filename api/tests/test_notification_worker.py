"""알림 워커가 대기열의 이벤트를 한 번씩만, 실패해도 다른 이벤트를 막지 않고 처리하는지 본다."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from src import notification_worker
from src.model import Complex, FavoriteComplex, Notification, TransactionEvent, User
from src.notification_worker import MAX_ATTEMPTS, process_events, run_worker

from .api.conftest import seed_rows
from .api.test_favorite import APARTMENT_ROW, OFFICETEL_ROW
from .api.test_user import USER_ROW

pytestmark = pytest.mark.asyncio(loop_scope="session")

PAST = datetime(2026, 1, 1, tzinfo=UTC)


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def session_factory(test_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """워커와 같은 설정으로 테스트 DB 세션을 만드는 팩토리."""
    return async_sessionmaker(test_engine, expire_on_commit=False, autobegin=False)


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def seed(session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Any]]:
    """사용자 셋과, 둘이 즐겨찾기한 아파트·한 명이 즐겨찾기한 오피스텔 단지."""
    user_rows = [USER_ROW | {"email": f"worker-{index}@example.com"} for index in range(3)]
    async with (
        seed_rows(session_factory, User, user_rows) as users,
        seed_rows(session_factory, Complex, [APARTMENT_ROW, OFFICETEL_ROW]) as (apartment, officetel),
        seed_rows(
            session_factory,
            FavoriteComplex,
            [
                {"user_id": users[0].id, "complex_id": apartment.id},
                {"user_id": users[1].id, "complex_id": apartment.id},
                {"user_id": users[2].id, "complex_id": officetel.id},
            ],
        ),
    ):
        yield {"users": users, "apartment": apartment, "officetel": officetel}


@asynccontextmanager
async def seed_events(
    session_factory: async_sessionmaker[AsyncSession], *rows: dict[str, Any]
) -> AsyncIterator[list[TransactionEvent]]:
    """이벤트를 넣는다. 블록을 벗어나면 이벤트와 함께 그 알림도 CASCADE로 지운다."""
    async with seed_rows(session_factory, TransactionEvent, list(rows)) as events:
        yield events


async def event_state(session_factory: async_sessionmaker[AsyncSession], event_id: int) -> TransactionEvent:
    async with session_factory() as session, session.begin():
        event = await session.get(TransactionEvent, event_id)
        assert event is not None
        return event


async def notified_users(session_factory: async_sessionmaker[AsyncSession], event_id: int) -> list[int]:
    async with session_factory() as session, session.begin():
        return list(
            await session.scalars(
                select(Notification.user_id).where(Notification.event_id == event_id).order_by(Notification.user_id)
            )
        )


async def test_notifies_every_user_who_favorited_the_complex(
    session_factory: async_sessionmaker[AsyncSession], seed: dict[str, Any]
) -> None:
    """열린 이벤트마다 그 단지를 즐겨찾기한 사용자에게만 알림을 만들고 처리 완료로 찍는다."""
    users, apartment, officetel = seed["users"], seed["apartment"], seed["officetel"]
    async with seed_events(
        session_factory,
        {"complex_id": apartment.id, "sale_count": 1, "available_at": PAST},
        {"complex_id": officetel.id, "rent_count": 2, "available_at": PAST},
    ) as (apartment_event, officetel_event):
        claimed = await process_events(session_factory)
        apartment_users = await notified_users(session_factory, apartment_event.id)
        officetel_users = await notified_users(session_factory, officetel_event.id)
        state = await event_state(session_factory, apartment_event.id)

    assert claimed == 2
    assert apartment_users == [users[0].id, users[1].id]
    assert officetel_users == [users[2].id]
    assert state.processed_at is not None
    assert state.attempts == 1


async def test_leaves_unpublished_and_postponed_events(
    session_factory: async_sessionmaker[AsyncSession], seed: dict[str, Any]
) -> None:
    """회차가 끝나지 않았거나 재시도 시각이 오지 않은 이벤트는 가져가지 않는다."""
    future = datetime.now(UTC) + timedelta(hours=1)
    async with seed_events(
        session_factory,
        {"complex_id": seed["apartment"].id, "sale_count": 1, "available_at": None},
        {"complex_id": seed["officetel"].id, "sale_count": 1, "available_at": future},
    ) as events:
        claimed = await process_events(session_factory)
        notified = [await notified_users(session_factory, event.id) for event in events]

    assert claimed == 0
    assert notified == [[], []]


async def test_processes_event_once(session_factory: async_sessionmaker[AsyncSession], seed: dict[str, Any]) -> None:
    """처리한 이벤트는 다시 가져가지 않고, 처리 표시가 빠진 채 다시 와도 알림이 겹치지 않는다."""
    async with seed_events(
        session_factory, {"complex_id": seed["apartment"].id, "sale_count": 1, "available_at": PAST}
    ) as (event,):
        first = await process_events(session_factory)
        second = await process_events(session_factory)
        # 같은 이벤트가 다시 전달된 상황을 만든다.
        async with session_factory() as session, session.begin():
            await session.execute(
                update(TransactionEvent).where(TransactionEvent.id == event.id).values(processed_at=None)
            )
        redelivered = await process_events(session_factory)
        users = await notified_users(session_factory, event.id)

    assert (first, second, redelivered) == (1, 0, 1)
    assert users == [seed["users"][0].id, seed["users"][1].id]


async def test_failed_event_is_retried_later_without_blocking_others(
    session_factory: async_sessionmaker[AsyncSession], seed: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """이벤트 하나가 실패하면 그 이벤트만 되돌리고 다음 시도를 늦춘다. 같은 묶음의 다른 이벤트는 처리된다."""
    apartment, officetel = seed["apartment"], seed["officetel"]
    notify_favorites = notification_worker.notify_favorites

    async def fail_on_apartment(session: AsyncSession, *, event_id: int, complex_id: int) -> int:
        created = await notify_favorites(session, event_id=event_id, complex_id=complex_id)
        # 알림을 넣은 뒤에 실패해야 세이브포인트가 넣은 알림까지 되돌리는지 보인다.
        if complex_id == apartment.id:
            raise RuntimeError("일시 오류")
        return created

    monkeypatch.setattr(notification_worker, "notify_favorites", fail_on_apartment)
    async with seed_events(
        session_factory,
        {"complex_id": apartment.id, "sale_count": 1, "available_at": PAST},
        {"complex_id": officetel.id, "rent_count": 1, "available_at": PAST},
    ) as (failing, succeeding):
        before = datetime.now(UTC)
        claimed = await process_events(session_factory)
        failed_state = await event_state(session_factory, failing.id)
        failed_users = await notified_users(session_factory, failing.id)
        succeeded_state = await event_state(session_factory, succeeding.id)
        retried = await process_events(session_factory)

    assert claimed == 2
    assert failed_users == []
    assert failed_state.processed_at is None
    assert failed_state.failed_at is None
    assert failed_state.attempts == 1
    assert failed_state.last_error == "RuntimeError('일시 오류')"
    assert failed_state.available_at is not None
    assert failed_state.available_at > before
    assert succeeded_state.processed_at is not None
    # 재시도 시각 전이라 바로 다시 가져가지 않는다.
    assert retried == 0


async def test_event_leaves_queue_after_last_attempt(
    session_factory: async_sessionmaker[AsyncSession], seed: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """재시도를 다 쓴 이벤트는 failed_at을 찍어 대기열에서 뺀다."""

    async def always_fail(session: AsyncSession, *, event_id: int, complex_id: int) -> int:
        raise RuntimeError("영구 오류")

    monkeypatch.setattr(notification_worker, "notify_favorites", always_fail)
    async with seed_events(
        session_factory,
        {"complex_id": seed["apartment"].id, "sale_count": 1, "available_at": PAST, "attempts": MAX_ATTEMPTS - 1},
    ) as (event,):
        await process_events(session_factory)
        state = await event_state(session_factory, event.id)

    assert state.attempts == MAX_ATTEMPTS
    assert state.failed_at is not None
    assert state.processed_at is None


async def test_skips_event_locked_by_another_worker(
    session_factory: async_sessionmaker[AsyncSession], seed: dict[str, Any]
) -> None:
    """다른 워커가 잠근 이벤트는 기다리지 않고 건너뛰고, 잠금이 풀리면 가져간다."""
    async with seed_events(
        session_factory, {"complex_id": seed["apartment"].id, "sale_count": 1, "available_at": PAST}
    ) as (event,):
        async with session_factory() as other_worker, other_worker.begin():
            await other_worker.execute(
                select(TransactionEvent.id).where(TransactionEvent.id == event.id).with_for_update()
            )
            while_locked = await asyncio.wait_for(process_events(session_factory), timeout=5)
        after_release = await process_events(session_factory)

    assert (while_locked, after_release) == (0, 1)


async def test_worker_drains_queue_until_stopped(
    session_factory: async_sessionmaker[AsyncSession], seed: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """멈추라는 신호가 올 때까지 대기열을 비우고, 신호를 받으면 쉬던 중이어도 바로 끝난다."""
    monkeypatch.setattr(notification_worker, "get_session_factory", lambda: session_factory)
    async with seed_events(
        session_factory, {"complex_id": seed["apartment"].id, "sale_count": 1, "available_at": PAST}
    ) as (event,):
        stop = asyncio.Event()
        worker = asyncio.create_task(run_worker(stop))
        for _ in range(50):
            if (await event_state(session_factory, event.id)).processed_at is not None:
                break
            await asyncio.sleep(0.1)
        stop.set()
        # POLL_INTERVAL(10초)을 기다리지 않고 끝나야 한다.
        await asyncio.wait_for(worker, timeout=2)
        users = await notified_users(session_factory, event.id)

    assert users == [seed["users"][0].id, seed["users"][1].id]
