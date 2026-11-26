"""신규 거래 이벤트 대기열을 읽어 즐겨찾기한 사용자에게 인앱 알림을 만드는 워커 프로세스.

`python -m src.notification_worker`로 API 서버와 따로 띄운다. 대기열은 정기 갱신이 채우는 `transaction_events`다.

- 여러 워커가 함께 돌아도 `FOR UPDATE SKIP LOCKED`로 서로 다른 이벤트를 가져간다.
- 알림 생성과 처리 완료 표시가 한 트랜잭션이다. 처리 중에 프로세스가 죽으면 둘 다 롤백되고 잠금이 풀려
  다른 워커가 다시 가져간다. 알림이 빠지거나 두 번 생기지 않는다.
- 이벤트 하나가 실패하면 그 이벤트만 세이브포인트로 되돌리고 다음 시도를 늦춘다. 재시도를 다 쓰면 대기열에서 뺀다.
"""

import asyncio
import logging
import signal
from contextlib import suppress
from datetime import timedelta

from dotenv import load_dotenv
from sqlalchemy import func, literal, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from .db import dispose_engine, get_session_factory
from .logging_config import configure_logging
from .model.favorite import FavoriteComplex
from .model.notification import Notification, TransactionEvent

log = logging.getLogger(__name__)

# 한 트랜잭션에서 잠그는 이벤트 수. 단지 하나의 즐겨찾기 사용자만큼 알림을 넣으므로 트랜잭션이 길어지지 않게 둔다.
BATCH_SIZE = 100
# 대기열이 비었을 때 다시 볼 간격(초). 이벤트는 하루 한 번 회차 끝에 열리므로 짧게 잡을 이유가 없다.
POLL_INTERVAL = 10.0
# 이벤트 하나를 시도하는 최대 횟수. 다 쓰면 failed_at을 찍어 대기열에서 뺀다.
MAX_ATTEMPTS = 5
# 재시도 간격은 1, 2, 4, 8분으로 늘린다. 일시적인 DB 오류가 풀릴 시간을 둔다.
RETRY_BASE_DELAY = timedelta(minutes=1)
# last_error에 남길 길이. 예외 메시지에 SQL과 파라미터가 통째로 붙어 오는 경우가 있다.
ERROR_MESSAGE_LENGTH = 1000


async def notify_favorites(session: AsyncSession, *, event_id: int, complex_id: int) -> int:
    """이벤트 단지를 즐겨찾기한 사용자마다 알림을 만들고, 새로 만든 알림 수를 돌려준다."""
    # 즐겨찾기는 발행 시점이 아니라 처리 시점에 찾는다. 그 사이 즐겨찾기를 지운 사용자에게는 보내지 않는다.
    result = await session.execute(
        insert(Notification)
        .from_select(
            ["user_id", "event_id"],
            select(FavoriteComplex.user_id, literal(event_id)).where(FavoriteComplex.complex_id == complex_id),
        )
        .on_conflict_do_nothing(index_elements=["user_id", "event_id"])
    )
    return result.rowcount  # pyright: ignore[reportAttributeAccessIssue]


async def process_events(session_factory: async_sessionmaker[AsyncSession]) -> int:
    """처리할 이벤트를 한 묶음 잠가 알림을 만들고, 가져간 이벤트 수를 돌려준다."""
    async with session_factory() as session, session.begin():
        claimed = (
            await session.execute(
                select(TransactionEvent.id, TransactionEvent.complex_id, TransactionEvent.attempts)
                .where(
                    TransactionEvent.available_at <= func.now(),
                    TransactionEvent.processed_at.is_(None),
                    TransactionEvent.failed_at.is_(None),
                )
                .order_by(TransactionEvent.available_at, TransactionEvent.id)
                .limit(BATCH_SIZE)
                # 다른 워커가 잠근 이벤트는 기다리지 않고 건너뛴다.
                .with_for_update(skip_locked=True)
            )
        ).all()

        for event_id, complex_id, attempts in claimed:
            try:
                async with session.begin_nested():
                    created = await notify_favorites(session, event_id=event_id, complex_id=complex_id)
                    await session.execute(
                        update(TransactionEvent)
                        .where(TransactionEvent.id == event_id)
                        .values(processed_at=func.now(), attempts=attempts + 1)
                    )
            except Exception as error:
                # 세이브포인트만 되돌아가 같은 묶음의 다른 이벤트는 그대로 커밋된다.
                failed = attempts + 1 >= MAX_ATTEMPTS
                log.exception("신규 거래 이벤트 %d 처리 실패(%d/%d회)", event_id, attempts + 1, MAX_ATTEMPTS)
                await session.execute(
                    update(TransactionEvent)
                    .where(TransactionEvent.id == event_id)
                    .values(
                        attempts=attempts + 1,
                        last_error=repr(error)[:ERROR_MESSAGE_LENGTH],
                        failed_at=func.now() if failed else None,
                        available_at=func.now() + RETRY_BASE_DELAY * 2**attempts,
                    )
                )
            else:
                # 즐겨찾기한 사람이 없는 단지가 대부분이라 회차마다 수만 줄이 쌓여 알림을 만든 이벤트를 가린다.
                level = logging.INFO if created else logging.DEBUG
                log.log(level, "신규 거래 이벤트 %d: 단지 %d, 알림 %d건", event_id, complex_id, created)
    return len(claimed)


async def run_worker(stop: asyncio.Event) -> None:
    """멈추라는 신호가 올 때까지 대기열을 비우고, 비면 잠시 쉬었다 다시 본다."""
    session_factory = get_session_factory()
    while not stop.is_set():
        try:
            claimed = await process_events(session_factory)
        except Exception:
            # DB 연결이 끊긴 경우 등이다. 묶음째 롤백돼 잠금이 풀렸으니 쉬었다가 다시 가져간다.
            log.exception("대기열을 처리하지 못했다")
            claimed = 0
        # 한 묶음을 꽉 채웠으면 남은 이벤트가 더 있을 수 있어 쉬지 않고 이어 간다.
        if claimed < BATCH_SIZE:
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), POLL_INTERVAL)


async def main() -> None:
    """종료 신호를 받으면 처리 중인 묶음을 마치고 끝나도록 워커를 띄운다."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    # 쿠버네티스는 파드를 내릴 때 SIGTERM을 보낸다. 묶음 중간에 끊겨도 롤백되지만, 마치고 나가면 다시 할 일이 없다.
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)
    log.info("알림 워커 시작")
    try:
        await run_worker(stop)
    finally:
        await dispose_engine()
        log.info("알림 워커 종료")


if __name__ == "__main__":
    load_dotenv()
    configure_logging("worker")
    asyncio.run(main())
