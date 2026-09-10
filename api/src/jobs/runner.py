"""갱신 작업의 실행 상태를 한 곳에서 들고, 예약 실행과 수동 실행을 조율한다."""

import asyncio
import logging
from dataclasses import dataclass, replace
from datetime import datetime

from .pipeline import DEFAULT_CONCURRENCY, DEFAULT_MONTHS, refresh_all
from .utils import KST

logger = logging.getLogger(__name__)


class RefreshAlreadyRunning(RuntimeError):
    """이미 갱신이 돌고 있어 새 실행을 시작할 수 없다."""

    def __init__(self, trigger: str | None, started_at: datetime | None) -> None:
        super().__init__(
            f"갱신이 이미 실행 중이다: trigger={trigger} started_at={started_at}"
        )
        self.trigger = trigger
        self.started_at = started_at


@dataclass(frozen=True)
class RefreshState:
    """마지막(또는 진행 중인) 갱신 1회의 상태."""

    running: bool = False
    trigger: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    summary: dict[str, int] | None = None
    error: str | None = None


class RefreshRunner:
    """`refresh_all`을 감싸 동시에 한 번만 돌게 만든다."""

    def __init__(self) -> None:
        self._state = RefreshState()
        # 태스크 참조를 붙들지 않으면 실행 도중 GC가 가져갈 수 있다.
        self._task: asyncio.Task[None] | None = None

    @property
    def state(self) -> RefreshState:
        return self._state

    def start(
        self,
        trigger: str = "manual",
        months: int = DEFAULT_MONTHS,
        concurrency: int = DEFAULT_CONCURRENCY,
    ) -> RefreshState:
        """갱신을 백그라운드로 띄우고 시작 시점의 상태를 돌려준다."""
        self._mark_started(trigger)
        self._task = asyncio.create_task(self._run(months, concurrency))
        return self._state

    async def run(
        self,
        trigger: str = "scheduler",
        months: int = DEFAULT_MONTHS,
        concurrency: int = DEFAULT_CONCURRENCY,
    ) -> RefreshState:
        """갱신을 끝까지 기다린다. 스케줄러가 등록하는 진입점이다."""
        self._mark_started(trigger)
        await self._run(months, concurrency)
        return self._state

    async def run_scheduled(self) -> None:
        """예약 실행. 수동 실행과 겹치면 이번 회차를 건너뛴다."""
        try:
            await self.run("scheduler")
        except RefreshAlreadyRunning as exc:
            logger.warning("갱신이 이미 실행 중이라 예약 실행을 건너뛴다: %s", exc)

    def _mark_started(self, trigger: str) -> None:
        """실행 자리를 선점한다."""
        if self._state.running:
            raise RefreshAlreadyRunning(self._state.trigger, self._state.started_at)
        self._state = RefreshState(
            running=True, trigger=trigger, started_at=datetime.now(KST)
        )

    async def _run(self, months: int, concurrency: int) -> None:
        try:
            summary = await refresh_all(months=months, concurrency=concurrency)
        except Exception as exc:
            # 백그라운드 태스크에서 예외를 흘리면 아무도 받지 않는다. 상태에 남겨
            # 조회로 확인할 수 있게 한다.
            logger.exception("갱신 실행 실패")
            self._state = replace(
                self._state,
                running=False,
                finished_at=datetime.now(KST),
                error=f"{type(exc).__name__}: {exc}",
            )
        else:
            self._state = replace(
                self._state,
                running=False,
                finished_at=datetime.now(KST),
                summary=summary,
            )
