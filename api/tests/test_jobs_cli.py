"""갱신 결과를 종료 코드로 줄이는 판정, 인자 검증, 그리고 락 분기."""

import argparse
import logging
import sys
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

import pytest

from src.jobs import __main__ as main_module
from src.jobs.__main__ import EXIT_FAILED, EXIT_OK, bounded_int, decide_exit_code
from src.jobs.pipeline import DEFAULT_CONCURRENCY, DEFAULT_MONTHS

# 가짜 advisory_lock을 받아 (종료 코드, refresh_all 호출 인자)를 돌려주는 함수.
Lock = Callable[[], AbstractAsyncContextManager[bool]]
RunMain = Callable[[Lock], Coroutine[Any, Any, tuple[int, list[dict[str, int]]]]]


def summary(**overrides: int) -> dict[str, int]:
    return {"daily_limit": 0, "failed": 0, "silver_failed": 0} | overrides


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (summary(), EXIT_OK),
        # bronze 단계 실패는 1건이라도 전량 재실행 대상이다.
        (summary(failed=1), EXIT_FAILED),
        # 재실행해 봐야 같은 응답을 받으므로 남은 단위는 내일 회차로 넘긴다.
        (summary(daily_limit=1, failed=3), EXIT_OK),
        # 2차 패스까지 실패한 정제는 다시 띄워도 결과가 같다.
        (summary(silver_failed=2), EXIT_OK),
    ],
)
def test_decide_exit_code(result: dict[str, int], expected: int) -> None:
    assert decide_exit_code(result) == expected


@pytest.mark.parametrize("value", ["1", "12", "24"])
def test_bounded_int_accepts_range(value: str) -> None:
    assert bounded_int(1, 24)(value) == int(value)


@pytest.mark.parametrize("value", ["0", "25"])
def test_bounded_int_rejects_out_of_range(value: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError, match="1~24 범위"):
        bounded_int(1, 24)(value)


def test_bounded_int_rejects_non_integer() -> None:
    with pytest.raises(ValueError, match="invalid literal"):
        bounded_int(1, 24)("두 달")


@pytest.fixture
def run_main(monkeypatch: pytest.MonkeyPatch) -> RunMain:
    """락 획득 결과만 바꿔 `main()`을 돌리고, 종료 코드와 refresh_all 호출 인자를 함께 돌려준다."""
    monkeypatch.setattr(sys, "argv", ["prop-trend"])
    # 실제 설정은 루트 로거를 갈아끼우고 로그 파일까지 만든다.
    monkeypatch.setattr(main_module, "configure_logging", lambda *args, **kwargs: None)

    async def run(lock: Callable[[], AbstractAsyncContextManager[bool]]) -> tuple[int, list[dict[str, int]]]:
        calls: list[dict[str, int]] = []

        async def fake_refresh_all(months: int, concurrency: int) -> dict[str, int]:
            calls.append({"months": months, "concurrency": concurrency})
            return summary()

        monkeypatch.setattr(main_module, "advisory_lock", lock)
        monkeypatch.setattr(main_module, "refresh_all", fake_refresh_all)
        return await main_module.main(), calls

    return run


def lock_yielding(acquired: bool) -> Callable[[], AbstractAsyncContextManager[bool]]:
    """획득 여부만 정해둔 가짜 advisory_lock."""

    @asynccontextmanager
    async def lock() -> AsyncGenerator[bool]:
        yield acquired

    return lock


async def test_busy_lock_skips_refresh_without_failing(run_main: RunMain) -> None:
    exit_code, calls = await run_main(lock_yielding(acquired=False))

    # 갱신을 건너뛴 채 비0을 내보내면 backoffLimit이 곧바로 전량 재실행을 지시한다.
    assert exit_code == EXIT_OK
    # 락을 못 잡고도 갱신이 돌면 락이 있으나 마나다.
    assert calls == []


async def test_acquired_lock_runs_refresh_with_parsed_args(run_main: RunMain) -> None:
    exit_code, calls = await run_main(lock_yielding(acquired=True))

    assert exit_code == EXIT_OK
    assert calls == [{"months": DEFAULT_MONTHS, "concurrency": DEFAULT_CONCURRENCY}]


async def test_lock_failure_is_logged_and_reported_as_failure(
    run_main: RunMain, caplog: pytest.LogCaptureFixture
) -> None:
    def failing_lock() -> AbstractAsyncContextManager[bool]:
        raise OSError("커넥션 거부")

    with caplog.at_level(logging.ERROR, logger="src.jobs.__main__"):
        exit_code, calls = await run_main(failing_lock)

    assert exit_code == EXIT_FAILED
    assert calls == []
    # try가 락 안쪽으로 들어가면 DB 접속 실패가 logging을 거치지 않고 stderr로만 샌다.
    assert any(record.exc_info for record in caplog.records)
