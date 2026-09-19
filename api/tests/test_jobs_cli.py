"""갱신 결과를 종료 코드로 줄이는 판정, 인자 검증, 그리고 락 분기."""

import argparse
import logging
import sys
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

import pytest

from src.jobs import __main__ as main_module
from src.jobs.__main__ import (
    EXIT_FAILED,
    EXIT_OK,
    bounded_int,
    decide_exit_code,
    decide_targeted_exit_code,
    unit_coordinate,
)
from src.jobs.pipeline import DEFAULT_CONCURRENCY, DEFAULT_MONTHS, SPEC_BY_API_ID, PipelineSpec
from src.jobs.state import UnitRecord
from src.model import UnitStatus

# 가짜 advisory_lock을 받아 (종료 코드, 실행기 호출 인자)를 돌려주는 함수.
Lock = Callable[[], AbstractAsyncContextManager[bool]]
RunMain = Callable[..., Coroutine[Any, Any, tuple[int, list[dict[str, Any]]]]]

UNIT = "apart_sale:11110:202602"


def summary(**overrides: int) -> dict[str, int]:
    return {"daily_limit": 0, "failed": 0, "silver_failed": 0, "no_bronze": 0} | overrides


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
    """락과 명령줄만 바꿔 `main()`을 돌리고, 종료 코드와 실행기 호출 인자를 함께 돌려준다."""
    # 실제 설정은 루트 로거를 갈아끼우고 로그 파일까지 만든다.
    monkeypatch.setattr(main_module, "configure_logging", lambda *args, **kwargs: None)

    async def run(
        lock: Lock,
        argv: list[str] | None = None,
        pending: list[tuple[PipelineSpec, str, str]] | None = None,
        result: dict[str, int] | None = None,
        stuck: list[UnitRecord] | None = None,
    ) -> tuple[int, list[dict[str, Any]]]:
        calls: list[dict[str, Any]] = []

        async def fake_refresh_all(months: int, concurrency: int) -> dict[str, int]:
            calls.append({"months": months, "concurrency": concurrency})
            return summary()

        async def fake_refresh_units(
            units: list[tuple[PipelineSpec, str, str]],
            concurrency: int,
            *,
            with_collect: bool,
        ) -> dict[str, int]:
            coordinates = [f"{spec.api_id}:{lawd_cd}:{deal_ymd}" for spec, lawd_cd, deal_ymd in units]
            calls.append({"units": coordinates, "concurrency": concurrency, "with_collect": with_collect})
            return result or summary()

        async def fake_failed_units(*, with_collect: bool) -> list[tuple[PipelineSpec, str, str]]:
            return list(pending or ())

        async def fake_stuck_units() -> list[UnitRecord]:
            return list(stuck or ())

        monkeypatch.setattr(sys, "argv", ["prop-trend", *(argv or ())])
        monkeypatch.setattr(main_module, "advisory_lock", lock)
        monkeypatch.setattr(main_module, "refresh_all", fake_refresh_all)
        monkeypatch.setattr(main_module, "refresh_units", fake_refresh_units)
        monkeypatch.setattr(main_module, "failed_units", fake_failed_units)
        monkeypatch.setattr(main_module, "stuck_units", fake_stuck_units)
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


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (summary(), EXIT_OK),
        (summary(failed=1), EXIT_FAILED),
        # 정기 회차와 갈리는 지점이다. 사람이 결과를 기다리므로 정제 실패를 숨기지 않는다.
        (summary(silver_failed=1), EXIT_FAILED),
        # 좌표가 가리키는 bronze가 없었다는 뜻이라 조용히 성공으로 넘기면 안 된다.
        (summary(no_bronze=1), EXIT_FAILED),
    ],
)
def test_decide_targeted_exit_code(result: dict[str, int], expected: int) -> None:
    assert decide_targeted_exit_code(result) == expected


def test_unit_coordinate_parses_a_valid_triple() -> None:
    assert unit_coordinate(UNIT) == (SPEC_BY_API_ID["apart_sale"], "11110", "202602")


@pytest.mark.parametrize(
    "value",
    [
        "apart_sale:11110",  # 좌표가 모자라다
        "apart_sale:11110:202602:extra",
        "apart_sail:11110:202602",  # 오타 난 슬러그
        "apart_sale:1111:202602",  # 시군구 5자리가 아니다
        "apart_sale:11110:202613",  # 13월
        "apart_sale:11110:2026",
    ],
)
def test_unit_coordinate_rejects_bad_input(value: str) -> None:
    # 여기서 막지 못하면 삭제 범위가 엉뚱한 구간을 가리킨다.
    with pytest.raises(argparse.ArgumentTypeError):
        unit_coordinate(value)


async def test_only_takes_the_targeted_path(run_main: RunMain) -> None:
    exit_code, calls = await run_main(lock_yielding(acquired=True), argv=["--only", UNIT])

    assert exit_code == EXIT_OK
    assert calls == [{"units": [UNIT], "concurrency": DEFAULT_CONCURRENCY, "with_collect": False}]


async def test_only_is_repeatable_and_deduplicated(run_main: RunMain) -> None:
    other = "apart_rent:11140:202601"
    _, calls = await run_main(lock_yielding(acquired=True), argv=["--only", UNIT, "--only", other, "--only", UNIT])

    assert calls[0]["units"] == [UNIT, other]


async def test_with_collect_reaches_the_runner(run_main: RunMain) -> None:
    _, calls = await run_main(lock_yielding(acquired=True), argv=["--only", UNIT, "--with-collect"])

    assert calls[0]["with_collect"] is True


async def test_retry_failed_runs_the_recorded_units(run_main: RunMain) -> None:
    pending = [(SPEC_BY_API_ID["apart_sale"], "11110", "202602")]
    exit_code, calls = await run_main(lock_yielding(acquired=True), argv=["--retry-failed"], pending=pending)

    assert exit_code == EXIT_OK
    assert calls[0]["units"] == [UNIT]


async def test_retry_failed_with_an_empty_worklist_exits_ok(run_main: RunMain) -> None:
    exit_code, calls = await run_main(lock_yielding(acquired=True), argv=["--retry-failed"])

    # 밀린 단위가 없다는 뜻이라, 이 조합이 곧 운영자의 건강 확인이 된다.
    assert (exit_code, calls) == (EXIT_OK, [])


async def test_empty_worklist_still_reports_stuck_units(run_main: RunMain, caplog: pytest.LogCaptureFixture) -> None:
    stuck = [UnitRecord("apart_sale", "11110", "202602", UnitStatus.COLLECTED, 10)]

    with caplog.at_level(logging.WARNING, logger="src.jobs.__main__"):
        exit_code, calls = await run_main(lock_yielding(acquired=True), argv=["--retry-failed"], stuck=stuck)

    # 빈 목록이 "다 끝났다"로 읽히면 상한에 걸린 단위가 영영 방치된다.
    assert (exit_code, calls) == (EXIT_OK, [])
    assert any(record.stuck_units == 1 for record in caplog.records)


async def test_targeted_failure_is_reported(run_main: RunMain) -> None:
    exit_code, _ = await run_main(lock_yielding(acquired=True), argv=["--only", UNIT], result=summary(silver_failed=1))

    assert exit_code == EXIT_FAILED


async def test_busy_lock_in_targeted_mode_is_a_failure(run_main: RunMain) -> None:
    exit_code, calls = await run_main(lock_yielding(acquired=False), argv=["--only", UNIT])

    # 정기 회차와 달리 0을 내면 "다 돌렸다"로 읽힌다.
    assert (exit_code, calls) == (EXIT_FAILED, [])


@pytest.mark.parametrize(
    "argv",
    [
        ["--months", "3", "--only", UNIT],
        ["--months", "3", "--retry-failed"],
        # 지정 없이 주면 아무 효과가 없다. 조용히 무시하지 않는다.
        ["--with-collect"],
    ],
)
async def test_incompatible_arguments_are_rejected(run_main: RunMain, argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        await run_main(lock_yielding(acquired=True), argv=argv)


async def test_default_run_is_unchanged(run_main: RunMain) -> None:
    exit_code, calls = await run_main(lock_yielding(acquired=True))

    # 인자 없는 호출은 예약 실행 그대로여야 한다.
    assert exit_code == EXIT_OK
    assert calls == [{"months": DEFAULT_MONTHS, "concurrency": DEFAULT_CONCURRENCY}]
