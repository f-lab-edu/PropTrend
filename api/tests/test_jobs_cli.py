"""갱신 결과를 종료 코드로 줄이는 판정과 인자 검증."""

import argparse

import pytest

from src.jobs.__main__ import EXIT_FAILED, EXIT_OK, bounded_int, decide_exit_code


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
