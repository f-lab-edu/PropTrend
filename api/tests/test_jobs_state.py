"""갱신 단위 상태 표의 갱신 규칙과 재시도 목록 질의."""

from typing import Any

import pytest
from conftest import FakeSession
from sqlalchemy.dialects import postgresql

from src.jobs.state import MAX_ATTEMPTS, RefreshUnitStateStore, UnitRecord, summarize_error
from src.model import LAST_ERROR_MAX, UnitStatus


def compiled(session: FakeSession) -> str:
    """세션에 마지막으로 실행된 문장을 PostgreSQL 문법으로 펼친다."""
    statement, _ = session.statements[-1]
    return str(statement.compile(dialect=postgresql.dialect()))


def rows(*records: tuple[str, str, str, str, int]) -> list[Any]:
    return list(records)


async def test_mark_failed_increments_attempts() -> None:
    session = FakeSession()

    await RefreshUnitStateStore(session).mark_failed(
        "apart_sale", "11110", "202602", UnitStatus.FAILED, ValueError("터짐")
    )

    sql = compiled(session)
    assert "ON CONFLICT" in sql
    # 상한이 걸리려면 재시도마다 누적돼야 한다. 덮어쓰면 영영 1에 머문다.
    assert "attempts = (refresh_unit_states.attempts +" in sql


async def test_mark_collected_does_not_reset_attempts() -> None:
    session = FakeSession()

    await RefreshUnitStateStore(session).mark_collected("apart_sale", "11110", "202602")

    update_clause = compiled(session).split("DO UPDATE SET")[1]
    # 여기서 attempts를 건드리면 --with-collect 재실행의 T1이 매번 카운터를 되돌린다.
    assert "attempts" not in update_clause
    assert "status" in update_clause


async def test_pending_excludes_units_over_the_attempt_cap() -> None:
    session = FakeSession()

    await RefreshUnitStateStore(session).pending((UnitStatus.COLLECTED,))

    sql = compiled(session)
    assert "attempts <" in sql
    assert "status IN" in sql


async def test_leftovers_counts_everything_including_stuck_rows() -> None:
    session = FakeSession()

    await RefreshUnitStateStore(session).leftovers()

    # 남은 단위 수는 상한을 넘긴 행까지 세야 "봐야 한다"는 신호가 된다.
    assert "attempts <" not in compiled(session)


async def test_pending_returns_newest_month_first() -> None:
    session = FakeSession(rows=rows(("apart_sale", "11110", "202602", UnitStatus.COLLECTED, 1)))

    records = await RefreshUnitStateStore(session).pending((UnitStatus.COLLECTED,))

    assert records == [UnitRecord("apart_sale", "11110", "202602", UnitStatus.COLLECTED, 1)]
    assert "ORDER BY refresh_unit_states.deal_ymd DESC" in compiled(session)


async def test_clear_deletes_only_the_one_unit() -> None:
    session = FakeSession()

    await RefreshUnitStateStore(session).clear("apart_sale", "11110", "202602")

    sql = compiled(session)
    assert sql.startswith("DELETE FROM refresh_unit_states")
    for column in ("api_id", "lawd_cd", "deal_ymd"):
        assert f"refresh_unit_states.{column} =" in sql


def test_unit_record_coordinate_matches_the_cli_format() -> None:
    record = UnitRecord("apart_sale", "11110", "202602", UnitStatus.COLLECTED, 1)

    # 경고 로그에 실린 좌표를 --only에 그대로 복사해 넣을 수 있어야 한다.
    assert record.coordinate() == "apart_sale:11110:202602"


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ValueError("숫자가 아니다"), "ValueError: 숫자가 아니다"),
        (RuntimeError(""), "RuntimeError: "),
    ],
)
def test_summarize_error_names_the_type(error: Exception, expected: str) -> None:
    assert summarize_error(error) == expected


def test_summarize_error_is_truncated() -> None:
    summary = summarize_error(ValueError("긴" * 2000))

    # 컬럼 길이를 넘기면 적재가 터진다. 인증키가 통째로 실리는 일도 함께 줄인다.
    assert len(summary) == LAST_ERROR_MAX


def test_attempt_cap_leaves_room_for_several_runs() -> None:
    # 한 회차가 한 단위에 최대 2회(본 패스 + 2차 패스)를 쓴다.
    assert MAX_ATTEMPTS >= 2
