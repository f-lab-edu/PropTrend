"""지정 재실행 DAG의 구조·입력 검사와, 정제만 돌릴 때 빈 bronze가 정제 테이블을 지우지 않는지."""

from pathlib import Path
from typing import Any

import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowSkipException, ParamValidationError
from rtms.open_api import API_IDS
from rtms.refresh import process_units
from rtms_replay import lock_or_fail, replay_units

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def replay_dag() -> DAG:
    dagbag = DagBag(dag_folder=DAGS_DIR)
    assert dagbag.import_errors == {}
    return dagbag.dags["rtms_replay"]


def replay_task(dag: DAG, task_id: str) -> PythonOperator:
    task = dag.get_task(task_id)
    assert isinstance(task, PythonOperator)
    return task


def test_each_api_collects_on_its_pool_then_processes(replay_dag: DAG) -> None:
    for api_id in API_IDS:
        collect = replay_task(replay_dag, f"collect_{api_id}")
        process = replay_task(replay_dag, f"process_{api_id}")

        assert tuple(collect.op_args) == (api_id,)
        assert tuple(process.op_args) == (api_id,)
        # 정기 갱신과 같은 pool에 줄 서야 같은 API의 한도를 동시에 쓰지 않는다.
        assert collect.pool == f"rtms_{api_id}"
        assert process.pool == "default_pool"
        assert process.upstream_task_ids == {f"collect_{api_id}"}
        # 수집이 꺼져 건너뛰어도 정제는 돌아야 한다.
        assert process.trigger_rule == "all_done"


def test_replay_does_not_retry_on_its_own(replay_dag: DAG) -> None:
    # 사람이 결과를 보고 다시 실행한다.
    assert all(task.retries == 0 for task in replay_dag.tasks)
    assert replay_dag.max_active_runs == 1


def test_watcher_fails_the_run_when_any_task_fails(replay_dag: DAG) -> None:
    watcher = replay_dag.get_task("watcher")

    assert watcher.upstream_task_ids == {f"{stage}_{api_id}" for api_id in API_IDS for stage in ("collect", "process")}
    assert watcher.trigger_rule == "one_failed"


def test_units_param_accepts_coordinates(replay_dag: DAG) -> None:
    units = ["apart_sale:11110:202602", "single_multi_family_rent:36110:202612"]

    assert replay_dag.params.get_param("units").resolve(units) == units
    # 기본은 정제만 돈다.
    assert replay_dag.params["with_collect"] is False


@pytest.mark.parametrize(
    "units",
    [
        [],
        ["apart:11110:202602"],
        ["apart_sale:1111:202602"],
        ["apart_sale:11110:202613"],
        ["apart_sale:11110:2026-02"],
        # 아라비아-인도 숫자. 파이썬 isdigit()은 통과시킨다.
        ["apart_sale:١١١١٠:202602"],
    ],
)
def test_units_param_rejects_bad_coordinates(replay_dag: DAG, units: list[str]) -> None:
    # 형식이 틀리면 실행이 만들어지기 전에 막힌다.
    with pytest.raises(ParamValidationError):
        replay_dag.params.get_param("units").resolve(units)


class FakeHook:
    """법정동코드 bronze에 시군구 두 곳만 있다."""

    def get_records(self, sql: str) -> list[tuple[str]]:
        return [("11110",), ("11140",)]


HOOK: Any = FakeHook()


def test_replay_units_pick_this_api_without_duplicates() -> None:
    coordinates = ["apart_sale:11110:202602", "apart_rent:11110:202602", "apart_sale:11110:202602"]

    assert replay_units(HOOK, "apart_sale", coordinates) == [("11110", "202602")]


def test_replay_units_skip_api_without_coordinates() -> None:
    with pytest.raises(AirflowSkipException):
        replay_units(HOOK, "officetel_sale", ["apart_sale:11110:202602"])


def test_replay_units_reject_unknown_sigungu() -> None:
    # 형식은 맞지만 없는 시군구다. 아무것도 건드리기 전에 멈춰야 한다.
    with pytest.raises(ValueError, match="99999"):
        replay_units(HOOK, "apart_sale", ["apart_sale:11110:202602", "apart_sale:99999:202602"])


class FakeConnection:
    """실행한 SQL과 커밋·롤백을 기록한다. bronze 조회는 늘 0건이다."""

    def __init__(self, *, lock_acquired: bool = True) -> None:
        self.lock_acquired = lock_acquired
        self.statements: list[str] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> Any:
        connection = self

        class Cursor:
            rowcount = 0

            def execute(self, sql: str, params: Any = None) -> None:
                connection.statements.append(sql)

            def executemany(self, sql: str, params: Any) -> None:
                connection.statements.append(sql)

            def fetchall(self) -> list[Any]:
                return []

            def fetchone(self) -> tuple[bool]:
                return (connection.lock_acquired,)

            def close(self) -> None:
                pass

        return Cursor()

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_empty_bronze_keeps_transactions_when_bronze_is_required() -> None:
    conn = FakeConnection()

    with pytest.raises(RuntimeError, match=r"apart_sale:11110:202602"):
        process_units(conn, "apart_sale", [("11110", "202602")], require_bronze=True)

    # 받은 적 없는 단위일 수 있다. 정제 테이블의 거래를 지우면 안 된다.
    assert not any(sql.startswith("DELETE") for sql in conn.statements)
    assert (conn.commits, conn.rollbacks) == (0, 1)


def test_empty_bronze_empties_the_unit_in_regular_runs() -> None:
    conn = FakeConnection()

    summary = process_units(conn, "apart_sale", [("11110", "202602")])

    # 회차 안에서 응답 0건은 거래 없는 단위라 정제 테이블도 비워야 한다.
    assert any(sql.startswith("DELETE FROM sale_transactions") for sql in conn.statements)
    assert summary == {"units": 1, "succeeded": 1, "failed": 0, "loaded": 0, "complexes": 0}


def test_busy_lock_fails_the_replay() -> None:
    # 건너뜀으로 끝나면 DAG 실행이 성공으로 보여 사람이 안 돈 것을 놓친다.
    with pytest.raises(RuntimeError, match="apart_sale"):
        lock_or_fail(FakeConnection(lock_acquired=False), "apart_sale")
