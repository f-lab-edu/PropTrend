"""정기 갱신 DAG의 구조. 태스크 상태 규칙이 어긋나면 실패가 조용히 묻힌다."""

import json
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

AIRFLOW_DIR = Path(__file__).resolve().parents[1]
DAGS_DIR = AIRFLOW_DIR / "dags"
POOLS_PATH = AIRFLOW_DIR / "pools.json"

API_TASK_IDS = [
    "refresh_apart_sale",
    "refresh_apart_rent",
    "refresh_officetel_sale",
    "refresh_officetel_rent",
    "refresh_multiflex_sale",
    "refresh_multiflex_rent",
    "refresh_single_multi_family_sale",
    "refresh_single_multi_family_rent",
]


def load_dagbag() -> DagBag:
    return DagBag(dag_folder=DAGS_DIR)


@pytest.fixture(scope="module")
def dag() -> DAG:
    dagbag = load_dagbag()
    assert dagbag.import_errors == {}
    return dagbag.dags["rtms_daily"]


def docker_task(dag: DAG, task_id: str) -> DockerOperator:
    task = dag.get_task(task_id)
    assert isinstance(task, DockerOperator)
    return task


def test_dag_has_one_task_per_api(dag: DAG) -> None:
    refresh_ids = [
        task_id for task_id in dag.task_ids if task_id.startswith("refresh_") and task_id != "refresh_legal_dong"
    ]

    # 한도·실패·재시도를 API 경계에서 가르려면 API 하나가 태스크 하나여야 한다.
    assert sorted(refresh_ids) == sorted(API_TASK_IDS)


def test_api_tasks_run_one_api_each(dag: DAG) -> None:
    for task_id in API_TASK_IDS:
        api_id = task_id.removeprefix("refresh_")
        command = docker_task(dag, task_id).command

        # 문자열 하나로 주면 셸 분해 규칙을 타서 인자 경계가 흐려진다.
        assert isinstance(command, list)
        assert command[:3] == ["python", "-m", "src.jobs"]
        assert command[command.index("--api-id") + 1] == api_id
        # summarize가 XCom으로 받는 요약은 이 플래그가 있어야 찍힌다.
        assert "--summary-json" in command


def test_api_tasks_queue_on_their_own_pool(dag: DAG) -> None:
    pools = json.loads(POOLS_PATH.read_text())

    for task_id in API_TASK_IDS:
        pool = docker_task(dag, task_id).pool
        # 없는 pool을 가리키는 태스크는 스케줄되지 않고 조용히 기다리기만 한다.
        assert pool in pools
        # 같은 API를 정기 갱신과 백필이 동시에 쓰지 않으려면 slot이 1이어야 한다.
        assert pools[pool]["slots"] == 1
    assert len(pools) == len(API_TASK_IDS)


def test_api_tasks_run_even_if_legal_dong_fails(dag: DAG) -> None:
    for task_id in API_TASK_IDS:
        task = docker_task(dag, task_id)

        assert task.upstream_task_ids == {"refresh_legal_dong"}
        # 법정동코드 교체는 원자적이라 실패해도 어제 목록으로 갱신할 수 있다.
        assert task.trigger_rule == "all_done"


def test_legal_dong_task_only_replaces_codes(dag: DAG) -> None:
    assert docker_task(dag, "refresh_legal_dong").command == ["python", "-m", "src.jobs", "--legal-dong-only"]


def test_every_container_maps_exit_99_to_skipped(dag: DAG) -> None:
    for task_id in ["refresh_legal_dong", *API_TASK_IDS]:
        # 한도 소진·락 대기를 실패로 두면 재시도가 같은 응답에 호출량만 쓴다.
        assert 99 in docker_task(dag, task_id).skip_on_exit_code


def test_containers_are_force_removed(dag: DAG) -> None:
    for task_id in ["refresh_legal_dong", *API_TASK_IDS]:
        # "success"면 타임아웃 뒤에도 컨테이너가 끝까지 돌아 execution_timeout이 일을 멈추지 못한다.
        assert docker_task(dag, task_id).auto_remove == "force"


def test_secrets_stay_out_of_rendered_environment(dag: DAG) -> None:
    for task_id in ["refresh_legal_dong", *API_TASK_IDS]:
        task = docker_task(dag, task_id)

        # environment는 템플릿으로 렌더링돼 UI에 보인다. 비밀값은 private_environment에만 있어야 한다.
        assert {"DATABASE_URL", "DATA_GO_KR_SERVICE_KEY"}.isdisjoint(task.environment)
        assert {"DATABASE_URL", "DATA_GO_KR_SERVICE_KEY"} <= set(task._private_environment)


def test_containers_do_not_share_environment(dag: DAG) -> None:
    environments = [docker_task(dag, task_id).environment for task_id in ["refresh_legal_dong", *API_TASK_IDS]]

    # 연산자가 실행 중에 environment를 고치므로 한 dict를 나눠 쓰면 서로의 값이 섞인다.
    assert len({id(environment) for environment in environments}) == len(environments)


def test_summarize_waits_for_every_api_task(dag: DAG) -> None:
    summarize = dag.get_task("summarize")

    assert summarize.upstream_task_ids == set(API_TASK_IDS)
    assert summarize.trigger_rule == "all_done"


def test_summarize_decodes_each_api_summary(dag: DAG) -> None:
    class FakeTaskInstance:
        def xcom_pull(self, task_ids: str) -> str | None:
            # 성공한 태스크만 stdout 마지막 줄이 XCom으로 남는다.
            return '{"units": 500, "failed": 0}' if task_ids == "refresh_apart_sale" else None

    summarize = dag.get_task("summarize")
    assert isinstance(summarize, PythonOperator)

    result = summarize.python_callable(ti=FakeTaskInstance())

    assert result["apart_sale"] == {"units": 500, "failed": 0}
    # 실패·건너뜀인 API도 키는 남아야 "안 돈 것"이 보인다.
    assert result.keys() == {task_id.removeprefix("refresh_") for task_id in API_TASK_IDS}
    assert result["apart_rent"] is None


def test_watcher_fails_the_run_when_any_refresh_fails(dag: DAG) -> None:
    watcher = dag.get_task("watcher")

    # summarize가 all_done으로 성공하면 상류 실패가 DAG 실행 상태에서 사라진다. watcher가 그것을 드러낸다.
    assert watcher.upstream_task_ids == {"refresh_legal_dong", *API_TASK_IDS}
    assert watcher.trigger_rule == "one_failed"
    assert watcher.retries == 0


def test_missing_secret_is_an_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATA_GO_KR_SERVICE_KEY")

    # 빈 값으로 컨테이너가 뜨면 첫 호출에서야 실패한다. 파싱에서 막혀야 UI에 바로 보인다.
    assert load_dagbag().import_errors
