"""정기 갱신 DAG의 구조. 태스크 상태 규칙이 어긋나면 실패가 조용히 묻힌다."""

import json
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowSkipException
from rtms_daily import LEGAL_DONG_ROWS_PER_PAGE, fetch_legal_dong_rows

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


def test_legal_dong_runs_natively_with_timeout(dag: DAG) -> None:
    task = dag.get_task("refresh_legal_dong")

    # 컨테이너 없이 태스크가 직접 오픈API와 DB를 다룬다.
    assert isinstance(task, PythonOperator)
    assert task.execution_timeout == timedelta(minutes=10)


def test_every_container_maps_exit_99_to_skipped(dag: DAG) -> None:
    for task_id in API_TASK_IDS:
        # 한도 소진·락 대기를 실패로 두면 재시도가 같은 응답에 호출량만 쓴다.
        assert 99 in docker_task(dag, task_id).skip_on_exit_code


def test_containers_are_force_removed(dag: DAG) -> None:
    for task_id in API_TASK_IDS:
        # "success"면 타임아웃 뒤에도 컨테이너가 끝까지 돌아 execution_timeout이 일을 멈추지 못한다.
        assert docker_task(dag, task_id).auto_remove == "force"


def test_secrets_stay_out_of_rendered_environment(dag: DAG) -> None:
    for task_id in API_TASK_IDS:
        task = docker_task(dag, task_id)

        # environment는 템플릿으로 렌더링돼 UI에 보인다. 비밀값은 private_environment에만 있어야 한다.
        assert {"DATABASE_URL", "DATA_GO_KR_SERVICE_KEY"}.isdisjoint(task.environment)
        assert {"DATABASE_URL", "DATA_GO_KR_SERVICE_KEY"} <= set(task._private_environment)


def test_containers_do_not_share_environment(dag: DAG) -> None:
    environments = [docker_task(dag, task_id).environment for task_id in API_TASK_IDS]

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


def legal_dong_row(region_cd: str) -> str:
    """법정동코드 응답의 row 하나. 코드 자릿수에서 시도·시군구·읍면동·리를 나눈다."""
    return (
        f"<row><region_cd>{region_cd}</region_cd><sido_cd>{region_cd[:2]}</sido_cd>"
        f"<sgg_cd>{region_cd[2:5]}</sgg_cd><umd_cd>{region_cd[5:8]}</umd_cd><ri_cd>{region_cd[8:]}</ri_cd>"
        "<locat_rm/></row>"
    )


def legal_dong_page(total_count: int, *region_cds: str, result_code: str = "INFO-0") -> str:
    return (
        f"<StanReginCd><head><totalCount>{total_count}</totalCount>"
        f"<RESULT><resultCode>{result_code}</resultCode><resultMsg>NORMAL SERVICE</resultMsg></RESULT></head>"
        f"{''.join(legal_dong_row(code) for code in region_cds)}</StanReginCd>"
    )


def mock_client(*responses: httpx.Response) -> tuple[httpx.Client, list[httpx.Request]]:
    """요청을 순서대로 받아 준비한 응답을 돌려주는 클라이언트."""
    requests: list[httpx.Request] = []
    pending = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return pending.pop(0)

    return httpx.Client(transport=httpx.MockTransport(handler)), requests


def test_legal_dong_keeps_only_sigungu_rows_across_pages() -> None:
    client, requests = mock_client(
        # 시도(11), 시군구(11110), 읍면동(11110101)이 섞여 온다.
        httpx.Response(200, text=legal_dong_page(LEGAL_DONG_ROWS_PER_PAGE + 1, "1100000000", "1111000000")),
        httpx.Response(200, text=legal_dong_page(LEGAL_DONG_ROWS_PER_PAGE + 1, "1111010100", "1114000000")),
    )

    rows = fetch_legal_dong_rows(client, "test-key")

    assert [row["region_cd"] for row in rows] == ["1111000000", "1114000000"]
    assert [request.url.params["pageNo"] for request in requests] == ["1", "2"]
    # 빈 태그는 None으로 담겨야 기존 bronze payload와 모양이 같다.
    assert rows[0]["locat_rm"] is None


def test_legal_dong_without_sigungu_rows_is_an_error() -> None:
    client, _ = mock_client(httpx.Response(200, text=legal_dong_page(0, result_code="INFO-200")))

    # 빈 목록으로 표를 갈아끼우면 실거래 갱신이 시군구 목록을 잃는다.
    with pytest.raises(RuntimeError, match="시군구 행이 없다"):
        fetch_legal_dong_rows(client, "test-key")


def test_legal_dong_error_result_code_is_an_error() -> None:
    client, _ = mock_client(
        httpx.Response(
            200, text="<StanReginCd><resultCode>ERROR-290</resultCode><resultMsg>KEY</resultMsg></StanReginCd>"
        )
    )

    with pytest.raises(RuntimeError, match="ERROR-290"):
        fetch_legal_dong_rows(client, "test-key")


def test_legal_dong_daily_limit_is_skipped() -> None:
    gateway_body = (
        "<OpenAPI_ServiceResponse><cmmMsgHeader>"
        "<returnReasonCode>22</returnReasonCode>"
        "</cmmMsgHeader></OpenAPI_ServiceResponse>"
    )
    client, _ = mock_client(httpx.Response(429, text=gateway_body))

    # 재시도해도 같은 응답이라 실패가 아니라 건너뜀이어야 한다.
    with pytest.raises(AirflowSkipException):
        fetch_legal_dong_rows(client, "test-key")


def test_legal_dong_http_error_hides_service_key() -> None:
    client, _ = mock_client(httpx.Response(500, text="Internal Server Error"))

    with pytest.raises(RuntimeError) as error:
        fetch_legal_dong_rows(client, "secret-service-key")

    # 예외 메시지는 태스크 로그에 그대로 찍힌다.
    assert "secret-service-key" not in str(error.value)
