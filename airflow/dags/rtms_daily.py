"""실거래 정기 갱신. jobs 이미지를 API별 컨테이너로 띄운다."""

import json
import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import TriggerRule, dag, task

# api/src/jobs/pipeline.py의 PIPELINES와 같아야 한다. DAG는 src를 import하지 않으므로 따로 든다.
# 어긋나면 CLI의 --api-id choices가 거부해 그 태스크가 첫 실행에서 실패한다.
API_IDS = (
    "apart_sale",
    "apart_rent",
    "officetel_sale",
    "officetel_rent",
    "multiflex_sale",
    "multiflex_rent",
    "single_multi_family_sale",
    "single_multi_family_rent",
)

# jobs의 EXIT_SKIPPED. 한도 소진·락 대기는 재시도해도 결과가 같아 skipped로 남긴다.
EXIT_SKIPPED = 99

# 루트 compose의 api 서비스가 이 태그로 이미지를 만든다.
JOBS_IMAGE = "prop-trend-api:latest"
# 도커 소켓은 호스트 root 권한과 같다. 직접 마운트하지 않고 프록시가 연 API만 쓴다.
DOCKER_URL = "tcp://docker-socket-proxy:2375"
# 루트 compose의 기본 네트워크. jobs 컨테이너가 서비스 DB에 서비스 이름으로 닿는다.
JOBS_NETWORK = "prop_trend_default"


def jobs_task(task_id: str, args: list[str], **kwargs: Any) -> DockerOperator:
    """jobs CLI를 컨테이너 하나로 띄우는 태스크를 만든다."""
    return DockerOperator(
        task_id=task_id,
        image=JOBS_IMAGE,
        command=["python", "-m", "src.jobs", *args],
        docker_url=DOCKER_URL,
        network_mode=JOBS_NETWORK,
        # 연산자가 실행 중에 environment를 고친다. 태스크끼리 dict 하나를 나눠 쓰지 않게 매번 새로 만든다.
        environment={"TZ": "Asia/Seoul"},
        # private_environment는 템플릿 처리도 UI 노출도 하지 않는다. 값이 없으면 DAG import 오류로 드러난다.
        private_environment={
            "DATABASE_URL": os.environ["PROP_TREND_DATABASE_URL"],
            "DATA_GO_KR_SERVICE_KEY": os.environ["DATA_GO_KR_SERVICE_KEY"],
        },
        # "success"도 끝나면 지우지만 강제 삭제가 아니다. 타임아웃 때 아직 도는 컨테이너를 지우려다 409가 나서
        # 타임아웃 예외를 덮고, 그러면 on_kill이 불리지 않아 컨테이너가 끝까지 돈다(2026-10-11 검증).
        auto_remove="force",
        # 기본값은 Airflow 쪽 임시 디렉터리를 바인드 마운트하는데, 프록시 너머 도커 호스트에는 그 경로가 없다.
        mount_tmp_dir=False,
        skip_on_exit_code=EXIT_SKIPPED,
        **kwargs,
    )


@dag(
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    # 밀린 회차를 따라잡으면 같은 최근 두 달을 회차 수만큼 다시 받아 한도만 쓴다.
    catchup=False,
    max_active_runs=1,
    # 외부 호출 동시성을 분할 전(프로세스 1개, --concurrency 4)과 같게 둔다. 실측 뒤 올린다.
    max_active_tasks=4,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["rtms"],
)
def rtms_daily() -> None:
    """법정동코드를 교체하고 API 8종을 따로 갱신한다."""
    legal_dong = jobs_task(
        "refresh_legal_dong",
        ["--legal-dong-only"],
        execution_timeout=timedelta(minutes=10),
    )

    refreshes = [
        jobs_task(
            f"refresh_{api_id}",
            ["--api-id", api_id, "--concurrency", "1", "--summary-json"],
            # 백필 DAG와 같은 pool을 써서 같은 API를 동시에 쓰지 않고 줄을 선다. 한도가 API마다 걸리기 때문이다.
            pool=f"rtms_{api_id}",
            # 법정동코드 교체는 한 트랜잭션이라 실패해도 어제 목록이 남는다. 실거래 갱신을 막을 이유가 없다.
            trigger_rule=TriggerRule.ALL_DONE,
            execution_timeout=timedelta(hours=1),
        )
        for api_id in API_IDS
    ]

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize(**context: Any) -> dict[str, dict[str, int] | None]:
        """API별 요약을 모아 한 번에 남긴다."""
        ti = context["ti"]
        # 실패·건너뜀이면 XCom이 없다. 사유는 그리드의 태스크 상태가 말한다.
        return {
            api_id: json.loads(raw) if (raw := ti.xcom_pull(task_ids=f"refresh_{api_id}")) else None
            for api_id in API_IDS
        }

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def watcher() -> None:
        """all_done 리프가 상류 실패를 가리지 않도록 DAG 실행을 실패로 만든다."""
        # DAG 실행 상태는 리프 태스크로 정해진다. summarize가 all_done으로 성공하면 상류 실패가 묻힌다.
        raise RuntimeError("갱신 태스크 중 실패한 것이 있다")

    legal_dong >> refreshes
    refreshes >> summarize()
    [legal_dong, *refreshes] >> watcher()


rtms_daily()
