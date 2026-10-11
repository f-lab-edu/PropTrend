"""실거래 정기 갱신. 법정동코드는 태스크가 직접 받고, 실거래는 jobs 이미지를 API별 컨테이너로 띄운다."""

import json
import logging
import os
import zlib
from contextlib import closing
from datetime import timedelta
from typing import Any
from xml.etree.ElementTree import ParseError

import httpx
import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseHook, TriggerRule, dag, task
from airflow.sdk.exceptions import AirflowSkipException
from defusedxml.ElementTree import fromstring as safe_xml_fromstring

log = logging.getLogger(__name__)

# httpx는 INFO에서 요청 URL을 통째로 남기는데, 오픈API 인증키가 쿼리 문자열에 실린다.
# URL 인코딩된 키는 Airflow의 비밀값 마스킹에도 걸리지 않으므로 로그 자체를 내지 않게 한다.
logging.getLogger("httpx").setLevel(logging.WARNING)

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


# docker-compose.yml이 AIRFLOW_CONN_* 환경변수로 만드는 Connection. 메타 DB에는 저장하지 않는다.
PROP_TREND_DB_CONN_ID = "prop_trend_db"
# 인증키를 password에 둬야 Airflow가 태스크 로그에서 가린다.
DATA_GO_KR_CONN_ID = "data_go_kr"

LEGAL_DONG_API_URL = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
LEGAL_DONG_ROWS_PER_PAGE = 1000  # 1회 요청 최대 건수(초과 시 에러코드 336)
LEGAL_DONG_SUCCESS_PREFIX = "INFO"
OPEN_API_TIMEOUT = 10
# 공공데이터포털 게이트웨이의 일일 활용건수 초과 사유 코드.
DAILY_LIMIT_REASON_CODE = "22"

# api/src/model/raw.py의 LEGAL_DONG_CODE_KNOWN_FIELDS와 같아야 한다. 드리프트 경고의 기준선이다.
LEGAL_DONG_CODE_KNOWN_FIELDS = frozenset(
    {
        "region_cd",
        "sido_cd",
        "sgg_cd",
        "umd_cd",
        "ri_cd",
        "locatjumin_cd",
        "locatjijuk_cd",
        "locatadd_nm",
        "locat_order",
        "locat_rm",
        "locathigh_cd",
        "locallow_nm",
        "adpt_de",
    }
)

# api/src/jobs/lock.py의 LOCK_NAMESPACE·scope_key("legal_dong")와 같은 키다. jobs CLI가 남아 있는 동안
# 수동 --legal-dong-only와 겹치면 두 트랜잭션이 서로의 INSERT를 못 보고 지워 행이 두 벌 남는다.
LOCK_NAMESPACE = 0x70726F70
LEGAL_DONG_LOCK_KEY = zlib.crc32(b"legal_dong") & 0x7FFFFFFF


def fetch_legal_dong_rows(client: httpx.Client, service_key: str) -> list[dict[str, str | None]]:
    """법정동코드 API를 끝 페이지까지 받아 시군구 행만 돌려준다."""
    rows: list[dict[str, str | None]] = []
    page_no = 1
    while True:
        params = {
            "ServiceKey": service_key,
            "type": "xml",
            "pageNo": page_no,
            "numOfRows": LEGAL_DONG_ROWS_PER_PAGE,
            "flag": "Y",
        }
        response = client.get(LEGAL_DONG_API_URL, params=params)
        if response.is_error:
            # 일일 한도를 넘기면 게이트웨이가 HTTP 429와 자체 오류 본문을 돌려준다. 초당 호출 제한 같은
            # 다른 429와 가르려고 본문의 사유 코드를 본다. 오류 본문은 형식을 보장하지 않는다.
            try:
                reason = safe_xml_fromstring(response.text).findtext("./cmmMsgHeader/returnReasonCode")
            except ParseError, ValueError:
                reason = None
            if reason == DAILY_LIMIT_REASON_CODE:
                # 재시도해도 같은 응답이다. 실패로 두면 재시도가 호출량만 쓴다.
                raise AirflowSkipException("법정동코드 API 일일 호출 제한")
            # raise_for_status()의 메시지에는 인증키가 실린 요청 URL이 들어가므로 상태 코드만 남긴다.
            raise RuntimeError(f"법정동코드 API HTTP 오류: status={response.status_code}")

        root = safe_xml_fromstring(response.text)
        head = root.find("./head")
        result = head.find("./RESULT") if head is not None else None
        # 에러 응답은 head 없이 최상위에 resultCode/resultMsg만 담겨 온다.
        result_code = (result.findtext("resultCode") if result is not None else root.findtext("./resultCode")) or ""
        result_msg = result.findtext("resultMsg") if result is not None else root.findtext("./resultMsg")
        if not result_code.startswith(LEGAL_DONG_SUCCESS_PREFIX):
            raise RuntimeError(f"법정동코드 API 오류 {result_code}: {result_msg}")

        for element in root.findall("./row"):
            row = {field.tag: (field.text.strip() if field.text else None) for field in element}
            # 시군구 단위(SSGGG00000) 행만 남긴다. 실거래 API의 LAWD_CD가 이 단위다.
            if row.get("sgg_cd") != "000" and row.get("umd_cd") == "000" and row.get("ri_cd") == "00":
                rows.append(row)

        total_count = int(head.findtext("totalCount") or 0) if head is not None else 0
        if page_no * LEGAL_DONG_ROWS_PER_PAGE >= total_count:
            break
        page_no += 1

    if not rows:
        # 이 목록으로 표를 통째로 갈아끼운다. 데이터없음(INFO-200)도 접두사 검사를 통과하므로,
        # 빈 목록을 정상으로 넘기면 표가 비고 이후 모든 실거래 갱신이 시군구 목록을 잃는다.
        raise RuntimeError("법정동코드 응답에 시군구 행이 없다")
    return rows


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

    @task(execution_timeout=timedelta(minutes=10))
    def refresh_legal_dong() -> dict[str, int]:
        """법정동코드 bronze 표를 오픈API 응답으로 통째로 갈아끼운다."""
        # 파싱 시점이 아니라 실행 시점에 읽는다. DAG 파일은 수시로 다시 파싱된다.
        service_key = BaseHook.get_connection(DATA_GO_KR_CONN_ID).password
        if not service_key:
            raise RuntimeError(f"{DATA_GO_KR_CONN_ID} Connection에 인증키(password)가 없다")
        # 응답을 기다리는 동안 트랜잭션과 락을 쥐지 않도록 DB 작업 전에 다 받는다.
        with httpx.Client(timeout=OPEN_API_TIMEOUT) as client:
            items = fetch_legal_dong_rows(client, service_key)

        # 적재는 막지 않고 경고만 남긴다. 응답은 값이 비어도 태그는 보내므로, 전체에서 한 번도 안 보이면 개명·삭제다.
        seen = {key for item in items for key in item}
        if unknown := seen - LEGAL_DONG_CODE_KNOWN_FIELDS:
            log.warning("법정동코드: 기준선에 없는 필드가 왔다 %s", sorted(unknown))
        if missing := LEGAL_DONG_CODE_KNOWN_FIELDS - seen:
            log.warning("법정동코드: 기준선에 있던 필드가 응답에 없다 %s", sorted(missing))

        hook = PostgresHook(postgres_conn_id=PROP_TREND_DB_CONN_ID)
        # 비우기와 채우기는 한 트랜잭션이어야 한다. 커밋 전에 예외가 나면 닫히면서 롤백돼 어제 목록이 남는다.
        with closing(hook.get_conn()) as conn, closing(conn.cursor()) as cursor:
            # 트랜잭션 락이라 커밋·롤백과 함께 풀린다. 기다리지 않는 것은 jobs CLI의 전체 갱신이 이 범위를
            # 회차 내내 쥐고 있어서다. 그쪽이 같은 표를 막 갈아끼우는 중이므로 건너뛰어도 잃는 것이 없다.
            cursor.execute("SELECT pg_try_advisory_xact_lock(%s, %s)", (LOCK_NAMESPACE, LEGAL_DONG_LOCK_KEY))
            acquired = cursor.fetchone()
            if not (acquired and acquired[0]):
                raise AirflowSkipException("다른 실행이 법정동코드를 갱신하고 있다")
            cursor.execute("DELETE FROM legal_dong_code_raw_items")
            deleted = cursor.rowcount
            # collected_at은 server_default에 맡긴다.
            cursor.executemany(
                "INSERT INTO legal_dong_code_raw_items (payload) VALUES (%s::jsonb)",
                [(json.dumps(item, ensure_ascii=False),) for item in items],
            )
            conn.commit()

        log.info("법정동코드 bronze %d건 교체(삭제 %d건)", len(items), deleted)
        return {"deleted": deleted, "loaded": len(items)}

    legal_dong = refresh_legal_dong()

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
