"""실거래 정기 갱신. 법정동코드와 실거래 8종의 오픈API 응답을 bronze 표에 갈아끼우고 정제 테이블로 옮긴다."""

import json
import logging
from contextlib import closing
from datetime import timedelta
from typing import Any, cast

import httpx
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook

# [Airflow] TriggerRule은 상류 상태에 따른 실행 조건, dag·task는 TaskFlow 데코레이터다.
from airflow.sdk import TriggerRule, dag, task

# [Airflow] DAG 폴더는 sys.path에 올라가므로 그 아래 패키지를 그냥 import한다. rtms/는 DAG를 정의하지 않아
# .airflowignore로 DAG 파싱 대상에서 뺐다.
from rtms.open_api import API_IDS, OPEN_API_TIMEOUT, fetch_legal_dong_rows, get_service_key
from rtms.preprocess import preprocess_regions
from rtms.refresh import (
    LEGAL_DONG_SCOPE,
    PROP_TREND_DB_CONN_ID,
    collect_units,
    process_units,
    sigungu_codes,
    try_lock,
)

log = logging.getLogger(__name__)

# 신고 기한 30일과 뒤늦은 해제·정정 때문에 지난 달 데이터도 계속 바뀐다.
RTMS_MONTHS = 2

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


def rtms_units(hook: PostgresHook, context: dict[str, Any]) -> list[tuple[str, str]]:
    """이번 회차의 갱신 단위(시군구, 계약년월)를 최신 월부터 만든다."""
    # "이번 달"은 실행 시각이 아니라 회차 기준 시각(KST)으로 센다. 재시도해도, bronze와 silver가 따로 돌아도
    # 같은 달을 가리킨다.
    # [Airflow] context["dag_run"]은 지금 도는 DAG 실행(DagRun)이다. run_after는 그 실행이 돌기로 예정된 시각으로,
    # 스케줄 실행이면 cron 시각(매일 04:00), 수동 실행이면 트리거한 시각이다. 재시도해도 바뀌지 않는다.
    run_after = pendulum.instance(context["dag_run"].run_after).in_timezone("Asia/Seoul")
    months = [run_after.subtract(months=offset).format("YYYYMM") for offset in range(RTMS_MONTHS)]
    sigungu = sigungu_codes(hook)
    # 최신 월부터 돈다. 중간에 한도로 끊겨도 최근 데이터가 먼저 반영된다.
    return [(lawd_cd, deal_ymd) for deal_ymd in months for lawd_cd in sigungu]


@task(execution_timeout=timedelta(minutes=10))
def refresh_legal_dong() -> dict[str, int]:
    """법정동코드 bronze 표를 오픈API 응답으로 통째로 갈아끼운다."""
    service_key = get_service_key()
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

    with closing(hook.get_conn()) as conn:
        try_lock(conn, LEGAL_DONG_SCOPE)
        # 비우기와 채우기는 한 트랜잭션이어야 한다. 커밋 전에 예외가 나면 닫히면서 롤백돼 어제 목록이 남는다.
        with closing(conn.cursor()) as cursor:
            cursor.execute("DELETE FROM legal_dong_code_raw_items")
            deleted = cursor.rowcount
            cursor.executemany(
                "INSERT INTO legal_dong_code_raw_items (payload) VALUES (%s::jsonb)",
                [(json.dumps(item, ensure_ascii=False),) for item in items],
            )
        conn.commit()

    log.info("법정동코드 bronze %d건 교체(삭제 %d건)", len(items), deleted)

    return {"deleted": deleted, "loaded": len(items)}


# 법정동코드 교체는 한 트랜잭션이라 실패해도 어제 목록이 남는다. 실거래 갱신을 막을 이유가 없다.
@task(trigger_rule=TriggerRule.ALL_DONE, execution_timeout=timedelta(hours=1))
# api_id처럼 DAG에서 넘긴 일반 인자는 앞쪽 위치 인자로 그대로 들어온다.
def collect_rtms(api_id: str, **context: Any) -> dict[str, int]:
    """API 하나의 최근 계약년월 × 전 시군구를 받아 단위마다 bronze를 갈아끼운다."""
    hook = PostgresHook(postgres_conn_id=PROP_TREND_DB_CONN_ID)
    units = rtms_units(hook, context)
    # 반환 타입(CompatConnection)이 psycopg2·3 공통 메서드 중 rollback을 빠뜨리고 있다.
    with closing(cast("Any", hook.get_conn())) as conn:
        try_lock(conn, api_id)
        return collect_units(conn, api_id, units)


# bronze 수집이 실패·건너뜀이어도 돈다. 정제 테이블은 bronze만의 함수라, 이번에 못 받은 단위는 어제 응답으로
# 다시 만들어 같은 결과가 나오고 받은 단위만 새로 반영된다.
@task(trigger_rule=TriggerRule.ALL_DONE, execution_timeout=timedelta(minutes=30))
def process_rtms(api_id: str, **context: Any) -> dict[str, int]:
    """bronze에 남은 응답으로 API 하나의 최근 계약년월 × 전 시군구 정제 테이블을 갈아끼운다."""
    hook = PostgresHook(postgres_conn_id=PROP_TREND_DB_CONN_ID)
    units = rtms_units(hook, context)
    with closing(cast("Any", hook.get_conn())) as conn:
        try_lock(conn, api_id)
        return process_units(conn, api_id, units)


# 법정동코드 수집이 실패·건너뜀이어도 남아 있는 bronze로 다시 만든다. 지난번 지역 가공이 실패했다면 여기서 복구된다.
@task(trigger_rule=TriggerRule.ALL_DONE, execution_timeout=timedelta(minutes=10))
def process_regions() -> dict[str, int]:
    """법정동코드 bronze로 지역 목록(regions)을 통째로 갈아끼운다."""
    hook = PostgresHook(postgres_conn_id=PROP_TREND_DB_CONN_ID)
    with closing(hook.get_conn()) as conn:
        try_lock(conn, LEGAL_DONG_SCOPE)
        with closing(conn.cursor()) as cursor:
            cursor.execute("SELECT payload FROM legal_dong_code_raw_items")
            rows = preprocess_regions([payload for (payload,) in cursor.fetchall()])
            # 전처리에서 전부 걸러진 채 확정되면 지역 필터가 통째로 사라진다.
            if not rows:
                raise RuntimeError("적재할 지역 행이 없다")
            # 비우기와 채우기는 한 트랜잭션이어야 한다. 사이에서 끊기면 지역 필터가 사라진다.
            cursor.execute("DELETE FROM regions")
            deleted = cursor.rowcount
            cursor.executemany(
                "INSERT INTO regions (sido_code, sigungu_code, sido_name, sigungu_name) "
                "VALUES (%(sido_code)s, %(sigungu_code)s, %(sido_name)s, %(sigungu_name)s)",
                rows,
            )
        conn.commit()

    log.info("지역 %d건 교체(삭제 %d건)", len(rows), deleted)
    return {"deleted": deleted, "loaded": len(rows)}


@dag(
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["rtms"],
)
def rtms_daily() -> None:
    """법정동코드와 지역 목록을 교체하고, API 8종을 따로 bronze에 받아 정제 테이블로 옮긴다."""
    legal_dong = refresh_legal_dong()
    regions = process_regions()

    legal_dong >> regions

    collects = []
    processes = []
    for api_id in API_IDS:
        # 재실행·백필 DAG와 같은 pool을 써서 같은 API를 동시에 쓰지 않고 줄을 선다. 한도가 API마다 걸리기 때문이다.
        collect = collect_rtms.override(task_id=f"collect_{api_id}", pool=f"rtms_{api_id}")(api_id)
        # 정제는 오픈API를 부르지 않으므로 pool을 쓰지 않는다. 같은 API의 bronze가 다 받아진 뒤에 돈다.
        process = process_rtms.override(task_id=f"process_{api_id}")(api_id)
        legal_dong >> collect >> process
        collects.append(collect)
        processes.append(process)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize(**context: Any) -> dict[str, dict[str, dict[str, int] | None]]:
        """API별 bronze·silver 요약을 모아 한 번에 남긴다."""
        ti = context["ti"]
        # 실패·건너뜀이면 XCom이 없다. 사유는 그리드의 태스크 상태가 말한다.
        return {
            api_id: {
                "bronze": ti.xcom_pull(task_ids=f"collect_{api_id}"),
                "silver": ti.xcom_pull(task_ids=f"process_{api_id}"),
            }
            for api_id in API_IDS
        }

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def watcher() -> None:
        """all_done 리프가 상류 실패를 가리지 않도록 DAG 실행을 실패로 만든다."""
        # DAG 실행 상태는 리프 태스크로 정해진다. summarize가 all_done으로 성공하면 상류 실패가 묻힌다.
        raise RuntimeError("갱신 태스크 중 실패한 것이 있다")

    # [Airflow] 리스트를 >>의 한쪽에 두면 리스트의 모든 태스크와 한꺼번에 의존 관계를 맺는다.
    processes >> summarize()
    [legal_dong, regions, *collects, *processes] >> watcher()


rtms_daily()
