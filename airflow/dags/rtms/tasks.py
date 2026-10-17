"""정기 갱신 DAG가 그래프로 엮는 태스크. 훅으로 연결을 열고 갱신 루프를 부른다."""

import json
import logging
from contextlib import closing
from datetime import timedelta
from typing import Any, cast

from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import TriggerRule, task
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter

from rtms.apis import LEGAL_DONG_CODE_KNOWN_FIELDS
from rtms.db import PROP_TREND_DB_CONN_ID, REGION_INSERT_SQL
from rtms.load import check_fields, collect_units, process_units, rtms_units
from rtms.open_api import DATA_GO_KR_CONN_ID, OPEN_API_RETRIES, fetch_legal_dong_rows, get_service_key
from rtms.preprocess import preprocess_regions

log = logging.getLogger(__name__)

# 수집 묶음이 받은 응답 필드를 남기는 XCom 키. 반환값과 달리 건너뜀·실패로 끝난 묶음도 남긴다.
FIELDS_XCOM_KEY = "fields"


@task(execution_timeout=timedelta(minutes=10))
def refresh_legal_dong() -> dict[str, int]:
    """법정동코드 bronze 표를 오픈API 응답으로 통째로 갈아끼운다."""
    service_key = get_service_key()
    # 응답을 기다리는 동안 트랜잭션을 열어 두지 않도록 DB 작업 전에 다 받는다.
    hook = HttpHook(
        method="GET",
        http_conn_id=DATA_GO_KR_CONN_ID,
        adapter=TCPKeepAliveAdapter(max_retries=OPEN_API_RETRIES),
    )
    with hook.get_conn() as session:
        items = fetch_legal_dong_rows(session, hook.base_url, service_key)

    # 적재는 막지 않고 경고만 남긴다. 응답은 값이 비어도 태그는 보내므로, 전체에서 한 번도 안 보이면 개명·삭제다.
    seen = {key for item in items for key in item}
    if unknown := seen - LEGAL_DONG_CODE_KNOWN_FIELDS:
        log.warning("법정동코드: 기준선에 없는 필드가 왔다 %s", sorted(unknown))
    if missing := LEGAL_DONG_CODE_KNOWN_FIELDS - seen:
        log.warning("법정동코드: 기준선에 있던 필드가 응답에 없다 %s", sorted(missing))

    hook = PostgresHook(postgres_conn_id=PROP_TREND_DB_CONN_ID)

    with closing(hook.get_conn()) as conn:
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
@task(trigger_rule=TriggerRule.ALL_DONE, execution_timeout=timedelta(minutes=5))
def plan_rtms_chunks(**context: Any) -> list[dict[str, Any]]:
    """이번 회차의 갱신 단위를 (계약년월, 시도) 묶음으로 나눈다."""
    # 재시도·clear·실패 표시의 단위가 묶음이다. 시군구 하나씩 펼치면 회차당 인스턴스가 수천 개가 되고 매핑
    # 상한(max_map_length)도 넘긴다. 한 달·한 시도면 재시도가 다시 부르는 양이 최대 수십 단위다.
    hook = PostgresHook(postgres_conn_id=PROP_TREND_DB_CONN_ID)
    chunks: dict[tuple[str, str], list[str]] = {}
    # rtms_units가 최신 월부터 주므로 묶음도 최신 월이 앞 map_index를 받는다. 스케줄러는 우선순위가 같으면
    # map_index 순으로 띄우므로, 중간에 한도로 끊겨도 최근 데이터가 먼저 반영된다.
    for lawd_cd, deal_ymd in rtms_units(hook, context):
        chunks.setdefault((deal_ymd, lawd_cd[:2]), []).append(lawd_cd)
    return [{"deal_ymd": deal_ymd, "sido": sido, "lawd_cds": codes} for (deal_ymd, sido), codes in chunks.items()]


@task(
    trigger_rule=TriggerRule.ALL_DONE,
    execution_timeout=timedelta(minutes=10),
    # 그리드에 0, 1, 2 대신 "202610-11"처럼 계약년월-시도로 보여 실패한 묶음을 로그 없이 찾는다.
    map_index_template="{{ task.op_kwargs['chunk']['deal_ymd'] }}-{{ task.op_kwargs['chunk']['sido'] }}",
)
def collect_rtms(api_id: str, chunk: dict[str, Any], **context: Any) -> dict[str, int]:
    """API 하나의 수집 묶음(계약년월 하나 × 시도 하나의 시군구)을 받아 단위마다 bronze를 갈아끼운다."""
    hook = PostgresHook(postgres_conn_id=PROP_TREND_DB_CONN_ID)
    units = [(lawd_cd, chunk["deal_ymd"]) for lawd_cd in chunk["lawd_cds"]]
    seen: set[str] = set()
    try:
        # 반환 타입(CompatConnection)이 psycopg2·3 공통 메서드 중 rollback을 빠뜨리고 있다.
        with closing(cast("Any", hook.get_conn())) as conn:
            return collect_units(conn, api_id, units, seen)
    finally:
        # 필드 대조는 회차 전체로 한 번 한다. 묶음마다 대조하면 개명 하나에 같은 실패가 묶음 수만큼 난다.
        # 반환값은 성공한 묶음만 남으므로, 한도로 건너뛰거나 실패한 묶음이 받은 응답도 넣으려고 따로 남긴다.
        context["ti"].xcom_push(key=FIELDS_XCOM_KEY, value=sorted(seen))


# 묶음이 실패·건너뜀이어도 돈다. 받은 응답만으로도 필드를 대조해야 한도에 걸린 날 개명이 묻히지 않는다.
@task(trigger_rule=TriggerRule.ALL_DONE, execution_timeout=timedelta(minutes=5))
def check_rtms(api_id: str, **context: Any) -> dict[str, int]:
    """API 하나의 수집 묶음 결과를 모으고, 회차에서 받은 응답 필드를 기준선과 대조한다."""
    ti = context["ti"]
    # 묶음 목록을 인자로 받으면 묶음 나누기가 실패했을 때 인자 해석 오류가 하나 더 붙는다. 그 실패는 watcher가 드러낸다.
    chunks = ti.xcom_pull(task_ids="plan_rtms_chunks") or []
    collect_task_id = f"collect_{api_id}"
    # map_indexes를 주지 않으면 남은 값이 하나뿐일 때 목록이 아니라 그 값만 돌아와, 필드 목록과 구분되지 않는다.
    fields = ti.xcom_pull(task_ids=collect_task_id, key=FIELDS_XCOM_KEY, map_indexes=range(len(chunks)))
    check_fields(api_id, {field for chunk_fields in fields if chunk_fields for field in chunk_fields})

    # 반환값은 성공한 묶음에만 있다. 묶음마다 꺼내면 없는 묶음마다 오류 로그가 남으므로 한 번에 받는다.
    # 남은 값이 하나뿐이면 목록이 아니라 그 dict만, 하나도 없으면 None이 돌아온다.
    pulled = ti.xcom_pull(task_ids=collect_task_id)
    summaries = [pulled] if isinstance(pulled, dict) else [summary for summary in pulled or [] if summary]
    summary = {
        "chunks": len(chunks),
        "succeeded_chunks": len(summaries),
        "units": sum(summary["units"] for summary in summaries),
        "loaded": sum(summary["loaded"] for summary in summaries),
    }
    log.info("%s: bronze 묶음 집계 %s", api_id, summary)
    return summary


# bronze 수집이 실패·건너뜀이어도 돈다. 정제 테이블은 bronze만의 함수라, 이번에 못 받은 단위는 어제 응답으로
# 다시 만들어 같은 결과가 나오고 받은 단위만 새로 반영된다.
@task(trigger_rule=TriggerRule.ALL_DONE, execution_timeout=timedelta(minutes=30))
def process_rtms(api_id: str, **context: Any) -> dict[str, int]:
    """bronze에 남은 응답으로 API 하나의 최근 계약년월 × 전 시군구 정제 테이블을 갈아끼운다."""
    hook = PostgresHook(postgres_conn_id=PROP_TREND_DB_CONN_ID)
    units = rtms_units(hook, context)
    with closing(cast("Any", hook.get_conn())) as conn:
        return process_units(conn, api_id, units)


# 법정동코드 수집이 실패·건너뜀이어도 남아 있는 bronze로 다시 만든다. 지난번 지역 가공이 실패했다면 여기서 복구된다.
@task(trigger_rule=TriggerRule.ALL_DONE, execution_timeout=timedelta(minutes=10))
def process_regions() -> dict[str, int]:
    """법정동코드 bronze로 지역 목록(regions)을 통째로 갈아끼운다."""
    hook = PostgresHook(postgres_conn_id=PROP_TREND_DB_CONN_ID)
    with closing(hook.get_conn()) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.execute("SELECT payload FROM legal_dong_code_raw_items")
            rows = preprocess_regions([payload for (payload,) in cursor.fetchall()])
            # 전처리에서 전부 걸러진 채 확정되면 지역 필터가 통째로 사라진다.
            if not rows:
                raise RuntimeError("적재할 지역 행이 없다")
            # 비우기와 채우기는 한 트랜잭션이어야 한다. 사이에서 끊기면 지역 필터가 사라진다.
            cursor.execute("DELETE FROM regions")
            deleted = cursor.rowcount
            cursor.executemany(REGION_INSERT_SQL, rows)
        conn.commit()

    log.info("지역 %d건 교체(삭제 %d건)", len(rows), deleted)
    return {"deleted": deleted, "loaded": len(rows)}
