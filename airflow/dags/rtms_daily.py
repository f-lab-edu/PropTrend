"""실거래 정기 갱신. 법정동코드와 실거래 8종의 오픈API 응답을 bronze 표에 갈아끼운다."""

import json
import logging
import time
import zlib
from contextlib import closing
from datetime import timedelta
from typing import Any, cast
from xml.etree.ElementTree import ParseError

import httpx
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseHook, TriggerRule, dag, task
from airflow.sdk.exceptions import AirflowSkipException
from defusedxml.ElementTree import fromstring as safe_xml_fromstring

log = logging.getLogger(__name__)

# httpx는 INFO에서 요청 URL을 통째로 남기는데, 오픈API 인증키가 쿼리 문자열에 실린다.
# URL 인코딩된 키는 Airflow의 비밀값 마스킹에도 걸리지 않으므로 로그 자체를 내지 않게 한다.
logging.getLogger("httpx").setLevel(logging.WARNING)

# 실거래가 오픈API 8종은 같은 서비스(1613000) 아래 오퍼레이션 이름만 다르다.
RTMS_API = "https://apis.data.go.kr/1613000"

# api/src/jobs/pipeline.py의 PIPELINES와 같아야 한다. DAG는 src를 import하지 않으므로 따로 든다.
# 키는 rtms_raw_items.api_id에 그대로 들어가는 슬러그다. 오타가 나면 silver가 그 행을 영영 못 찾는다.
RTMS_API_URLS = {
    # 단지 일련번호(aptSeq)는 상세 자료 API에만 있다. 나머지 필드는 기본 API와 같다.
    "apart_sale": f"{RTMS_API}/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
    "apart_rent": f"{RTMS_API}/RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
    "officetel_sale": f"{RTMS_API}/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",
    "officetel_rent": f"{RTMS_API}/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent",
    "multiflex_sale": f"{RTMS_API}/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",
    "multiflex_rent": f"{RTMS_API}/RTMSDataSvcRHRent/getRTMSDataSvcRHRent",
    "single_multi_family_sale": f"{RTMS_API}/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade",
    "single_multi_family_rent": f"{RTMS_API}/RTMSDataSvcSHRent/getRTMSDataSvcSHRent",
}
API_IDS = tuple(RTMS_API_URLS)

# 신고 기한 30일과 뒤늦은 해제·정정 때문에 지난 달 데이터도 계속 바뀐다.
RTMS_MONTHS = 2
RTMS_ROWS_PER_PAGE = 10000
# 단위 사이 간격(초). 간격 없이 순차로 부르면 초당 20건을 넘겨 게이트웨이가 초당 요청 제한(사유 코드 23)으로
# 막는다(2026-10-11 실측). jobs가 단위 처리 부담으로 냈던 초당 약 6건에 맞춘다.
RTMS_UNIT_INTERVAL = 0.1
# 실거래가 API 8종이 공유하는 결과코드.
RTMS_SUCCESS_RESULT_CODE = "000"
RTMS_NO_DATA_RESULT_CODE = "03"
# 회차 실패 메시지에 실을 좌표 수. 전량은 태스크 로그에 단위마다 남는다.
FAILED_UNITS_IN_MESSAGE = 20


# docker-compose.yml이 AIRFLOW_CONN_* 환경변수로 만드는 Connection. 메타 DB에는 저장하지 않는다.
PROP_TREND_DB_CONN_ID = "prop_trend_db"
# 인증키를 password에 둬야 Airflow가 태스크 로그에서 가린다.
DATA_GO_KR_CONN_ID = "data_go_kr"

LEGAL_DONG_API_URL = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
LEGAL_DONG_ROWS_PER_PAGE = 1000  # 1회 요청 최대 건수(초과 시 에러코드 336)
LEGAL_DONG_SUCCESS_PREFIX = "INFO"
OPEN_API_TIMEOUT = 10
# 일일 활용건수 초과. 실거래가 API 본문의 결과코드와 공공데이터포털 게이트웨이의 사유 코드가 같은 값이다.
DAILY_LIMIT_CODE = "22"

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

# api/src/jobs/lock.py의 LOCK_NAMESPACE·scope_key()와 같은 키다. jobs CLI와 백필이 남아 있는 동안
# 같은 범위를 동시에 갈아끼우면 두 트랜잭션이 서로의 INSERT를 못 보고 지워 행이 두 벌 남는다.
LOCK_NAMESPACE = 0x70726F70


def lock_key(scope: str) -> int:
    """범위 이름을 jobs와 같은 int4 advisory lock 키로 바꾼다."""
    return zlib.crc32(scope.encode()) & 0x7FFFFFFF


def raise_for_open_api_status(response: httpx.Response, api_name: str) -> None:
    """HTTP 오류 응답을 인증키 없는 예외로 바꾼다. 일일 한도 초과는 건너뜀으로 올린다."""
    if not response.is_error:
        return
    # 일일 한도를 넘기면 게이트웨이가 HTTP 429와 자체 오류 본문을 돌려준다. 초당 호출 제한 같은
    # 다른 429와 가르려고 본문의 사유 코드를 본다. 오류 본문은 형식을 보장하지 않는다.
    try:
        reason = safe_xml_fromstring(response.text).findtext("./cmmMsgHeader/returnReasonCode")
    except ParseError, ValueError:
        reason = None
    if reason == DAILY_LIMIT_CODE:
        # 재시도해도 같은 응답이다. 실패로 두면 재시도가 호출량만 쓴다.
        raise AirflowSkipException(f"{api_name} 일일 호출 제한(게이트웨이)")
    # raise_for_status()의 메시지에는 인증키가 실린 요청 URL이 들어가므로 상태 코드와 사유 코드만 남긴다.
    raise RuntimeError(f"{api_name} HTTP 오류: status={response.status_code}, reason={reason}")


def get_service_key() -> str:
    """공공데이터포털 인증키를 Connection에서 읽는다."""
    # 파싱 시점이 아니라 실행 시점에 읽는다. DAG 파일은 수시로 다시 파싱된다.
    service_key = BaseHook.get_connection(DATA_GO_KR_CONN_ID).password
    if not service_key:
        raise RuntimeError(f"{DATA_GO_KR_CONN_ID} Connection에 인증키(password)가 없다")
    return service_key


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
        raise_for_open_api_status(response, "법정동코드 API")

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


def fetch_rtms_items(
    client: httpx.Client,
    service_key: str,
    api_id: str,
    lawd_cd: str,
    deal_ymd: str,
) -> list[dict[str, str | None]]:
    """실거래가 API에서 갱신 단위(시군구 × 계약년월) 하나를 끝 페이지까지 받는다."""
    items: list[dict[str, str | None]] = []
    page_no = 1
    while True:
        params = {
            "serviceKey": service_key,
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ymd,
            "pageNo": page_no,
            "numOfRows": RTMS_ROWS_PER_PAGE,
        }
        response = client.get(RTMS_API_URLS[api_id], params=params)
        raise_for_open_api_status(response, f"{api_id} API")

        root = safe_xml_fromstring(response.text)
        result_code = root.findtext("./header/resultCode")
        if result_code == DAILY_LIMIT_CODE:
            raise AirflowSkipException(f"{api_id} API 일일 호출 제한")
        # 해당 시군구·계약년월에 거래가 없는 달은 정상이다. 빈 목록으로 갈아끼우면 그만이다.
        if result_code == RTMS_NO_DATA_RESULT_CODE:
            return items
        # 일시적 오류(01·02·04·05)도 예외로 올린다. 단위를 실패로 남기면 태스크 재시도가 다시 가져간다.
        if result_code != RTMS_SUCCESS_RESULT_CODE:
            raise RuntimeError(f"{api_id} API 오류 {result_code}: {root.findtext('./header/resultMsg')}")

        items.extend(
            {field.tag: (field.text.strip() if field.text else None) for field in item}
            for item in root.findall("./body/items/item")
        )
        if page_no * RTMS_ROWS_PER_PAGE >= int(root.findtext("./body/totalCount") or 0):
            return items
        page_no += 1


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
    # 비우기와 채우기는 한 트랜잭션이어야 한다. 커밋 전에 예외가 나면 닫히면서 롤백돼 어제 목록이 남는다.
    with closing(hook.get_conn()) as conn, closing(conn.cursor()) as cursor:
        # 트랜잭션 락이라 커밋·롤백과 함께 풀린다. 기다리지 않는 것은 jobs CLI의 전체 갱신이 이 범위를
        # 회차 내내 쥐고 있어서다. 그쪽이 같은 표를 막 갈아끼우는 중이므로 건너뛰어도 잃는 것이 없다.
        cursor.execute("SELECT pg_try_advisory_xact_lock(%s, %s)", (LOCK_NAMESPACE, lock_key("legal_dong")))
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


# 법정동코드 교체는 한 트랜잭션이라 실패해도 어제 목록이 남는다. 실거래 갱신을 막을 이유가 없다.
@task(trigger_rule=TriggerRule.ALL_DONE, execution_timeout=timedelta(hours=1))
def collect_rtms(api_id: str, **context: Any) -> dict[str, int]:
    """API 하나의 최근 계약년월 × 전 시군구를 받아 단위마다 bronze를 갈아끼운다."""
    service_key = get_service_key()

    # "이번 달"은 실행 시각이 아니라 회차 기준 시각(KST)으로 센다. 재시도해도 같은 달을 다시 받는다.
    run_after = pendulum.instance(context["dag_run"].run_after).in_timezone("Asia/Seoul")
    # 최신 월부터 돈다. 중간에 한도로 끊겨도 최근 데이터가 먼저 반영된다.
    months = [run_after.subtract(months=offset).format("YYYYMM") for offset in range(RTMS_MONTHS)]

    hook = PostgresHook(postgres_conn_id=PROP_TREND_DB_CONN_ID)
    # region_cd 앞 5자리가 실거래 API의 LAWD_CD다. 법정동코드 태스크가 시군구 행만 넣어 둔다.
    sigungu = [
        row[0]
        for row in hook.get_records(
            "SELECT DISTINCT left(payload ->> 'region_cd', 5) FROM legal_dong_code_raw_items "
            "WHERE payload ->> 'region_cd' IS NOT NULL ORDER BY 1"
        )
    ]
    if not sigungu:
        # 0단위로 끝나면 성공으로 보인다. 법정동코드가 한 번도 적재되지 않은 DB다.
        raise RuntimeError("시군구 목록이 비어 있다. 법정동코드를 먼저 적재해야 한다")

    units = [(lawd_cd, deal_ymd) for deal_ymd in months for lawd_cd in sigungu]
    log.info("%s: 시군구 %d × %d개월 = %d단위", api_id, len(sigungu), len(months), len(units))

    failed: list[str] = []
    loaded = 0
    # 반환 타입(CompatConnection)이 psycopg2·3 공통 메서드 중 rollback을 빠뜨리고 있다.
    with closing(cast("Any", hook.get_conn())) as conn, httpx.Client(timeout=OPEN_API_TIMEOUT) as client:
        # 세션 락이라 커밋을 거듭해도 태스크가 끝날 때까지 유지되고, 커넥션이 닫히면 풀린다.
        # 백필이 같은 API를 쥐고 있으면 한도도 그쪽이 쓰고 있다. 기다리지 않고 건너뛴다.
        with closing(conn.cursor()) as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s, %s)", (LOCK_NAMESPACE, lock_key(api_id)))
            acquired = cursor.fetchone()
        conn.commit()
        if not (acquired and acquired[0]):
            raise AirflowSkipException(f"다른 실행이 {api_id}를 갱신하고 있다")

        for done, (lawd_cd, deal_ymd) in enumerate(units):
            coordinate = f"{api_id}:{lawd_cd}:{deal_ymd}"
            time.sleep(RTMS_UNIT_INTERVAL)
            try:
                # 응답을 기다리는 동안 트랜잭션을 열어 두지 않도록 DB 작업 전에 다 받는다.
                items = fetch_rtms_items(client, service_key, api_id, lawd_cd, deal_ymd)
                # 단위마다 커밋한다. 다음 단위가 실패해도 이미 받은 응답은 남는다. 일일 호출 제한과
                # 30일 신고 기한 때문에 같은 파라미터로 다시 불러도 같은 응답이 온다는 보장이 없다.
                with closing(conn.cursor()) as cursor:
                    cursor.execute(
                        "DELETE FROM rtms_raw_items WHERE api_id = %s AND deal_ymd = %s AND lawd_cd = %s",
                        (api_id, deal_ymd, lawd_cd),
                    )
                    # 단위 키는 응답 필드가 아니라 요청 파라미터에서 온다. 필드가 개명돼도 삭제가 빗나가지 않는다.
                    cursor.executemany(
                        "INSERT INTO rtms_raw_items (api_id, lawd_cd, deal_ymd, payload) "
                        "VALUES (%s, %s, %s, %s::jsonb)",
                        [(api_id, lawd_cd, deal_ymd, json.dumps(item, ensure_ascii=False)) for item in items],
                    )
                conn.commit()
            except AirflowSkipException:
                # 남은 단위도 전부 같은 응답을 받는다. 받은 단위는 커밋됐으니 남은 것은 내일 회차에 맡긴다.
                conn.rollback()
                log.warning("%s: 일일 호출 제한으로 %d단위를 남기고 멈춘다", api_id, len(units) - done)
                raise
            except Exception:
                # 단위 하나는 트랜잭션째 되돌아가므로 나머지를 멈추지 않는다. 실패는 끝에서 태스크 상태로 올린다.
                conn.rollback()
                failed.append(coordinate)
                log.exception("bronze 갱신 실패: %s", coordinate)
            else:
                loaded += len(items)

    summary = {"units": len(units), "succeeded": len(units) - len(failed), "failed": len(failed), "loaded": loaded}
    log.info("%s: bronze 갱신 종료 %s", api_id, summary)
    if failed:
        # 한 단위라도 실패하면 태스크를 실패시켜 재시도에 맡긴다. 한 API의 단위 수가 호출 한도에 한참
        # 못 미치고 회차가 짧아, 실패 단위만 고르는 것보다 통째로 다시 도는 편이 단순하다.
        raise RuntimeError(f"{api_id}: {len(failed)}/{len(units)}단위 실패 {failed[:FAILED_UNITS_IN_MESSAGE]}")
    return summary


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
    """법정동코드를 교체하고 API 8종의 bronze를 따로 갱신한다."""

    legal_dong = refresh_legal_dong()

    collects = [
        # 백필 DAG와 같은 pool을 써서 같은 API를 동시에 쓰지 않고 줄을 선다. 한도가 API마다 걸리기 때문이다.
        collect_rtms.override(task_id=f"collect_{api_id}", pool=f"rtms_{api_id}")(api_id)
        for api_id in API_IDS
    ]

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize(**context: Any) -> dict[str, dict[str, int] | None]:
        """API별 요약을 모아 한 번에 남긴다."""
        ti = context["ti"]
        # 실패·건너뜀이면 XCom이 없다. 사유는 그리드의 태스크 상태가 말한다.
        return {api_id: ti.xcom_pull(task_ids=f"collect_{api_id}") for api_id in API_IDS}

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def watcher() -> None:
        """all_done 리프가 상류 실패를 가리지 않도록 DAG 실행을 실패로 만든다."""
        # DAG 실행 상태는 리프 태스크로 정해진다. summarize가 all_done으로 성공하면 상류 실패가 묻힌다.
        raise RuntimeError("갱신 태스크 중 실패한 것이 있다")

    legal_dong >> collects
    collects >> summarize()
    [legal_dong, *collects] >> watcher()


rtms_daily()
