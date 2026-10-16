"""갱신 단위마다 오픈API 응답으로 bronze를, bronze로 정제 테이블을 갈아끼운다."""

import json
import logging
import time
from contextlib import closing
from datetime import date
from typing import Any

import pendulum
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

# HttpHook이 adapter를 받지 않을 때 기본으로 다는 어댑터다. 재시도를 더하려면 직접 만들어 넘겨야 한다.
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter

from rtms.apis import RTMS_APIS
from rtms.db import (
    COMPLEX_COLUMNS,
    COMPLEX_KEY_COLUMNS,
    COMPLEX_UPSERT_SQL,
    RENT_COLUMNS,
    RENT_DELETE_SQL,
    RENT_INSERT_SQL,
    SALE_COLUMNS,
    SALE_DELETE_SQL,
    SALE_INSERT_SQL,
)
from rtms.open_api import DATA_GO_KR_CONN_ID, OPEN_API_RETRIES, fetch_rtms_items, get_service_key
from rtms.preprocess import preprocess_rtms

log = logging.getLogger(__name__)

# 단위 사이 간격(초). 간격 없이 순차로 부르면 초당 20건을 넘겨 게이트웨이가 초당 요청 제한(사유 코드 23)으로
# 막는다(2026-10-11 실측). 이전 갱신 배치가 단위 처리 부담으로 냈던 초당 약 6건에 맞춘다.
RTMS_UNIT_INTERVAL = 0.1
# 회차 실패 메시지에 실을 좌표 수. 전량은 태스크 로그에 단위마다 남는다.
FAILED_UNITS_IN_MESSAGE = 20


def sigungu_codes(hook: PostgresHook) -> list[str]:
    """법정동코드 bronze에서 실거래 API의 LAWD_CD로 쓸 시군구 코드를 읽는다."""
    # region_cd 앞 5자리가 실거래 API의 LAWD_CD다. 법정동코드 태스크가 시군구 행만 넣어 둔다.
    codes = [
        row[0]
        for row in hook.get_records(
            "SELECT DISTINCT left(payload ->> 'region_cd', 5) FROM legal_dong_code_raw_items "
            "WHERE payload ->> 'region_cd' IS NOT NULL ORDER BY 1"
        )
    ]
    if not codes:
        # 0단위로 끝나면 성공으로 보인다. 법정동코드가 한 번도 적재되지 않은 DB다.
        raise RuntimeError("시군구 목록이 비어 있다. 법정동코드를 먼저 적재해야 한다")
    return codes


def upsert_complexes(cursor: Any, property_type: str, rows: list[dict[str, Any]]) -> int:
    """정제 행에서 단지별로 가장 최근 거래의 속성을 골라 complexes에 upsert한다."""
    key_columns = COMPLEX_KEY_COLUMNS[property_type]
    latest: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        # 상세 자료로 재수집되기 전의 아파트 매매는 단지 일련번호가 없어 단지를 특정할 수 없다.
        if property_type == "APT" and row["apartment_serial_number"] is None:
            continue
        key = tuple(row[column] for column in key_columns)
        if key not in latest or row["deal_date"] > latest[key]["deal_date"]:
            latest[key] = row
    # 같은 단지를 upsert하는 태스크(아파트 매매·전월세)가 동시에 돌 때 행 락을 서로 다른 순서로 잡아
    # 데드락이 나지 않게 키 순으로 넣는다. NULL과 값을 직접 비교하면 TypeError가 나므로 NULL 여부를 먼저 비교한다.
    ordered = sorted(latest.items(), key=lambda item: tuple((value is not None, value) for value in item[0]))
    if ordered:
        cursor.executemany(
            COMPLEX_UPSERT_SQL[property_type],
            [tuple(row[column] for column in COMPLEX_COLUMNS) for _, row in ordered],
        )
    return len(ordered)


def collect_units(conn: Any, api_id: str, units: list[tuple[str, str]], seen: set[str]) -> dict[str, int]:
    """API 하나의 단위마다 오픈API 응답을 받아 bronze를 갈아끼우고, 받은 응답 필드를 seen에 모은다."""
    service_key = get_service_key()
    log.info("%s: bronze %d단위", api_id, len(units))

    failed: list[str] = []
    loaded = 0
    skipped: AirflowSkipException | None = None
    hook = HttpHook(
        method="GET",
        http_conn_id=DATA_GO_KR_CONN_ID,
        adapter=TCPKeepAliveAdapter(max_retries=OPEN_API_RETRIES),
    )
    with hook.get_conn() as session:
        for done, unit in enumerate(units):
            lawd_cd, deal_ymd = unit
            coordinate = f"{api_id}:{lawd_cd}:{deal_ymd}"
            time.sleep(RTMS_UNIT_INTERVAL)
            try:
                # 응답을 기다리는 동안 트랜잭션을 열어 두지 않도록 DB 작업 전에 다 받는다.
                items = fetch_rtms_items(session, hook.base_url, service_key, api_id, unit)
                seen.update(key for item in items for key in item)
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
            except AirflowSkipException as error:
                # 남은 단위도 전부 같은 응답을 받는다. 받은 단위는 커밋됐으니 남은 것은 다음 실행에 맡긴다.
                conn.rollback()
                log.warning("%s: 일일 호출 제한으로 %d단위를 남기고 멈춘다", api_id, len(units) - done)
                # 바로 던지지 않는다. 앞서 실패한 단위가 건너뜀에 묻히지 않게 끝에서 가른다.
                skipped = error
                break
            except Exception:
                # 단위 하나는 트랜잭션째 되돌아가므로 나머지를 멈추지 않는다. 실패는 끝에서 태스크 상태로 올린다.
                conn.rollback()
                failed.append(coordinate)
                log.exception("bronze 갱신 실패: %s", coordinate)
            else:
                loaded += len(items)

    if skipped:
        # skipped로 끝나면 DAG 실행이 성공으로 보여 앞서 실패한 단위가 묻힌다.
        if failed:
            # 재시도는 한도가 풀리기 전에 돌아 첫 단위에서 다시 skipped로 끝나므로, 실패 단위를 메우지 못하고
            # 실패만 다시 묻힌다. 재시도 없이 실패시켜 드러낸다.
            raise AirflowFailException(
                f"{api_id}: 일일 호출 제한 전 {len(failed)}/{len(units)}단위 실패 {failed[:FAILED_UNITS_IN_MESSAGE]}"
            )
        raise skipped

    summary = {"units": len(units), "succeeded": len(units) - len(failed), "failed": len(failed), "loaded": loaded}
    log.info("%s: bronze 갱신 종료 %s", api_id, summary)
    if failed:
        # 한 단위라도 실패하면 묶음을 실패시켜 재시도에 맡긴다. 묶음이 한 달·한 시도라 다시 부르는 양이 작아,
        # 실패 단위만 고르는 것보다 묶음째 다시 도는 편이 단순하다.
        raise RuntimeError(f"{api_id}: {len(failed)}/{len(units)}단위 실패 {failed[:FAILED_UNITS_IN_MESSAGE]}")
    return summary


def check_fields(api_id: str, seen: set[str]) -> None:
    """회차에서 받은 응답 필드를 API 기준선과 대조한다. 기준선에 있던 필드가 없으면 실패시킨다."""
    # 받은 item이 없으면 기준선 전 필드가 사라진 것으로 보여 오경보가 난다.
    if not seen:
        return
    known = RTMS_APIS[api_id].known_fields
    if unknown := seen - known:
        log.warning("%s: 기준선에 없는 필드가 왔다 %s", api_id, sorted(unknown))
    # 응답은 값이 비어도 태그는 보내므로, 회차 전체에서 한 번도 안 보인 필드는 개명·삭제다.
    if missing := known - seen:
        # 정리 규칙이 옛 이름으로 값을 찾아 정제 테이블에 빈칸이 쌓인다. 받은 bronze는 커밋됐고 정제는 all_done이라
        # 그대로 돈다. 재시도해도 같은 응답이므로 재시도 없이 실패시켜 DAG 실행을 실패로 드러낸다.
        raise AirflowFailException(f"{api_id}: 기준선에 있던 필드가 응답에 없다 {sorted(missing)}")


def process_units(conn: Any, api_id: str, units: list[tuple[str, str]]) -> dict[str, int]:
    """bronze에 남은 응답으로 API 하나의 단위마다 정제 테이블을 갈아끼운다."""
    api = RTMS_APIS[api_id]
    delete_sql, insert_sql = (SALE_DELETE_SQL, SALE_INSERT_SQL) if api.is_sale else (RENT_DELETE_SQL, RENT_INSERT_SQL)
    columns = SALE_COLUMNS if api.is_sale else RENT_COLUMNS
    log.info("%s: silver %d단위", api_id, len(units))

    failed: list[str] = []
    loaded = complexes = 0
    for lawd_cd, deal_ymd in units:
        coordinate = f"{api_id}:{lawd_cd}:{deal_ymd}"
        year, month = int(deal_ymd[:4]), int(deal_ymd[4:])
        month_start = date(year, month, 1)
        month_end = date(year + month // 12, month % 12 + 1, 1)
        try:
            # 실거래와 단지가 한 트랜잭션이라 정제가 롤백되면 이번 단위로 생긴 단지도 함께 되돌아간다.
            with closing(conn.cursor()) as cursor:
                cursor.execute(
                    "SELECT id, payload FROM rtms_raw_items WHERE api_id = %s AND deal_ymd = %s AND lawd_cd = %s",
                    (api_id, deal_ymd, lawd_cd),
                )
                raw_rows = cursor.fetchall()
                rows = [preprocess_rtms(api_id, raw_id, payload) for raw_id, payload in raw_rows]
                cursor.execute(delete_sql, (api.property_type, lawd_cd[:2], lawd_cd[2:], month_start, month_end))
                cursor.executemany(
                    insert_sql,
                    [
                        tuple(
                            json.dumps(value, ensure_ascii=False) if isinstance(value, dict) else value
                            for value in (row[column] for column in columns)
                        )
                        for row in rows
                    ],
                )
                unit_complexes = (
                    upsert_complexes(cursor, api.property_type, rows) if api.property_type in COMPLEX_KEY_COLUMNS else 0
                )
            conn.commit()
        except Exception:
            # 같은 자리에서 또 터진다면 일시 오류가 아니라 전처리 로직 쪽이다. bronze는 그대로라 코드를 고친 뒤
            # 이 태스크만 다시 돌리면 된다. 나머지 단위는 멈추지 않는다.
            conn.rollback()
            failed.append(coordinate)
            log.exception("silver 갱신 실패: %s", coordinate)
        else:
            loaded += len(rows)
            complexes += unit_complexes

    summary = {
        "units": len(units),
        "succeeded": len(units) - len(failed),
        "failed": len(failed),
        "loaded": loaded,
        "complexes": complexes,
    }
    log.info("%s: silver 갱신 종료 %s", api_id, summary)
    if failed:
        raise RuntimeError(f"{api_id}: {len(failed)}/{len(units)}단위 실패 {failed[:FAILED_UNITS_IN_MESSAGE]}")
    return summary


def rtms_units(hook: PostgresHook, context: dict[str, Any]) -> list[tuple[str, str]]:
    """이번 회차의 갱신 단위(시군구, 계약년월)를 기준 달부터 거슬러 최신 월부터 만든다."""
    params = context["params"]
    # "이번 달"은 실행 시각이 아니라 회차 기준 시각(KST)으로 센다. 재시도해도, bronze와 silver가 따로 돌아도,
    # 지난 회차를 clear로 다시 돌려도 그 회차의 달을 가리킨다. 입력한 기준 계약년월도 실행의 conf에 남아 같다.
    run_after = pendulum.instance(context["dag_run"].run_after).in_timezone("Asia/Seoul")
    base = run_after
    if params["base_ymd"]:
        base = pendulum.from_format(params["base_ymd"], "YYYYMM", tz="Asia/Seoul")
        # 아직 오지 않은 달은 응답이 비어 있어 정제 테이블의 그 달을 빈 목록으로 갈아끼운다. 형식은 Param이 막지만
        # 회차 기준 달과의 비교는 스키마로 나타낼 수 없다.
        if base > run_after:
            raise ValueError(
                f"기준 계약년월 {params['base_ymd']}이 회차 기준 달({run_after.format('YYYYMM')})보다 뒤다"
            )
    months = [base.subtract(months=offset).format("YYYYMM") for offset in range(params["months"])]
    sigungu = sigungu_codes(hook)
    # 최신 월부터 돈다. 중간에 한도로 끊겨도 최근 데이터가 먼저 반영된다.
    return [(lawd_cd, deal_ymd) for deal_ymd in months for lawd_cd in sigungu]
