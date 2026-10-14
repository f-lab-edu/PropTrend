"""갱신 단위(시군구 × 계약년월)마다 bronze와 정제 테이블을 갈아끼운다."""

import json
import logging
import time
import zlib
from contextlib import closing
from datetime import date
from typing import Any

import httpx

# [Airflow] provider 패키지(apache-airflow-providers-postgres)의 Hook. Connection 하나를 받아 DB 커넥션을 만들어 준다.
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.exceptions import AirflowSkipException

from rtms.open_api import OPEN_API_TIMEOUT, RTMS_APIS, fetch_rtms_items, get_service_key
from rtms.preprocess import preprocess_rtms

# [Airflow] 태스크 실행 중 표준 logging으로 남긴 로그는 Airflow가 가로채 UI의 태스크 로그에 보여 준다.
log = logging.getLogger(__name__)

# docker-compose.yml이 AIRFLOW_CONN_* 환경변수로 만드는 Connection. 메타 DB에는 저장하지 않는다.
PROP_TREND_DB_CONN_ID = "prop_trend_db"

# 단위 사이 간격(초). 간격 없이 순차로 부르면 초당 20건을 넘겨 게이트웨이가 초당 요청 제한(사유 코드 23)으로
# 막는다(2026-10-11 실측). jobs가 단위 처리 부담으로 냈던 초당 약 6건에 맞춘다.
RTMS_UNIT_INTERVAL = 0.1
# 회차 실패 메시지에 실을 좌표 수. 전량은 태스크 로그에 단위마다 남는다.
FAILED_UNITS_IN_MESSAGE = 20

# api/src/jobs/lock.py의 LOCK_NAMESPACE·scope_key()와 같은 키다. jobs CLI와 백필이 남아 있는 동안
# 같은 범위를 동시에 갈아끼우면 두 트랜잭션이 서로의 INSERT를 못 보고 지워 행이 두 벌 남는다.
LOCK_NAMESPACE = 0x70726F70
# 법정동코드 bronze와 지역 목록 범위. 실거래 범위는 api_id를 그대로 쓴다.
LEGAL_DONG_SCOPE = "legal_dong"

# 아래 컬럼은 api/src/model의 정제 테이블과 같아야 한다. id·created_at은 DB 기본값에 맡긴다.
TRANSACTION_COMMON_COLUMNS = (
    "property_type",
    "house_type",
    "sido_code",
    "sigungu_code",
    "umd_name",
    "jibun",
    "building_name",
    "deal_date",
    "exclusive_use_area",
    "floor",
    "build_year",
    "total_floor_area",
    "apartment_serial_number",
    "sigungu_name",
)
SALE_COLUMNS = (
    *TRANSACTION_COMMON_COLUMNS,
    "plottage_area",
    "land_area",
    "deal_amount",
    "dealing_type",
    "estate_agent_sigungu_name",
    "seller_type",
    "buyer_type",
    "cancel_deal_type",
    "cancel_deal_date",
    "registration_date",
    "apartment_dong",
    "land_leasehold_type",
)
RENT_COLUMNS = (
    *TRANSACTION_COMMON_COLUMNS,
    "deposit",
    "monthly_rent",
    "contract_term",
    "contract_type",
    "renewal_right_used",
    "previous_deposit",
    "previous_monthly_rent",
    "road_address_detail",
)


def _transaction_sql(table: str, columns: tuple[str, ...]) -> tuple[str, str]:
    """정제 테이블 하나의 단위 삭제문과 적재문을 만든다."""
    # 테이블·컬럼 이름은 이 모듈의 상수에서만 온다. 값은 전부 바인드 파라미터다.
    placeholders = ", ".join("%s::jsonb" if column == "road_address_detail" else "%s" for column in columns)
    delete_sql = (
        f"DELETE FROM {table} WHERE property_type = %s AND sido_code = %s AND sigungu_code = %s "  # noqa: S608
        "AND deal_date >= %s AND deal_date < %s"
    )
    insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"  # noqa: S608
    return delete_sql, insert_sql


SALE_DELETE_SQL, SALE_INSERT_SQL = _transaction_sql("sale_transactions", SALE_COLUMNS)
RENT_DELETE_SQL, RENT_INSERT_SQL = _transaction_sql("rent_transactions", RENT_COLUMNS)

# api/src/model/complex.py의 COMPLEX_KEY_COLUMNS와 같아야 한다. 단지를 만드는 유형은 이 둘뿐이다.
# 아파트는 단지명이 과거 거래까지 소급해 바뀌므로 이름이 아니라 단지 일련번호로 식별한다.
COMPLEX_KEY_COLUMNS = {
    "APT": ("apartment_serial_number",),
    "OFFICETEL": ("sido_code", "sigungu_code", "umd_name", "jibun", "building_name", "build_year"),
}
COMPLEX_COLUMNS = (
    "sido_code",
    "sigungu_code",
    "umd_name",
    "jibun",
    "building_name",
    "build_year",
    "apartment_serial_number",
)
COMPLEX_UPSERT_SQL = {
    # 키가 아닌 속성은 최근 거래 값으로 맞추되, 매일 같은 값으로 덮어 updated_at과 행 버전만 쌓이지 않도록
    # 달라졌을 때만 고친다. ON CONFLICT가 부분 유니크 인덱스를 추론하려면 인덱스와 같은 WHERE를 넘겨야 한다.
    "APT": """
        INSERT INTO complexes (property_type, sido_code, sigungu_code, umd_name, jibun, building_name, build_year,
                               apartment_serial_number)
        VALUES ('APT', %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (apartment_serial_number) WHERE property_type = 'APT'
        DO UPDATE SET sido_code = EXCLUDED.sido_code, sigungu_code = EXCLUDED.sigungu_code,
                      umd_name = EXCLUDED.umd_name, jibun = EXCLUDED.jibun,
                      building_name = EXCLUDED.building_name, build_year = EXCLUDED.build_year,
                      updated_at = now()
        WHERE (complexes.sido_code, complexes.sigungu_code, complexes.umd_name, complexes.jibun,
               complexes.building_name, complexes.build_year)
              IS DISTINCT FROM
              (EXCLUDED.sido_code, EXCLUDED.sigungu_code, EXCLUDED.umd_name, EXCLUDED.jibun,
               EXCLUDED.building_name, EXCLUDED.build_year)
    """,
    # 오피스텔은 키 컬럼이 곧 속성 전부라 고칠 값이 없다.
    "OFFICETEL": """
        INSERT INTO complexes (property_type, sido_code, sigungu_code, umd_name, jibun, building_name, build_year,
                               apartment_serial_number)
        VALUES ('OFFICETEL', %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sido_code, sigungu_code, umd_name, jibun, building_name, build_year)
            WHERE property_type = 'OFFICETEL'
        DO NOTHING
    """,
}


def try_lock(conn: Any, scope: str) -> None:
    """범위 락을 잡는다. 다른 실행이 쥐고 있으면 태스크를 건너뛴다."""
    # 세션 락이라 커밋을 거듭해도 태스크가 끝날 때까지 유지되고, 커넥션이 닫히면 풀린다.
    # 기다리지 않는 것은 jobs CLI와 백필이 범위를 회차 내내 쥐고 있어서다. 그쪽이 같은 범위를
    # 막 갈아끼우는 중이고 API 한도도 그쪽이 쓰고 있다.
    with closing(conn.cursor()) as cursor:
        cursor.execute(
            "SELECT pg_try_advisory_lock(%s, %s)",
            (LOCK_NAMESPACE, zlib.crc32(scope.encode()) & 0x7FFFFFFF),
        )
        acquired = cursor.fetchone()
    conn.commit()
    if not (acquired and acquired[0]):
        raise AirflowSkipException(f"다른 실행이 {scope}를 갱신하고 있다")


def sigungu_codes(hook: PostgresHook) -> list[str]:
    """법정동코드 bronze에서 실거래 API의 LAWD_CD로 쓸 시군구 코드를 읽는다."""
    # region_cd 앞 5자리가 실거래 API의 LAWD_CD다. 법정동코드 태스크가 시군구 행만 넣어 둔다.
    codes = [
        row[0]
        # [Airflow] DbApiHook의 편의 메서드. 커넥션을 열어 쿼리를 실행하고 fetchall() 결과를 돌려준 뒤 닫는다.
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


def collect_units(conn: Any, api_id: str, units: list[tuple[str, str]]) -> dict[str, int]:
    """API 하나의 단위마다 오픈API 응답을 받아 bronze를 갈아끼운다. 락은 부르는 쪽이 잡는다."""
    service_key = get_service_key()
    log.info("%s: bronze %d단위", api_id, len(units))

    failed: list[str] = []
    loaded = 0
    with httpx.Client(timeout=OPEN_API_TIMEOUT) as client:
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
                # 남은 단위도 전부 같은 응답을 받는다. 받은 단위는 커밋됐으니 남은 것은 다음 실행에 맡긴다.
                conn.rollback()
                log.warning("%s: 일일 호출 제한으로 %d단위를 남기고 멈춘다", api_id, len(units) - done)
                # 태스크가 skipped로 끝난다.
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


def process_units(
    conn: Any,
    api_id: str,
    units: list[tuple[str, str]],
    *,
    require_bronze: bool = False,
) -> dict[str, int]:
    """bronze에 남은 응답으로 API 하나의 단위마다 정제 테이블을 갈아끼운다. 락은 부르는 쪽이 잡는다."""
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
                if require_bronze and not raw_rows:
                    # 회차 안에서는 응답 0건이 곧 거래 없는 단위라 정상이다. 사람이 찍은 단위에서는 받은 적 없는
                    # 단위일 수 있어, 그대로 두면 정제 테이블의 거래를 지우고 0건을 넣는다.
                    raise RuntimeError("bronze에 응답이 없다. with_collect로 수집부터 다시 돌려야 한다")  # noqa: TRY301
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
