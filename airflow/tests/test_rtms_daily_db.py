"""정기 갱신 태스크를 실제 Postgres로 돌려 맡은 범위만 갈아끼우는지 본다. 가짜 커넥션은 SQL이 스키마와 맞는지 모른다."""

import json
import os
import shutil
import subprocess
from collections.abc import Iterator
from contextlib import closing
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pendulum
import psycopg2
import pytest
import rtms_daily
from airflow.providers.postgres.hooks.postgres import PostgresHook
from rtms_daily import (
    PROP_TREND_DB_CONN_ID,
    SALE_COLUMNS,
    SALE_INSERT_SQL,
    collect_rtms,
    preprocess_rtms,
    process_regions,
    process_units,
    refresh_legal_dong,
)

API_DIR = Path(__file__).resolve().parents[2] / "api"

# 실데이터가 든 prop_trend와 분리한다. 테이블을 지우고 다시 만들기 때문에 이름이 _test로 끝나야만 쓴다.
# api 테스트 DB와도 나눠 두 프로젝트의 테스트가 서로의 테이블을 지우지 않게 한다.
TEST_DATABASE_URL = os.environ.get(
    "DAG_TEST_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/prop_trend_airflow_test"
)

# DAG는 api 모델을 import하지 않으므로 스키마는 api 가상환경에서 모델로 만든다. 테스트에 DDL을 따로 두면
# 모델이 바뀌어도 테스트는 옛 스키마로 통과한다.
SCHEMA_SCRIPT = """
import asyncio, os
from sqlalchemy.ext.asyncio import create_async_engine
from src.model import Base

async def main():
    engine = create_async_engine(os.environ["SCHEMA_DATABASE_URL"])
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    await engine.dispose()

asyncio.run(main())
"""

TABLES = (
    "rtms_raw_items",
    "legal_dong_code_raw_items",
    "sale_transactions",
    "rent_transactions",
    "complexes",
    "regions",
)

# 2026년 10월 회차라 최근 두 달(202610, 202609)이 갱신 범위다.
CONTEXT = {"dag_run": SimpleNamespace(run_after=pendulum.datetime(2026, 10, 15, 4, tz="Asia/Seoul"))}


@pytest.fixture(scope="module")
def database_url() -> str:
    database = TEST_DATABASE_URL.rsplit("/", 1)[1]
    if not database.endswith("_test"):
        raise RuntimeError(f"테스트 DB 이름은 _test로 끝나야 합니다: {database}")

    admin = psycopg2.connect(TEST_DATABASE_URL.rsplit("/", 1)[0] + "/postgres")
    admin.autocommit = True
    with closing(admin), closing(admin.cursor()) as cursor:
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
        if cursor.fetchone() is None:
            cursor.execute(f'CREATE DATABASE "{database}"')

    uv = shutil.which("uv")
    assert uv is not None, "스키마를 만들려면 uv가 필요하다"
    # 이 테스트를 airflow 가상환경에서 돌리고 있으므로, 물려받은 VIRTUAL_ENV를 지워야 uv가 api 가상환경을 쓴다.
    env = {key: value for key, value in os.environ.items() if key != "VIRTUAL_ENV"}
    env["SCHEMA_DATABASE_URL"] = TEST_DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
    subprocess.run(  # noqa: S603 - 인자는 이 모듈의 상수뿐이다.
        [uv, "run", "--frozen", "--no-build", "python", "-c", SCHEMA_SCRIPT],
        cwd=API_DIR,
        env=env,
        check=True,
    )
    return TEST_DATABASE_URL


@pytest.fixture
def conn(database_url: str, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """태스크가 쓰는 Connection을 테스트 DB로 돌리고, 빈 테이블에서 시작하는 시드·확인용 커넥션을 준다."""
    monkeypatch.setenv(f"AIRFLOW_CONN_{PROP_TREND_DB_CONN_ID.upper()}", database_url)
    monkeypatch.setenv(
        "AIRFLOW_CONN_DATA_GO_KR",
        json.dumps({"conn_type": "http", "host": "https://apis.data.go.kr", "password": "test-service-key"}),
    )
    with closing(psycopg2.connect(database_url)) as connection:
        with closing(connection.cursor()) as cursor:
            cursor.execute(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE")
        connection.commit()
        yield connection


def execute(conn: Any, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
    with closing(conn.cursor()) as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall() if cursor.description else []
    conn.commit()
    return rows


def put_bronze(conn: Any, api_id: str, unit: tuple[str, str], *payloads: dict[str, Any]) -> None:
    """단위 하나의 bronze를 주어진 응답으로 바꿔 둔다."""
    lawd_cd, deal_ymd = unit
    execute(
        conn,
        "DELETE FROM rtms_raw_items WHERE api_id = %s AND lawd_cd = %s AND deal_ymd = %s",
        (api_id, lawd_cd, deal_ymd),
    )
    for payload in payloads:
        execute(
            conn,
            "INSERT INTO rtms_raw_items (api_id, lawd_cd, deal_ymd, payload) VALUES (%s, %s, %s, %s::jsonb)",
            (api_id, lawd_cd, deal_ymd, json.dumps(payload, ensure_ascii=False)),
        )


def put_legal_dong(conn: Any, *items: dict[str, str]) -> None:
    for item in items:
        execute(
            conn,
            "INSERT INTO legal_dong_code_raw_items (payload) VALUES (%s::jsonb)",
            (json.dumps(item, ensure_ascii=False),),
        )


def sale(sgg_cd: str, deal_date: date, amount: str, **overrides: str) -> dict[str, str]:
    """매매 응답 item 하나."""
    return {
        "sggCd": sgg_cd,
        "dealYear": str(deal_date.year),
        "dealMonth": str(deal_date.month),
        "dealDay": str(deal_date.day),
        "dealAmount": amount,
        "umdNm": "청운동",
        "jibun": "1-1",
        "buildYear": "2000",
    } | overrides


def put_sale_transaction(conn: Any, api_id: str, payload: dict[str, str]) -> None:
    """정제 테이블에 이미 있던 매매 한 건을 넣는다."""
    row = preprocess_rtms(api_id, 0, payload)
    execute(conn, SALE_INSERT_SQL, tuple(row[column] for column in SALE_COLUMNS))


def run_process(api_id: str, units: list[tuple[str, str]]) -> dict[str, int]:
    with closing(PostgresHook(postgres_conn_id=PROP_TREND_DB_CONN_ID).get_conn()) as task_conn:
        return process_units(task_conn, api_id, units)


def test_collect_replaces_only_units_of_this_run(conn: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    put_legal_dong(conn, {"region_cd": "1111000000", "locatadd_nm": "서울특별시 종로구"})
    put_bronze(conn, "apart_sale", ("11110", "202610"), {"old": "회차 안"})
    put_bronze(conn, "apart_sale", ("11110", "202608"), {"old": "회차 밖"})
    put_bronze(conn, "apart_rent", ("11110", "202610"), {"old": "다른 API"})

    def fetch(session: Any, base_url: str, service_key: str, api_id: str, unit: tuple[str, str]) -> list[Any]:
        return [{"dealAmount": "1,000", "umdNm": "청운동"}] if unit == ("11110", "202610") else []

    monkeypatch.setattr(rtms_daily, "fetch_rtms_items", fetch)

    summary = collect_rtms.function("apart_sale", **CONTEXT)

    assert summary == {"units": 2, "succeeded": 2, "failed": 0, "loaded": 1}
    assert execute(conn, "SELECT api_id, deal_ymd, payload FROM rtms_raw_items ORDER BY api_id, deal_ymd") == [
        ("apart_rent", "202610", {"old": "다른 API"}),
        ("apart_sale", "202608", {"old": "회차 밖"}),
        ("apart_sale", "202610", {"dealAmount": "1,000", "umdNm": "청운동"}),
    ]


def test_process_replaces_only_its_month_sigungu_and_type(conn: Any) -> None:
    # 12월 단위는 끝 경계가 다음 해 1월 1일이다.
    for deal_date in (date(2026, 11, 30), date(2026, 12, 1), date(2026, 12, 31), date(2027, 1, 1)):
        put_sale_transaction(conn, "apart_sale", sale("11110", deal_date, "10,000"))
    put_sale_transaction(conn, "apart_sale", sale("11140", date(2026, 12, 15), "10,000"))
    put_sale_transaction(conn, "officetel_sale", sale("11110", date(2026, 12, 15), "10,000"))
    put_bronze(conn, "apart_sale", ("11110", "202612"), sale("11110", date(2026, 12, 10), "50,000"))

    summary = run_process("apart_sale", [("11110", "202612")])

    assert summary == {"units": 1, "succeeded": 1, "failed": 0, "loaded": 1, "complexes": 0}
    assert execute(
        conn,
        "SELECT property_type, sido_code || sigungu_code, deal_date, deal_amount FROM sale_transactions "
        "ORDER BY property_type, sigungu_code, deal_date",
    ) == [
        ("APT", "11110", date(2026, 11, 30), 100_000_000),
        ("APT", "11110", date(2026, 12, 10), 500_000_000),
        ("APT", "11110", date(2027, 1, 1), 100_000_000),
        ("APT", "11140", date(2026, 12, 15), 100_000_000),
        ("OFFICETEL", "11110", date(2026, 12, 15), 100_000_000),
    ]


def test_failed_unit_is_rolled_back_alone(conn: Any) -> None:
    put_sale_transaction(conn, "apart_sale", sale("11110", date(2026, 10, 5), "10,000"))
    put_sale_transaction(conn, "apart_sale", sale("11140", date(2026, 10, 5), "10,000"))
    put_bronze(conn, "apart_sale", ("11110", "202610"), sale("11110", date(2026, 10, 6), "삼만"))
    put_bronze(conn, "apart_sale", ("11140", "202610"), sale("11140", date(2026, 10, 6), "30,000"))

    with pytest.raises(RuntimeError, match="1/2단위 실패"):
        run_process("apart_sale", [("11110", "202610"), ("11140", "202610")])

    # 가공에 실패한 단위는 지우기 전 상태로 남고, 나머지 단위는 그대로 갈아끼워진다.
    assert execute(
        conn, "SELECT sido_code || sigungu_code, deal_date, deal_amount FROM sale_transactions ORDER BY 1"
    ) == [
        ("11110", date(2026, 10, 5), 100_000_000),
        ("11140", date(2026, 10, 6), 300_000_000),
    ]


def test_rent_road_address_is_stored_as_jsonb(conn: Any) -> None:
    put_bronze(
        conn,
        "apart_rent",
        ("11110", "202610"),
        {
            "sggCd": "11110",
            "dealYear": "2026",
            "dealMonth": "10",
            "dealDay": "1",
            "deposit": "1,000",
            "monthlyRent": "0",
            "roadnm": "자하문로",
            "roadnmbonbun": "00010",
        },
    )

    run_process("apart_rent", [("11110", "202610")])

    assert execute(conn, "SELECT deposit, road_address_detail FROM rent_transactions") == [
        (10_000_000, {"roadnm": "자하문로", "roadnmbonbun": "00010"})
    ]


def test_complexes_follow_latest_name_and_merge_null_keys(conn: Any) -> None:
    apart_unit, officetel_unit = ("11110", "202610"), ("11140", "202610")
    put_bronze(conn, "apart_sale", apart_unit, sale("11110", date(2026, 10, 1), "1", aptNm="옛이름", aptSeq="11110-1"))
    # 지번이 빈 오피스텔도 같은 건물이면 한 단지다. 부분 유니크 인덱스가 NULL끼리 같게 본다.
    put_bronze(conn, "officetel_sale", officetel_unit, sale("11140", date(2026, 10, 1), "1", offiNm="오피", jibun=""))
    run_process("apart_sale", [apart_unit])
    run_process("officetel_sale", [officetel_unit])

    put_bronze(conn, "apart_sale", apart_unit, sale("11110", date(2026, 10, 2), "1", aptNm="새이름", aptSeq="11110-1"))
    run_process("apart_sale", [apart_unit])
    run_process("officetel_sale", [officetel_unit])
    updated_at_sql = "SELECT updated_at FROM complexes WHERE property_type = 'APT'"
    updated_at = execute(conn, updated_at_sql)
    run_process("apart_sale", [apart_unit])

    assert execute(conn, "SELECT property_type, building_name, jibun FROM complexes ORDER BY id") == [
        ("APT", "새이름", "1-1"),
        ("OFFICETEL", "오피", None),
    ]
    # 같은 값으로 다시 돌면 고치지 않는다. 매일 덮으면 updated_at과 행 버전만 쌓인다.
    assert execute(conn, updated_at_sql) == updated_at


def test_regions_are_replaced_from_legal_dong_bronze(conn: Any) -> None:
    execute(conn, "INSERT INTO regions VALUES ('99', '999', '옛시도', '옛시군구')")
    put_legal_dong(
        conn,
        {"region_cd": "4111000000", "locatadd_nm": "경기도 수원시"},
        {"region_cd": "4111100000", "locatadd_nm": "경기도 수원시 장안구"},
        {"region_cd": "3611000000", "locatadd_nm": "세종특별자치시"},
    )

    summary = process_regions.function()

    assert summary == {"deleted": 1, "loaded": 2}
    assert execute(conn, "SELECT sido_code, sigungu_code, sido_name, sigungu_name FROM regions ORDER BY 1") == [
        ("36", "110", "세종특별자치시", "세종특별자치시"),
        ("41", "111", "경기도", "수원시 장안구"),
    ]


def test_legal_dong_bronze_is_replaced(conn: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    put_legal_dong(conn, {"region_cd": "9999900000", "locatadd_nm": "옛 지역"})
    new_row = {"region_cd": "1111000000", "locatadd_nm": "서울특별시 종로구"}
    monkeypatch.setattr(rtms_daily, "fetch_legal_dong_rows", lambda session, base_url, service_key: [new_row])

    summary = refresh_legal_dong.function()

    assert summary == {"deleted": 1, "loaded": 1}
    assert execute(conn, "SELECT payload FROM legal_dong_code_raw_items") == [(new_row,)]
