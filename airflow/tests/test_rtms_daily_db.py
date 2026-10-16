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
from typing import Any, NamedTuple

import pendulum
import psycopg2
import pytest
import rtms.load
import rtms.tasks
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from rtms.apis import RTMS_APIS
from rtms.db import COMPLEX_COLUMNS, PROP_TREND_DB_CONN_ID, REGION_COLUMNS, RENT_COLUMNS, SALE_COLUMNS, SALE_INSERT_SQL
from rtms.load import process_units
from rtms.preprocess import preprocess_rtms
from rtms.tasks import (
    FIELDS_XCOM_KEY,
    check_rtms,
    collect_rtms,
    plan_rtms_chunks,
    process_regions,
    refresh_legal_dong,
)
from rtms_daily import RTMS_MONTHS

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
CONTEXT = {
    "dag_run": SimpleNamespace(run_after=pendulum.datetime(2026, 10, 15, 4, tz="Asia/Seoul")),
    "params": {"months": RTMS_MONTHS, "base_ymd": None},
}


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


def apart_sale_item(**overrides: str) -> dict[str, str]:
    """기준선 필드를 모두 갖춘 아파트 매매 응답 item 하나."""
    return dict.fromkeys(sorted(RTMS_APIS["apart_sale"].known_fields), "") | overrides


def without(item: dict[str, str], *fields: str) -> dict[str, str]:
    return {key: value for key, value in item.items() if key not in fields}


Responses = dict[tuple[str, str], list[dict[str, str]] | Exception]


class FakeTaskInstance:
    """수집 묶음이 남긴 XCom을 map_index별로 담았다가 Airflow처럼 돌려준다."""

    def __init__(self, chunks: list[dict[str, Any]]) -> None:
        self.chunks = chunks
        self.map_index = 0
        self.fields: dict[int, list[str]] = {}
        self.summaries: dict[int, dict[str, int]] = {}

    def xcom_push(self, key: str, value: list[str]) -> None:
        assert key == FIELDS_XCOM_KEY
        self.fields[self.map_index] = value

    def xcom_pull(self, task_ids: str, key: str = "return_value", map_indexes: Any = None) -> Any:
        if task_ids == "plan_rtms_chunks":
            return self.chunks
        if key == FIELDS_XCOM_KEY:
            return [self.fields.get(map_index) for map_index in map_indexes]
        # map_indexes 없이 매핑된 태스크를 꺼내면 남은 값이 하나뿐일 때 그 값만, 없으면 None이 온다.
        summaries = list(self.summaries.values())
        return summaries[0] if len(summaries) == 1 else summaries or None


class CollectRun(NamedTuple):
    outcomes: list[dict[str, int] | Exception]
    ti: FakeTaskInstance


def run_collect(conn: Any, monkeypatch: pytest.MonkeyPatch, responses: Responses, *lawd_cds: str) -> CollectRun:
    """주어진 시군구(기본은 종로구)로 202610·202609 묶음의 아파트 매매를 받는다. 응답은 단위마다 준다."""
    for lawd_cd in lawd_cds or ("11110",):
        put_legal_dong(conn, {"region_cd": f"{lawd_cd}00000", "locatadd_nm": f"시군구 {lawd_cd}"})

    def fetch(session: Any, base_url: str, service_key: str, api_id: str, unit: tuple[str, str]) -> list[Any]:
        response = responses.get(unit, [])
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(rtms.load, "fetch_rtms_items", fetch)
    chunks = plan_rtms_chunks.function(**CONTEXT)
    ti = FakeTaskInstance(chunks)
    outcomes: list[dict[str, int] | Exception] = []
    for map_index, chunk in enumerate(chunks):
        ti.map_index = map_index
        try:
            summary = collect_rtms.function("apart_sale", chunk, ti=ti, **CONTEXT)
        except Exception as error:  # noqa: BLE001 - 묶음마다 끝난 상태를 모아 본다.
            outcomes.append(error)
        else:
            ti.summaries[map_index] = summary
            outcomes.append(summary)
    return CollectRun(outcomes, ti)


def run_check(run: CollectRun) -> dict[str, int]:
    return check_rtms.function("apart_sale", ti=run.ti)


def run_process(api_id: str, units: list[tuple[str, str]]) -> dict[str, int]:
    with closing(PostgresHook(postgres_conn_id=PROP_TREND_DB_CONN_ID).get_conn()) as task_conn:
        return process_units(task_conn, api_id, units)


OCT, SEP = "202610", "202609"
SUMMARY_1_UNIT = {"units": 1, "succeeded": 1, "failed": 0, "loaded": 0}


def test_collect_replaces_only_units_of_this_run(conn: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    put_bronze(conn, "apart_sale", ("11110", OCT), {"old": "회차 안"})
    put_bronze(conn, "apart_sale", ("11110", "202608"), {"old": "회차 밖"})
    put_bronze(conn, "apart_rent", ("11110", OCT), {"old": "다른 API"})
    item = apart_sale_item(dealAmount="1,000", umdNm="청운동")

    run = run_collect(conn, monkeypatch, {("11110", OCT): [item]})

    assert run.outcomes == [SUMMARY_1_UNIT | {"loaded": 1}, SUMMARY_1_UNIT]
    assert execute(conn, "SELECT api_id, deal_ymd, payload FROM rtms_raw_items ORDER BY api_id, deal_ymd") == [
        ("apart_rent", OCT, {"old": "다른 API"}),
        ("apart_sale", "202608", {"old": "회차 밖"}),
        ("apart_sale", OCT, item),
    ]


def test_chunks_split_months_and_sido_from_latest(conn: Any) -> None:
    for region_cd in ("1111000000", "4111000000", "1114000000"):
        put_legal_dong(conn, {"region_cd": region_cd, "locatadd_nm": "시군구"})

    chunks = plan_rtms_chunks.function(**CONTEXT | {"params": {"months": 3, "base_ymd": None}})

    # 한도로 끊겨도 최근 데이터가 먼저 반영되도록 늘린 범위도 최신 월이 앞 map_index를 받는다.
    assert [(chunk["deal_ymd"], chunk["sido"], chunk["lawd_cds"]) for chunk in chunks] == [
        (OCT, "11", ["11110", "11140"]),
        (OCT, "41", ["41110"]),
        (SEP, "11", ["11110", "11140"]),
        (SEP, "41", ["41110"]),
        ("202608", "11", ["11110", "11140"]),
        ("202608", "41", ["41110"]),
    ]


@pytest.mark.parametrize(
    ("months", "expected"),
    [
        # 지난 달 하나가 잘못 들어갔을 때 그 달만 다시 돌린다.
        (1, ["202503"]),
        # 해를 넘겨 거슬러 올라간다.
        (3, ["202503", "202502", "202501"]),
    ],
)
def test_base_ymd_sets_end_of_range_apart_from_run(conn: Any, months: int, expected: list[str]) -> None:
    put_legal_dong(conn, {"region_cd": "1111000000", "locatadd_nm": "서울특별시 종로구"})

    chunks = plan_rtms_chunks.function(**CONTEXT | {"params": {"months": months, "base_ymd": "202503"}})

    assert [chunk["deal_ymd"] for chunk in chunks] == expected


def test_base_ymd_after_run_month_is_rejected(conn: Any) -> None:
    put_legal_dong(conn, {"region_cd": "1111000000", "locatadd_nm": "서울특별시 종로구"})

    # 아직 오지 않은 달은 빈 응답으로 정제 테이블의 그 달을 비운다.
    with pytest.raises(ValueError, match="202611"):
        plan_rtms_chunks.function(**CONTEXT | {"params": {"months": 1, "base_ymd": "202611"}})


def test_check_sums_summaries_of_succeeded_chunks(conn: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    responses: Responses = {("11110", OCT): [apart_sale_item()], ("11110", SEP): AirflowSkipException("한도")}

    # 실패·건너뜀인 묶음은 반환값이 없다. 남은 값이 하나뿐이어도 목록처럼 모아야 한다.
    assert run_check(run_collect(conn, monkeypatch, responses)) == {
        "chunks": 2,
        "succeeded_chunks": 1,
        "units": 1,
        "loaded": 1,
    }


def test_unknown_field_only_warns(conn: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    run = run_collect(conn, monkeypatch, {("11110", OCT): [apart_sale_item(newField="1")]})

    # 새 필드는 잃는 값이 없으므로 태스크를 막지 않는다.
    assert run_check(run)["loaded"] == 1
    assert "기준선에 없는 필드가 왔다 ['newField']" in caplog.text


def test_missing_field_fails_without_retry_after_storing_bronze(conn: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    renamed = without(apart_sale_item(), "aptSeq") | {"aptSeqNo": "11110-1"}
    run = run_collect(conn, monkeypatch, {("11110", OCT): [renamed]})

    # 수집 묶음은 성공으로 끝나고 받은 응답은 커밋돼 정제가 그대로 돌 수 있다. 실패는 대조 태스크가 낸다.
    assert run.outcomes == [SUMMARY_1_UNIT | {"loaded": 1}, SUMMARY_1_UNIT]
    assert execute(conn, "SELECT count(*) FROM rtms_raw_items") == [(1,)]
    with pytest.raises(AirflowFailException, match=r"\['aptSeq'\]"):
        run_check(run)


def test_field_seen_in_any_chunk_is_not_missing(conn: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # 묶음 하나에 없더라도 회차 전체에서 한 번이라도 보이면 개명·삭제가 아니다.
    responses: Responses = {
        ("11110", OCT): [without(apart_sale_item(), "aptSeq")],
        ("11110", SEP): [without(apart_sale_item(), "aptDong")],
    }

    assert run_check(run_collect(conn, monkeypatch, responses))["loaded"] == 2


def test_empty_responses_skip_field_check(conn: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    # 거래 없는 달만 있으면 기준선 전 필드가 사라진 것처럼 보인다.
    assert run_check(run_collect(conn, monkeypatch, {}))["loaded"] == 0


def test_daily_limit_skips_chunk_after_storing_received_units(conn: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    responses: Responses = {("11110", OCT): [apart_sale_item()], ("11140", OCT): AirflowSkipException("한도")}

    run = run_collect(conn, monkeypatch, responses, "11110", "11140")

    assert isinstance(run.outcomes[0], AirflowSkipException)
    assert execute(conn, "SELECT lawd_cd FROM rtms_raw_items") == [("11110",)]


def test_daily_limit_does_not_hide_missing_field(conn: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    responses: Responses = {
        ("11110", OCT): [without(apart_sale_item(), "aptSeq")],
        ("11140", OCT): AirflowSkipException("한도"),
    }

    run = run_collect(conn, monkeypatch, responses, "11110", "11140")

    # 건너뛴 묶음은 반환값이 없지만, 한도 전에 받은 응답의 필드도 대조에 들어가야 누락이 묻히지 않는다.
    assert isinstance(run.outcomes[0], AirflowSkipException)
    with pytest.raises(AirflowFailException, match="aptSeq"):
        run_check(run)


def test_daily_limit_does_not_hide_failed_unit(conn: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    responses: Responses = {("11110", OCT): RuntimeError("일시 오류"), ("11140", OCT): AirflowSkipException("한도")}

    run = run_collect(conn, monkeypatch, responses, "11110", "11140")

    # 재시도는 한도가 풀리기 전에 돌아 skipped로 끝나므로, 재시도 없는 실패로 끝나야 DAG 실행에 드러난다.
    assert isinstance(run.outcomes[0], AirflowFailException)
    assert "1/2단위 실패" in str(run.outcomes[0])


def test_failed_unit_retries_chunk_even_with_missing_field(conn: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    responses: Responses = {
        ("11110", OCT): [without(apart_sale_item(), "aptSeq")],
        ("11140", OCT): RuntimeError("일시 오류"),
    }

    run = run_collect(conn, monkeypatch, responses, "11110", "11140")

    # 묶음은 재시도할 수 있는 실패로 끝나야 일시 오류로 못 받은 단위가 메워진다. 누락은 대조 태스크가 따로 낸다.
    failure = run.outcomes[0]
    assert type(failure) is RuntimeError
    assert "1/2단위 실패" in str(failure)
    with pytest.raises(AirflowFailException, match="aptSeq"):
        run_check(run)


@pytest.mark.parametrize(
    ("table", "columns", "defaults"),
    [
        ("sale_transactions", SALE_COLUMNS, {"id", "created_at"}),
        ("rent_transactions", RENT_COLUMNS, {"id", "created_at"}),
        # property_type은 유형마다 upsert문에 값으로 박혀 있다. updated_at은 upsert문이 now()로 고친다.
        ("complexes", ("property_type", *COMPLEX_COLUMNS), {"id", "created_at", "updated_at"}),
        ("regions", REGION_COLUMNS, set()),
    ],
)
def test_insert_columns_match_model_table(conn: Any, table: str, columns: tuple[str, ...], defaults: set[str]) -> None:
    rows = execute(
        conn,
        "SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = %s",
        (table,),
    )

    # 모델에 컬럼이 늘었는데 DAG 목록을 놓치면 적재문이 그 컬럼을 건너뛰어 오류 없이 NULL만 쌓인다.
    # defaults는 DB 기본값에 맡기는 컬럼이다.
    assert {name for (name,) in rows} - defaults == set(columns)


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
    monkeypatch.setattr(rtms.tasks, "fetch_legal_dong_rows", lambda session, base_url, service_key: [new_row])

    summary = refresh_legal_dong.function()

    assert summary == {"deleted": 1, "loaded": 1}
    assert execute(conn, "SELECT payload FROM legal_dong_code_raw_items") == [(new_row,)]
