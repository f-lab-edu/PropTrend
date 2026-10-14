"""정기 갱신 DAG의 구조와 태스크가 부르는 수집 함수. 태스크 상태 규칙이 어긋나면 실패가 조용히 묻힌다."""

import json
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowSkipException
from rtms.open_api import LEGAL_DONG_ROWS_PER_PAGE, RTMS_ROWS_PER_PAGE, fetch_legal_dong_rows, fetch_rtms_items
from rtms.preprocess import preprocess_regions, preprocess_rtms
from rtms.refresh import RENT_COLUMNS, SALE_COLUMNS, upsert_complexes

AIRFLOW_DIR = Path(__file__).resolve().parents[1]
DAGS_DIR = AIRFLOW_DIR / "dags"
POOLS_PATH = AIRFLOW_DIR / "pools.json"

API_IDS = [
    "apart_sale",
    "apart_rent",
    "officetel_sale",
    "officetel_rent",
    "multiflex_sale",
    "multiflex_rent",
    "single_multi_family_sale",
    "single_multi_family_rent",
]
COLLECT_TASK_IDS = [f"collect_{api_id}" for api_id in API_IDS]
PROCESS_TASK_IDS = [f"process_{api_id}" for api_id in API_IDS]


def load_dagbag() -> DagBag:
    return DagBag(dag_folder=DAGS_DIR)


@pytest.fixture(scope="module")
def dag() -> DAG:
    dagbag = load_dagbag()
    assert dagbag.import_errors == {}
    return dagbag.dags["rtms_daily"]


def test_shared_package_is_not_parsed_as_dags() -> None:
    dagbag = load_dagbag()

    # 공용 패키지는 DAG를 정의하지 않는다. 파싱 대상이면 dag-processor가 매번 import만 하고 버린다.
    assert {Path(path).name for path in dagbag.file_last_changed} == {"rtms_daily.py", "rtms_replay.py"}


def python_task(dag: DAG, task_id: str) -> PythonOperator:
    task = dag.get_task(task_id)
    assert isinstance(task, PythonOperator)
    return task


def test_dag_has_one_collect_task_per_api(dag: DAG) -> None:
    collect_ids = [task_id for task_id in dag.task_ids if task_id.startswith("collect_")]

    # 한도·실패·재시도를 API 경계에서 가르려면 API 하나가 태스크 하나여야 한다.
    assert sorted(collect_ids) == sorted(COLLECT_TASK_IDS)


def test_collect_tasks_run_one_api_each(dag: DAG) -> None:
    for api_id, task_id in zip(API_IDS, COLLECT_TASK_IDS, strict=True):
        # 같은 함수를 API마다 인자만 바꿔 띄운다. 인자가 어긋나면 다른 API의 bronze를 갈아끼운다.
        assert tuple(python_task(dag, task_id).op_args) == (api_id,)


def test_collect_tasks_queue_on_their_own_pool(dag: DAG) -> None:
    pools = json.loads(POOLS_PATH.read_text())

    for task_id in COLLECT_TASK_IDS:
        pool = python_task(dag, task_id).pool
        # 없는 pool을 가리키는 태스크는 스케줄되지 않고 조용히 기다리기만 한다.
        assert pool in pools
        # 같은 API를 정기 갱신과 백필이 동시에 쓰지 않으려면 slot이 1이어야 한다.
        assert pools[pool]["slots"] == 1
    assert len(pools) == len(COLLECT_TASK_IDS)


def test_collect_tasks_run_even_if_legal_dong_fails(dag: DAG) -> None:
    for task_id in COLLECT_TASK_IDS:
        task = python_task(dag, task_id)

        assert task.upstream_task_ids == {"refresh_legal_dong"}
        # 법정동코드 교체는 원자적이라 실패해도 어제 목록으로 갱신할 수 있다.
        assert task.trigger_rule == "all_done"
        assert task.execution_timeout == timedelta(hours=1)


def test_legal_dong_runs_with_timeout(dag: DAG) -> None:
    assert python_task(dag, "refresh_legal_dong").execution_timeout == timedelta(minutes=10)


def test_process_tasks_follow_their_own_collect_task(dag: DAG) -> None:
    for api_id, task_id in zip(API_IDS, PROCESS_TASK_IDS, strict=True):
        task = python_task(dag, task_id)

        assert tuple(task.op_args) == (api_id,)
        # 다른 API의 수집을 기다리면 한도로 늦어진 API 하나가 나머지 정제까지 붙잡는다.
        assert task.upstream_task_ids == {f"collect_{api_id}"}
        # 수집이 한도로 건너뛰어도 받은 단위는 정제돼야 한다.
        assert task.trigger_rule == "all_done"
        # 오픈API를 부르지 않으므로 API pool에 줄 서지 않는다.
        assert task.pool == "default_pool"


def test_regions_follow_legal_dong_even_if_it_fails(dag: DAG) -> None:
    task = python_task(dag, "process_regions")

    assert task.upstream_task_ids == {"refresh_legal_dong"}
    # 법정동코드 수집이 실패해도 남은 bronze로 지역 목록을 다시 만든다.
    assert task.trigger_rule == "all_done"


def test_summarize_waits_for_every_process_task(dag: DAG) -> None:
    summarize = dag.get_task("summarize")

    assert summarize.upstream_task_ids == set(PROCESS_TASK_IDS)
    assert summarize.trigger_rule == "all_done"


def test_summarize_keeps_every_api_key(dag: DAG) -> None:
    class FakeTaskInstance:
        def xcom_pull(self, task_ids: str) -> dict[str, int] | None:
            # 성공한 태스크만 반환값이 XCom으로 남는다.
            return {"units": 500, "failed": 0} if task_ids == "collect_apart_sale" else None

    result = python_task(dag, "summarize").python_callable(ti=FakeTaskInstance())

    assert result["apart_sale"] == {"bronze": {"units": 500, "failed": 0}, "silver": None}
    # 실패·건너뜀인 API도 키는 남아야 "안 돈 것"이 보인다.
    assert result.keys() == set(API_IDS)
    assert result["apart_rent"] == {"bronze": None, "silver": None}


def test_watcher_fails_the_run_when_any_task_fails(dag: DAG) -> None:
    watcher = dag.get_task("watcher")

    # summarize가 all_done으로 성공하면 상류 실패가 DAG 실행 상태에서 사라진다. watcher가 그것을 드러낸다.
    assert watcher.upstream_task_ids == {
        "refresh_legal_dong",
        "process_regions",
        *COLLECT_TASK_IDS,
        *PROCESS_TASK_IDS,
    }
    assert watcher.trigger_rule == "one_failed"
    assert watcher.retries == 0


def mock_client(*responses: httpx.Response) -> tuple[httpx.Client, list[httpx.Request]]:
    """요청을 순서대로 받아 준비한 응답을 돌려주는 클라이언트."""
    requests: list[httpx.Request] = []
    pending = list(responses)

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return pending.pop(0)

    return httpx.Client(transport=httpx.MockTransport(handler)), requests


GATEWAY_DAILY_LIMIT_BODY = (
    "<OpenAPI_ServiceResponse><cmmMsgHeader>"
    "<returnReasonCode>22</returnReasonCode>"
    "</cmmMsgHeader></OpenAPI_ServiceResponse>"
)


def rtms_page(total_count: int, *deal_amounts: str, result_code: str = "000") -> str:
    items = "".join(f"<item><dealAmount> {amount} </dealAmount><aptDong/></item>" for amount in deal_amounts)
    return (
        f"<response><header><resultCode>{result_code}</resultCode><resultMsg>MSG</resultMsg></header>"
        f"<body><items>{items}</items><totalCount>{total_count}</totalCount></body></response>"
    )


def test_rtms_reads_every_page_of_a_unit() -> None:
    client, requests = mock_client(
        httpx.Response(200, text=rtms_page(RTMS_ROWS_PER_PAGE + 1, "10,000")),
        httpx.Response(200, text=rtms_page(RTMS_ROWS_PER_PAGE + 1, "20,000")),
    )

    items = fetch_rtms_items(client, "test-key", "apart_sale", "11110", "202610")

    assert [item["dealAmount"] for item in items] == ["10,000", "20,000"]
    # 빈 태그는 None으로 담겨야 기존 bronze payload와 모양이 같다.
    assert items[0]["aptDong"] is None
    assert [request.url.params["pageNo"] for request in requests] == ["1", "2"]
    assert requests[0].url.path.endswith("/getRTMSDataSvcAptTradeDev")
    assert requests[0].url.params["LAWD_CD"] == "11110"
    assert requests[0].url.params["DEAL_YMD"] == "202610"


def test_rtms_no_data_is_an_empty_unit() -> None:
    client, _ = mock_client(httpx.Response(200, text=rtms_page(0, result_code="03")))

    # 거래 없는 달도 0건으로 갈아끼워야 지난번 응답이 남지 않는다.
    assert fetch_rtms_items(client, "test-key", "apart_rent", "11110", "202610") == []


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, text=rtms_page(0, result_code="22")),
        httpx.Response(429, text=GATEWAY_DAILY_LIMIT_BODY),
    ],
)
def test_rtms_daily_limit_is_skipped(response: httpx.Response) -> None:
    client, _ = mock_client(response)

    # 남은 단위도 같은 응답을 받는다. 실패로 두면 재시도가 호출량만 쓴다.
    with pytest.raises(AirflowSkipException):
        fetch_rtms_items(client, "test-key", "apart_sale", "11110", "202610")


def test_rtms_error_result_code_is_an_error() -> None:
    client, _ = mock_client(httpx.Response(200, text=rtms_page(0, result_code="04")))

    with pytest.raises(RuntimeError, match="오류 04"):
        fetch_rtms_items(client, "test-key", "apart_sale", "11110", "202610")


def test_rtms_http_error_hides_service_key() -> None:
    client, _ = mock_client(httpx.Response(500, text="Internal Server Error"))

    with pytest.raises(RuntimeError) as error:
        fetch_rtms_items(client, "secret-service-key", "apart_sale", "11110", "202610")

    # 예외 메시지는 태스크 로그에 그대로 찍힌다.
    assert "secret-service-key" not in str(error.value)


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
    client, _ = mock_client(httpx.Response(429, text=GATEWAY_DAILY_LIMIT_BODY))

    # 재시도해도 같은 응답이라 실패가 아니라 건너뜀이어야 한다.
    with pytest.raises(AirflowSkipException):
        fetch_legal_dong_rows(client, "test-key")


def test_legal_dong_http_error_hides_service_key() -> None:
    client, _ = mock_client(httpx.Response(500, text="Internal Server Error"))

    with pytest.raises(RuntimeError) as error:
        fetch_legal_dong_rows(client, "secret-service-key")

    # 예외 메시지는 태스크 로그에 그대로 찍힌다.
    assert "secret-service-key" not in str(error.value)


SALE_PAYLOAD = {
    "sggCd": "11110",
    "dealYear": "2026",
    "dealMonth": "2",
    "dealDay": "27",
    "umdNm": "청운동",
    "jibun": "1-1",
    "aptNm": " 청운현대 ",
    "excluUseAr": "84.95",
    "dealAmount": "36,900",
    "floor": "3",
    "buildYear": "2000",
    "cdealType": " ",
    "cdealDay": "26.03.02",
    "rgstDate": "",
    "aptSeq": "11110-1",
}
RENT_PAYLOAD = {
    "sggCd": "11110",
    "dealYear": "2026",
    "dealMonth": "10",
    "dealDay": "1",
    "deposit": "1,000",
    "monthlyRent": "50",
    "preDeposit": "-500",
    "preMonthlyRent": "",
    "roadnm": "자하문로",
    "roadnmbonbun": "00010",
    "roadnmbubun": " ",
}


def test_sale_payload_is_normalized() -> None:
    row = preprocess_rtms("apart_sale", 1, SALE_PAYLOAD)

    # 적재문이 컬럼 상수 순서대로 값을 꺼내므로 키가 하나라도 어긋나면 안 된다.
    assert row.keys() == set(SALE_COLUMNS)
    assert row["property_type"] == "APT"
    assert (row["sido_code"], row["sigungu_code"]) == ("11", "110")
    assert row["deal_date"] == date(2026, 2, 27)
    assert row["deal_amount"] == 369_000_000
    assert row["exclusive_use_area"] == Decimal("84.95")
    assert row["building_name"] == "청운현대"
    # 원본의 빈 값 세 가지(없음, "", 공백)는 모두 NULL이다.
    assert row["cancel_deal_type"] is None
    assert row["registration_date"] is None
    assert row["house_type"] is None
    assert row["cancel_deal_date"] == date(2026, 3, 2)


def test_building_name_is_none_without_field() -> None:
    # 단독·다가구는 건물명 필드가 없다. 다른 유형의 필드가 섞여 와도 읽지 않는다.
    assert preprocess_rtms("single_multi_family_sale", 1, SALE_PAYLOAD)["building_name"] is None


def test_rent_payload_is_normalized() -> None:
    row = preprocess_rtms("apart_rent", 1, RENT_PAYLOAD)

    assert row.keys() == set(RENT_COLUMNS)
    assert (row["deposit"], row["monthly_rent"]) == (10_000_000, 500_000)
    # 종전 보증금은 음수로 들어오는 행이 있다. 원본 값을 그대로 둔다.
    assert row["previous_deposit"] == -5_000_000
    assert row["previous_monthly_rent"] is None
    # 도로명 필드는 값이 있는 것만 dict 하나로 묶는다.
    assert row["road_address_detail"] == {"roadnm": "자하문로", "roadnmbonbun": "00010"}


def test_rent_road_address_is_none_when_all_blank() -> None:
    payload = {key: value for key, value in RENT_PAYLOAD.items() if not key.startswith("roadnm")}

    assert preprocess_rtms("officetel_rent", 1, payload)["road_address_detail"] is None


@pytest.mark.parametrize(
    "payload",
    [
        {key: value for key, value in SALE_PAYLOAD.items() if key != "dealAmount"},
        SALE_PAYLOAD | {"dealAmount": "삼만"},
        SALE_PAYLOAD | {"excluUseAr": "84..9"},
        SALE_PAYLOAD | {"sggCd": "1111"},
    ],
)
def test_bad_payload_points_at_bronze_row(payload: dict[str, str]) -> None:
    # 어느 bronze 행인지 알아야 원본을 열어 볼 수 있다.
    with pytest.raises(ValueError, match=r"apart_sale 가공 실패\(rtms_raw_items.id=7\)"):
        preprocess_rtms("apart_sale", 7, payload)


def test_complexes_take_latest_deal_and_skip_apartments_without_serial() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.params: list[tuple[object, ...]] = []

        def executemany(self, sql: str, params: list[tuple[object, ...]]) -> None:
            self.params = params

    old = preprocess_rtms("apart_sale", 1, SALE_PAYLOAD | {"aptNm": "옛이름", "dealDay": "1"})
    new = preprocess_rtms("apart_sale", 2, SALE_PAYLOAD | {"aptNm": "새이름", "dealDay": "28"})
    # 상세 자료로 재수집되기 전의 행이라 단지를 특정할 수 없다.
    unknown = preprocess_rtms("apart_sale", 3, SALE_PAYLOAD | {"aptSeq": ""})
    other = preprocess_rtms("apart_sale", 4, SALE_PAYLOAD | {"aptSeq": "11110-0"})
    cursor = FakeCursor()

    assert upsert_complexes(cursor, "APT", [new, old, unknown, other]) == 2
    # 데드락을 피하려고 단지 키 순으로 넣는다. 단지명은 가장 최근 거래의 것이다.
    assert [(params[4], params[6]) for params in cursor.params] == [("청운현대", "11110-0"), ("새이름", "11110-1")]


def legal_dong_item(region_cd: str, name: str) -> dict[str, str]:
    return {"region_cd": region_cd, "locatadd_nm": name}


def test_regions_exclude_city_with_general_districts() -> None:
    rows = preprocess_regions(
        [
            legal_dong_item("4111000000", "경기도 수원시"),
            legal_dong_item("4111100000", "경기도 수원시 장안구"),
            legal_dong_item("3611000000", "세종특별자치시"),
        ]
    )

    # 일반구를 둔 시는 실거래가 구 코드로만 들어와 고르면 항상 빈 결과다.
    assert rows == [
        {"sido_code": "41", "sigungu_code": "111", "sido_name": "경기도", "sigungu_name": "수원시 장안구"},
        # 세종은 시군구 단계가 없어 시도명을 그대로 쓴다.
        {"sido_code": "36", "sigungu_code": "110", "sido_name": "세종특별자치시", "sigungu_name": "세종특별자치시"},
    ]


def test_regions_reject_sido_with_two_names() -> None:
    with pytest.raises(ValueError, match="이름이 하나가 아니다"):
        preprocess_regions(
            [legal_dong_item("4111100000", "경기도 수원시 장안구"), legal_dong_item("4113100000", "경기 성남시 수정구")]
        )
