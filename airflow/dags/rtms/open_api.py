"""공공데이터포털 오픈API(법정동코드, 실거래가 8종)를 부른다."""

import logging
from typing import NamedTuple
from xml.etree.ElementTree import ParseError

import httpx

# [Airflow] airflow.sdk는 Airflow 3에서 DAG 작성자가 쓰는 공개 인터페이스다. 내부 모듈 대신 여기서 가져온다.
# BaseHook은 Connection 조회에 쓴다.
from airflow.sdk import BaseHook

# [Airflow] 태스크 안에서 던지면 실패가 아니라 skipped 상태로 끝나고 재시도도 하지 않는다.
from airflow.sdk.exceptions import AirflowSkipException
from defusedxml.ElementTree import fromstring as safe_xml_fromstring

# httpx는 INFO에서 요청 URL을 통째로 남기는데, 오픈API 인증키가 쿼리 문자열에 실린다.
# URL 인코딩된 키는 Airflow의 비밀값 마스킹에도 걸리지 않으므로 로그 자체를 내지 않게 한다.
logging.getLogger("httpx").setLevel(logging.WARNING)

# 실거래가 오픈API 8종은 같은 서비스(1613000) 아래 오퍼레이션 이름만 다르다.
RTMS_API = "https://apis.data.go.kr/1613000"


class RtmsApi(NamedTuple):
    """실거래가 API 하나의 호출 주소와 정제 테이블로 옮기는 방식."""

    url: str
    # api/src/model/prop_transaction.py의 PropertyType 값. 정제 테이블의 property_type 컬럼에 그대로 들어간다.
    property_type: str
    is_sale: bool
    # 건물명 필드 이름은 유형마다 다르고, 단독·다가구에는 아예 없다.
    building_name_field: str | None = None


# api/src/jobs/pipeline.py의 PIPELINES와 같아야 한다. DAG는 src를 import하지 않으므로 따로 든다.
# 키는 rtms_raw_items.api_id에 그대로 들어가는 슬러그다. 오타가 나면 silver가 그 행을 영영 못 찾는다.
RTMS_APIS = {
    # 단지 일련번호(aptSeq)는 상세 자료 API에만 있다. 나머지 필드는 기본 API와 같다.
    "apart_sale": RtmsApi(f"{RTMS_API}/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev", "APT", True, "aptNm"),
    "apart_rent": RtmsApi(f"{RTMS_API}/RTMSDataSvcAptRent/getRTMSDataSvcAptRent", "APT", False, "aptNm"),
    "officetel_sale": RtmsApi(f"{RTMS_API}/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade", "OFFICETEL", True, "offiNm"),
    "officetel_rent": RtmsApi(f"{RTMS_API}/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent", "OFFICETEL", False, "offiNm"),
    "multiflex_sale": RtmsApi(f"{RTMS_API}/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade", "ROW_HOUSE", True, "mhouseNm"),
    "multiflex_rent": RtmsApi(f"{RTMS_API}/RTMSDataSvcRHRent/getRTMSDataSvcRHRent", "ROW_HOUSE", False, "mhouseNm"),
    "single_multi_family_sale": RtmsApi(f"{RTMS_API}/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade", "SINGLE_MULTI", True),
    "single_multi_family_rent": RtmsApi(f"{RTMS_API}/RTMSDataSvcSHRent/getRTMSDataSvcSHRent", "SINGLE_MULTI", False),
}
API_IDS = tuple(RTMS_APIS)

RTMS_ROWS_PER_PAGE = 10000
# 실거래가 API 8종이 공유하는 결과코드.
RTMS_SUCCESS_RESULT_CODE = "000"
RTMS_NO_DATA_RESULT_CODE = "03"

# docker-compose.yml이 AIRFLOW_CONN_* 환경변수로 만드는 Connection. 메타 DB에는 저장하지 않는다.
# [Airflow] Connection은 외부 시스템 접속 정보(호스트·계정·비밀번호·extra)를 conn_id 하나로 묶은 것이다.
# 환경변수 AIRFLOW_CONN_{conn_id 대문자}가 있으면 그것이 먼저 쓰이고, 없으면 메타 DB(UI에서 등록한 것)를 찾는다.
# 인증키를 password에 둬야 Airflow가 태스크 로그에서 가린다.
DATA_GO_KR_CONN_ID = "data_go_kr"

LEGAL_DONG_API_URL = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
LEGAL_DONG_ROWS_PER_PAGE = 1000  # 1회 요청 최대 건수(초과 시 에러코드 336)
LEGAL_DONG_SUCCESS_PREFIX = "INFO"
OPEN_API_TIMEOUT = 10
# 일일 활용건수 초과. 실거래가 API 본문의 결과코드와 공공데이터포털 게이트웨이의 사유 코드가 같은 값이다.
DAILY_LIMIT_CODE = "22"


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
        response = client.get(RTMS_APIS[api_id].url, params=params)
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
