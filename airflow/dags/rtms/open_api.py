"""공공데이터포털 오픈API를 부르고 응답 XML을 행 목록으로 읽는다."""

import logging
from typing import Any
from urllib.parse import quote_plus
from xml.etree.ElementTree import ParseError

import requests
from airflow.sdk import BaseHook
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk.log import mask_secret
from defusedxml.ElementTree import fromstring as safe_xml_fromstring

from rtms.apis import RTMS_APIS

# urllib3는 DEBUG에서 요청 줄을 쿼리 문자열째 남기는데, 오픈API 인증키가 거기 실린다. URL 인코딩된 키는
# Airflow의 비밀값 마스킹에도 걸리지 않으므로 로그 수준을 낮춰도 새지 않게 막아 둔다.
logging.getLogger("urllib3").setLevel(logging.WARNING)

RTMS_ROWS_PER_PAGE = 10000
# 실거래가 API 8종이 공유하는 결과코드.
RTMS_SUCCESS_RESULT_CODE = "000"
RTMS_NO_DATA_RESULT_CODE = "03"

# docker-compose.yml이 AIRFLOW_CONN_* 환경변수로 만드는 Connection. 메타 DB에는 저장하지 않는다.
# host는 공공데이터포털 주소, password는 인증키다. 인증키를 password에 둬야 Airflow가 태스크 로그에서 가린다.
# login을 비워야 HttpHook이 Basic 인증 헤더를 붙이지 않는다.
DATA_GO_KR_CONN_ID = "data_go_kr"

LEGAL_DONG_ENDPOINT = "1741000/StanReginCd/getStanReginCdList"
LEGAL_DONG_ROWS_PER_PAGE = 1000  # 1회 요청 최대 건수(초과 시 에러코드 336)
LEGAL_DONG_SUCCESS_PREFIX = "INFO"
OPEN_API_TIMEOUT = 10
# 세션이 연결을 재사용하므로, 서버가 유휴 연결을 닫는 순간과 요청이 겹치면 단위 하나가 연결 오류로 실패한다.
# 실패는 태스크 재시도로 묶음의 모든 단위를 다시 부르게 하므로 GET을 한 번만 다시 보낸다.
# HTTP 오류 응답은 다시 보내지 않는다.
OPEN_API_RETRIES = 1
# 일일 활용건수 초과. 실거래가 API 본문의 결과코드와 공공데이터포털 게이트웨이의 사유 코드가 같은 값이다.
DAILY_LIMIT_CODE = "22"


def get_open_api(session: requests.Session, url: str, params: dict[str, Any], api_name: str) -> requests.Response:
    """오픈API를 GET으로 부른다. 실패는 인증키 없는 예외로 바꾸고, 일일 한도 초과는 건너뜀으로 올린다."""
    try:
        response = session.get(url, params=params, timeout=OPEN_API_TIMEOUT)
    except requests.RequestException as error:
        # 연결 실패·타임아웃의 메시지에는 인증키가 실린 요청 URL이 퍼센트 인코딩된 채 들어간다. 원래 예외를
        # 체인에 남기면 트레이스백으로 함께 찍히므로 끊는다.
        raise RuntimeError(f"{api_name} 요청 실패: {type(error).__name__}") from None
    if response.ok:
        return response
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
    # Airflow는 password 원문만 가린다. urllib3는 재시도할 때 요청 URL을 경고로 남기는데, 그 URL에는 requests가
    # 퍼센트 인코딩한 인증키가 실리므로 인코딩된 형태도 가린다.
    mask_secret(quote_plus(service_key))
    return service_key


def fetch_legal_dong_rows(session: requests.Session, base_url: str, service_key: str) -> list[dict[str, str | None]]:
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
        response = get_open_api(session, f"{base_url}/{LEGAL_DONG_ENDPOINT}", params, "법정동코드 API")

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
    session: requests.Session,
    base_url: str,
    service_key: str,
    api_id: str,
    unit: tuple[str, str],
) -> list[dict[str, str | None]]:
    """실거래가 API에서 갱신 단위(시군구, 계약년월) 하나를 끝 페이지까지 받는다."""
    lawd_cd, deal_ymd = unit
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
        response = get_open_api(session, f"{base_url}/{RTMS_APIS[api_id].endpoint}", params, f"{api_id} API")

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
