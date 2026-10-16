"""실거래 정기 갱신. 법정동코드와 실거래 8종의 오픈API 응답을 bronze 표에 갈아끼우고 정제 테이블로 옮긴다."""

import json
import logging
import time
from contextlib import closing
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, NamedTuple, cast
from urllib.parse import quote_plus
from xml.etree.ElementTree import ParseError

import pendulum
import requests
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseHook, Param, TriggerRule, dag, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk.log import mask_secret
from defusedxml.ElementTree import fromstring as safe_xml_fromstring

# HttpHook이 adapter를 받지 않을 때 기본으로 다는 어댑터다. 재시도를 더하려면 직접 만들어 넘겨야 한다.
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter

log = logging.getLogger(__name__)

# urllib3는 DEBUG에서 요청 줄을 쿼리 문자열째 남기는데, 오픈API 인증키가 거기 실린다. URL 인코딩된 키는
# Airflow의 비밀값 마스킹에도 걸리지 않으므로 로그 수준을 낮춰도 새지 않게 막아 둔다.
logging.getLogger("urllib3").setLevel(logging.WARNING)

# 실거래가 오픈API 8종은 같은 서비스(1613000) 아래 오퍼레이션 이름만 다르다. 호스트는 Connection이 갖는다.
RTMS_API = "1613000"


class RtmsApi(NamedTuple):
    """실거래가 API 하나의 호출 경로와 정제 테이블로 옮기는 방식."""

    endpoint: str
    # api/src/model/prop_transaction.py의 PropertyType 값. 정제 테이블의 property_type 컬럼에 그대로 들어간다.
    property_type: str
    is_sale: bool
    # 건물명 필드 이름은 유형마다 다르고, 단독·다가구에는 아예 없다.
    building_name_field: str | None
    # 응답 필드 드리프트 감지의 기준선. 이 API는 실제로 전 필드를 한 번 개명한 이력이 있다.
    known_fields: frozenset[str]


# scripts/src/process_silver.py의 SPECS와 같아야 한다. DAG는 src를 import하지 않으므로 따로 든다.
# 키는 rtms_raw_items.api_id에 그대로 들어가는 슬러그다. 오타가 나면 silver가 그 행을 영영 못 찾는다.
# 필드 기준선은 여기에만 있다. 응답 필드가 바뀌어 정리 규칙을 고칠 때 함께 고친다.
RTMS_APIS = {
    # 단지 일련번호(aptSeq)는 상세 자료 API에만 있다. 나머지 필드는 기본 API와 같다.
    "apart_sale": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
        "APT",
        True,
        "aptNm",
        frozenset(
            {
                "sggCd",
                "dealYear",
                "dealMonth",
                "dealDay",
                "umdNm",
                "aptNm",
                "jibun",
                "excluUseAr",
                "dealAmount",
                "floor",
                "buildYear",
                "cdealType",
                "cdealDay",
                "dealingGbn",
                "estateAgentSggNm",
                "rgstDate",
                "aptDong",
                "slerGbn",
                "buyerGbn",
                "landLeaseholdGbn",
                # 상세 자료 API(RTMSDataSvcAptTradeDev)에만 있는 필드.
                "aptSeq",
                "umdCd",
                "landCd",
                "bonbun",
                "bubun",
                "roadNm",
                "roadNmSggCd",
                "roadNmCd",
                "roadNmSeq",
                "roadNmbCd",
                "roadNmBonbun",
                "roadNmBubun",
            }
        ),
    ),
    "apart_rent": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
        "APT",
        False,
        "aptNm",
        frozenset(
            {
                "sggCd",
                "dealYear",
                "dealMonth",
                "dealDay",
                "umdNm",
                "aptNm",
                "jibun",
                "excluUseAr",
                "deposit",
                "monthlyRent",
                "floor",
                "buildYear",
                "contractTerm",
                "contractType",
                "useRRRight",
                "preDeposit",
                "preMonthlyRent",
                "roadnm",
                "roadnmsggcd",
                "roadnmcd",
                "roadnmseq",
                "roadnmbcd",
                "roadnmbonbun",
                "roadnmbubun",
                "aptSeq",
            }
        ),
    ),
    "officetel_sale": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",
        "OFFICETEL",
        True,
        "offiNm",
        frozenset(
            {
                "sggCd",
                "dealYear",
                "dealMonth",
                "dealDay",
                "sggNm",
                "umdNm",
                "jibun",
                "offiNm",
                "excluUseAr",
                "dealAmount",
                "floor",
                "buildYear",
                "cdealType",
                "cdealDay",
                "dealingGbn",
                "estateAgentSggNm",
                "slerGbn",
                "buyerGbn",
            }
        ),
    ),
    "officetel_rent": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent",
        "OFFICETEL",
        False,
        "offiNm",
        frozenset(
            {
                "sggCd",
                "dealYear",
                "dealMonth",
                "dealDay",
                "sggNm",
                "umdNm",
                "jibun",
                "offiNm",
                "excluUseAr",
                "deposit",
                "monthlyRent",
                "floor",
                "buildYear",
                "contractTerm",
                "contractType",
                "useRRRight",
                "preDeposit",
                "preMonthlyRent",
            }
        ),
    ),
    "multiflex_sale": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",
        "ROW_HOUSE",
        True,
        "mhouseNm",
        frozenset(
            {
                "sggCd",
                "dealYear",
                "dealMonth",
                "dealDay",
                "umdNm",
                "mhouseNm",
                "jibun",
                "buildYear",
                "excluUseAr",
                "landAr",
                "dealAmount",
                "floor",
                "cdealType",
                "cdealDay",
                "dealingGbn",
                "estateAgentSggNm",
                "rgstDate",
                "slerGbn",
                "buyerGbn",
                "houseType",
            }
        ),
    ),
    "multiflex_rent": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcRHRent/getRTMSDataSvcRHRent",
        "ROW_HOUSE",
        False,
        "mhouseNm",
        frozenset(
            {
                "sggCd",
                "dealYear",
                "dealMonth",
                "dealDay",
                "umdNm",
                "houseType",
                "mhouseNm",
                "jibun",
                "buildYear",
                "excluUseAr",
                "deposit",
                "monthlyRent",
                "floor",
                "contractTerm",
                "contractType",
                "useRRRight",
                "preDeposit",
                "preMonthlyRent",
            }
        ),
    ),
    "single_multi_family_sale": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade",
        "SINGLE_MULTI",
        True,
        None,
        frozenset(
            {
                "sggCd",
                "dealYear",
                "dealMonth",
                "dealDay",
                "umdNm",
                "houseType",
                "jibun",
                "totalFloorAr",
                "plottageAr",
                "dealAmount",
                "buildYear",
                "cdealType",
                "cdealDay",
                "dealingGbn",
                "estateAgentSggNm",
                "slerGbn",
                "buyerGbn",
            }
        ),
    ),
    "single_multi_family_rent": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcSHRent/getRTMSDataSvcSHRent",
        "SINGLE_MULTI",
        False,
        None,
        frozenset(
            {
                "sggCd",
                "dealYear",
                "dealMonth",
                "dealDay",
                "houseType",
                "umdNm",
                "totalFloorAr",
                "deposit",
                "monthlyRent",
                "buildYear",
                "contractTerm",
                "contractType",
                "useRRRight",
                "preDeposit",
                "preMonthlyRent",
            }
        ),
    ),
}
API_IDS = tuple(RTMS_APIS)

RTMS_ROWS_PER_PAGE = 10000
# 실거래가 API 8종이 공유하는 결과코드.
RTMS_SUCCESS_RESULT_CODE = "000"
RTMS_NO_DATA_RESULT_CODE = "03"

# docker-compose.yml이 AIRFLOW_CONN_* 환경변수로 만드는 Connection. 메타 DB에는 저장하지 않는다.
PROP_TREND_DB_CONN_ID = "prop_trend_db"
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


# 아파트 전월세 전용 도로명 7개 필드. 나머지 유형에서는 전부 NULL이라 컬럼 대신 JSONB 하나로 묶는다.
ROAD_ADDRESS_FIELDS = ("roadnm", "roadnmsggcd", "roadnmcd", "roadnmseq", "roadnmbcd", "roadnmbonbun", "roadnmbubun")


def _text(value: str | None) -> str | None:
    """원본이 쓰는 빈 값 세 가지(None, "", 공백 한 칸)를 모두 None으로 맞춘다."""
    if value is None:
        return None
    return value.strip() or None


def _required(row: dict[str, Any], field: str) -> str:
    """비어 있으면 안 되는 필드를 읽는다. payload에 키 자체가 없어도 빈 값과 같게 다룬다."""
    value = _text(row.get(field))
    if value is None:
        raise ValueError(f"{field}가 비어 있다")
    return value


def _sgg_cd(value: str) -> tuple[str, str]:
    """5자리 시군구코드를 시도 2자리 + 시군구 3자리로 나눈다."""
    # isdigit()만으로는 부족하다. 아라비아-인도 숫자 같은 비ASCII 숫자도 True다.
    if len(value) != 5 or not (value.isascii() and value.isdigit()):  # noqa: PLR2004
        raise ValueError(f"sgg_cd는 숫자 5자리여야 한다: {value!r}")
    return value[:2], value[2:]


def _int(value: str | None) -> int | None:
    text = _text(value)
    return None if text is None else int(text)


def _decimal(value: str | None) -> Decimal | None:
    text = _text(value)
    return None if text is None else Decimal(text)


def _amount(value: str | None) -> int | None:
    """만원 단위 콤마 문자열("36,900")을 원 단위 정수로 바꾼다."""
    text = _text(value)
    return None if text is None else int(text.replace(",", "")) * 10_000


def _short_date(value: str | None) -> date | None:
    """cdealDay/rgstDate의 "26.02.27"(YY.MM.DD) 형식을 date로 바꾼다."""
    text = _text(value)
    if text is None:
        return None
    year, month, day = text.split(".")
    return date(2000 + int(year), int(month), int(day))


def preprocess_rtms(api_id: str, raw_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    """bronze payload 한 건을 정제 테이블 행으로 바꾼다. 실패하면 어느 bronze 행인지 붙여 던진다."""
    api = RTMS_APIS[api_id]
    try:
        sido_code, sigungu_code = _sgg_cd(_required(payload, "sggCd"))
        row: dict[str, Any] = {
            "property_type": api.property_type,
            "house_type": _text(payload.get("houseType")),
            "sido_code": sido_code,
            "sigungu_code": sigungu_code,
            "umd_name": _text(payload.get("umdNm")),
            "jibun": _text(payload.get("jibun")),
            "building_name": _text(payload.get(api.building_name_field)) if api.building_name_field else None,
            "deal_date": date(
                int(_required(payload, "dealYear")),
                int(_required(payload, "dealMonth")),
                int(_required(payload, "dealDay")),
            ),
            "exclusive_use_area": _decimal(payload.get("excluUseAr")),
            "floor": _int(payload.get("floor")),
            "build_year": _int(payload.get("buildYear")),
            "total_floor_area": _decimal(payload.get("totalFloorAr")),
            "apartment_serial_number": _text(payload.get("aptSeq")),
            "sigungu_name": _text(payload.get("sggNm")),
        }
        if api.is_sale:
            row |= {
                "plottage_area": _decimal(payload.get("plottageAr")),
                "land_area": _decimal(payload.get("landAr")),
                "deal_amount": _amount(_required(payload, "dealAmount")),
                "dealing_type": _text(payload.get("dealingGbn")),
                "estate_agent_sigungu_name": _text(payload.get("estateAgentSggNm")),
                "seller_type": _text(payload.get("slerGbn")),
                "buyer_type": _text(payload.get("buyerGbn")),
                "cancel_deal_type": _text(payload.get("cdealType")),
                "cancel_deal_date": _short_date(payload.get("cdealDay")),
                "registration_date": _short_date(payload.get("rgstDate")),
                "apartment_dong": _text(payload.get("aptDong")),
                "land_leasehold_type": _text(payload.get("landLeaseholdGbn")),
            }
        else:
            road_address = {
                field: value for field in ROAD_ADDRESS_FIELDS if (value := _text(payload.get(field))) is not None
            }
            row |= {
                "deposit": _amount(_required(payload, "deposit")),
                "monthly_rent": _amount(_required(payload, "monthlyRent")),
                "contract_term": _text(payload.get("contractTerm")),
                "contract_type": _text(payload.get("contractType")),
                "renewal_right_used": _text(payload.get("useRRRight")),
                # 종전 보증금은 음수로 들어오는 행이 있다. 원본 값을 그대로 둔다.
                "previous_deposit": _amount(payload.get("preDeposit")),
                "previous_monthly_rent": _amount(payload.get("preMonthlyRent")),
                "road_address_detail": road_address or None,
            }
    except (ValueError, InvalidOperation) as error:
        raise ValueError(f"{api_id} 가공 실패(rtms_raw_items.id={raw_id}): {error}") from error
    return row


def preprocess_regions(items: list[dict[str, Any]]) -> list[dict[str, str]]:
    """법정동코드 시군구 행을 regions 행으로 바꾼다. 한 행이라도 비정상이면 예외를 던진다."""
    names = [_required(item, "locatadd_nm") for item in items]
    # 일반구를 둔 시("경기도 수원시")는 실거래가 구 코드로만 들어와 고르면 항상 빈 결과다.
    # 코드 앞자리로는 영동군(43740)·증평군(43745)처럼 별개 지역이 겹쳐 이름으로 판별한다.
    ancestors = {" ".join(tokens[:i]) for tokens in (name.split() for name in names) for i in range(1, len(tokens))}

    rows: list[dict[str, str]] = []
    sido_names: dict[str, str] = {}
    for item, name in zip(items, names, strict=True):
        if name in ancestors:
            continue
        # 실거래 수집의 LAWD_CD와 같은 출처(region_cd 앞 5자리)를 쓴다.
        sido_code, sigungu_code = _sgg_cd(_required(item, "region_cd")[:5])
        sido_name, _, sigungu_name = name.partition(" ")
        # 시도명이 행마다 반복되므로 같은 시도가 다른 이름으로 갈라지면 드롭다운에 시도가 둘로 보인다.
        if sido_names.setdefault(sido_code, sido_name) != sido_name:
            raise ValueError(f"시도 {sido_code}의 이름이 하나가 아니다: {sido_names[sido_code]!r}, {sido_name!r}")
        # 세종은 시군구 단계 없이 "세종특별자치시"만 온다.
        rows.append(
            {
                "sido_code": sido_code,
                "sigungu_code": sigungu_code,
                "sido_name": sido_name,
                "sigungu_name": sigungu_name or sido_name,
            }
        )
    return rows


# 단위 사이 간격(초). 간격 없이 순차로 부르면 초당 20건을 넘겨 게이트웨이가 초당 요청 제한(사유 코드 23)으로
# 막는다(2026-10-11 실측). 이전 갱신 배치가 단위 처리 부담으로 냈던 초당 약 6건에 맞춘다.
RTMS_UNIT_INTERVAL = 0.1
# 회차 실패 메시지에 실을 좌표 수. 전량은 태스크 로그에 단위마다 남는다.
FAILED_UNITS_IN_MESSAGE = 20
# 수집 묶음이 받은 응답 필드를 남기는 XCom 키. 반환값과 달리 건너뜀·실패로 끝난 묶음도 남긴다.
FIELDS_XCOM_KEY = "fields"

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

# api/src/model/region.py의 컬럼과 같아야 한다. 값은 preprocess_regions가 만든 dict에서 이름으로 꺼낸다.
REGION_COLUMNS = ("sido_code", "sigungu_code", "sido_name", "sigungu_name")
REGION_INSERT_SQL = (
    f"INSERT INTO regions ({', '.join(REGION_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join(f'%({column})s' for column in REGION_COLUMNS)})"
)


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


# 신고 기한 30일과 뒤늦은 해제·정정 때문에 지난 달 데이터도 계속 바뀐다. 정기 실행의 갱신 개월 수다.
RTMS_MONTHS = 2
# 수동 실행에서 늘릴 수 있는 상한. 24개월이면 API당 약 6,500회(2026-10-15 기준 시군구 269개)로 하루 호출
# 한도(10,000회) 안에 들고, 묶음은 API당 24 × 시도 16 = 384개로 Airflow의 매핑 상한(max_map_length 1024) 안에 든다.
MAX_RTMS_MONTHS = 24

# 법정동코드 응답 필드 드리프트 경고의 기준선. RtmsApi.known_fields처럼 여기에만 있다.
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


@dag(
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["rtms"],
    params={
        "months": Param(
            RTMS_MONTHS,
            type="integer",
            minimum=1,
            maximum=MAX_RTMS_MONTHS,
            title="갱신 개월 수",
            description="기준 계약년월부터 거슬러 올라가 갈아끼울 계약년월 수",
        ),
        # 갱신 범위의 끝 달. 비우면 회차 기준 달이라 정기 실행은 입력 없이 최근 달을 갈아끼운다.
        # 지난 달 하나가 잘못 들어갔을 때 그 달만 다시 돌리려면 끝 달을 실행 시각과 따로 정할 수 있어야 한다.
        "base_ymd": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}(0[1-9]|1[0-2])$",
            title="기준 계약년월",
            description="YYYYMM. 비우면 회차 기준 달이다. 예: 202503과 갱신 개월 수 1이면 2025년 3월만 갈아끼운다",
        ),
    },
)
def rtms_daily() -> None:
    """법정동코드와 지역 목록을 교체하고, API 8종을 묶음별로 bronze에 받아 정제 테이블로 옮긴다."""
    legal_dong = refresh_legal_dong()
    regions = process_regions()
    chunks = plan_rtms_chunks()

    legal_dong >> [regions, chunks]

    collects = []
    checks = []
    processes = []
    for api_id in API_IDS:
        # 묶음끼리는 단위가 겹치지 않고, 회차끼리는 max_active_runs=1로 겹치지 않는다.
        collect = collect_rtms.override(task_id=f"collect_{api_id}").partial(api_id=api_id).expand(chunk=chunks)
        check = check_rtms.override(task_id=f"check_{api_id}")(api_id)
        # 같은 API의 묶음이 다 끝난 뒤에 돈다. 필드 대조를 기다리지 않는다. 누락이 있어도 받은 bronze로 정제한다.
        process = process_rtms.override(task_id=f"process_{api_id}")(api_id)
        collect >> [check, process]
        collects.append(collect)
        checks.append(check)
        processes.append(process)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize(**context: Any) -> dict[str, dict[str, dict[str, int] | None]]:
        """API별 bronze·silver 요약을 모아 한 번에 남긴다."""
        ti = context["ti"]
        # 실패·건너뜀이면 XCom이 없다. 사유는 그리드의 태스크 상태가 말한다.
        return {
            api_id: {
                "bronze": ti.xcom_pull(task_ids=f"check_{api_id}"),
                "silver": ti.xcom_pull(task_ids=f"process_{api_id}"),
            }
            for api_id in API_IDS
        }

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def watcher() -> None:
        """all_done 리프가 상류 실패를 가리지 않도록 DAG 실행을 실패로 만든다."""
        # DAG 실행 상태는 리프 태스크로 정해진다. summarize가 all_done으로 성공하면 상류 실패가 묻힌다.
        raise RuntimeError("갱신 태스크 중 실패한 것이 있다")

    # bronze 요약을 check에서 꺼내므로 정제와 나란히 도는 check도 기다린다.
    [*checks, *processes] >> summarize()
    [legal_dong, regions, chunks, *collects, *checks, *processes] >> watcher()


rtms_daily()
