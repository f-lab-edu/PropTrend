import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import requests
from botocore.client import BaseClient
from defusedxml.ElementTree import fromstring as safe_xml_fromstring

from app.config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()
S3_BUCKET = settings.s3_bucket
S3_PREFIX = settings.s3_prefix
SERVICE_KEY = settings.data_go_kr_service_key

REGION_CODE_KEY = f"{S3_PREFIX}legal_dong_code.json"
COLLECTION_MONTHS = 24
MAX_ROWS_PER_PAGE = 10000
REQUEST_DELAY_SECONDS = 0.1
MAX_TRANSIENT_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2.0
REQUEST_TIMEOUT_SECONDS = 10

SUCCESS_RESULT_CODE = "000"
NO_DATA_RESULT_CODE = "03"
DAILY_LIMIT_RESULT_CODE = "22"
TRANSIENT_RESULT_CODES = {"01", "02", "04", "05"}

LAWD_CD_PATTERN = re.compile(r"^\d{5}$")
KST = timezone(timedelta(hours=9))

API_CONFIGS: list[tuple[str, str]] = [
    (
        "apart_sale",
        "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade",
    ),
    (
        "apart_rent",
        "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
    ),
    (
        "officetel_sale",
        "https://apis.data.go.kr/1613000/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",
    ),
    (
        "officetel_rent",
        "https://apis.data.go.kr/1613000/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent",
    ),
    (
        "multiflex_sale",
        "https://apis.data.go.kr/1613000/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",
    ),
    (
        "multiflex_rent",
        "https://apis.data.go.kr/1613000/RTMSDataSvcRHRent/getRTMSDataSvcRHRent",
    ),
    (
        "single_multi_family_sale",
        "https://apis.data.go.kr/1613000/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade",
    ),
    (
        "single_multi_family_rent",
        "https://apis.data.go.kr/1613000/RTMSDataSvcSHRent/getRTMSDataSvcSHRent",
    ),
]


class DailyLimitReached(Exception):
    pass


class FatalApiError(Exception):
    pass


def _recent_yyyymm_range(months: int = COLLECTION_MONTHS) -> list[str]:
    """오늘(KST)이 속한 달부터 과거로 `months`개월치 연월 문자열을 생성한다.

    매 실행마다 체크포인트 없이 이 구간 전체를 새로 수집해 최근에 등록/정정된
    거래를 반영한다."""
    today = datetime.now(KST)
    year, month = today.year, today.month
    result = []
    for _ in range(months):
        result.append(f"{year:04d}{month:02d}")
        month -= 1
        if month == 0:
            month, year = 12, year - 1
    return result


def _load_region_codes(s3_client: BaseClient) -> list[dict[str, str]]:
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=REGION_CODE_KEY)
    region_codes = json.loads(response["Body"].read().decode("utf-8"))
    for region in region_codes:
        code = region.get("code", "")
        if not LAWD_CD_PATTERN.fullmatch(code):
            raise ValueError(f"올바르지 않은 법정동 코드입니다: {code!r}")
    return region_codes


def _retry_or_raise(
    attempt: int, error_message: str, cause: Exception | None = None
) -> None:
    """재시도 한도 내면 백오프만큼 대기하고, 넘었으면 FatalApiError를 발생시킨다."""
    if attempt > MAX_TRANSIENT_RETRIES:
        raise FatalApiError(error_message) from cause
    time.sleep(RETRY_BACKOFF_SECONDS * attempt)


def parse_item_element(item_el: ET.Element) -> dict[str, str]:
    return {child.tag: (child.text or "") for child in item_el}


def parse_response(root: ET.Element) -> tuple[str, str, list[dict[str, str]], int]:
    result_code = root.findtext("./header/resultCode") or ""
    result_msg = root.findtext("./header/resultMsg") or ""

    items_el = root.find("./body/items")
    items = (
        []
        if items_el is None
        else [parse_item_element(item_el) for item_el in items_el.findall("item")]
    )

    total_count = int(root.findtext("./body/totalCount") or 0)

    return result_code, result_msg, items, total_count


def fetch_page_with_retry(
    session: requests.Session,
    base_url: str,
    region_code: str,
    yyyymm: str,
    page_no: int,
) -> tuple[str, str, list[dict[str, str]], int]:
    params = {
        "serviceKey": SERVICE_KEY,
        "LAWD_CD": region_code,
        "DEAL_YMD": yyyymm,
        "pageNo": page_no,
        "numOfRows": MAX_ROWS_PER_PAGE,
    }

    attempt = 0
    while True:
        attempt += 1
        try:
            response = session.get(
                base_url, params=params, timeout=REQUEST_TIMEOUT_SECONDS
            )
            response.raise_for_status()
            root = safe_xml_fromstring(response.text)
            result_code, result_msg, items, total_count = parse_response(root)
        except (requests.RequestException, ET.ParseError) as exc:
            _retry_or_raise(
                attempt, f"네트워크/파싱 오류가 반복되어 중단합니다: {exc}", exc
            )
            continue

        if result_code == SUCCESS_RESULT_CODE or result_code == NO_DATA_RESULT_CODE:
            time.sleep(REQUEST_DELAY_SECONDS)
            return result_code, result_msg, items, total_count

        if result_code == DAILY_LIMIT_RESULT_CODE:
            raise DailyLimitReached(result_msg)

        if result_code in TRANSIENT_RESULT_CODES:
            _retry_or_raise(attempt, f"API 오류 {result_code}: {result_msg}")
            continue

        raise FatalApiError(f"API 오류 {result_code}: {result_msg}")


def fetch_all_items_for_region_month(
    session: requests.Session, base_url: str, region_code: str, yyyymm: str
) -> list[dict[str, str]]:
    page_no = 1
    result_code, _, items, total_count = fetch_page_with_retry(
        session, base_url, region_code, yyyymm, page_no
    )

    if result_code == NO_DATA_RESULT_CODE:
        return []

    all_items = list(items)
    while page_no * MAX_ROWS_PER_PAGE < total_count:
        page_no += 1
        _, _, items, total_count = fetch_page_with_retry(
            session, base_url, region_code, yyyymm, page_no
        )
        all_items.extend(items)

    return all_items


def _upload_month(
    s3_client: BaseClient, api_id: str, yyyymm: str, items: list[dict[str, str]]
) -> None:
    """해당 (api_id, yyyymm)의 전 지역 병합 결과를 S3에 업로드한다.

    동일 키가 이미 있으면 덮어쓴다 - run_data_extraction_job이 읽는 파일을
    준비/갱신하는 것이 이 함수의 역할이다."""
    key = f"{S3_PREFIX}{api_id}/{yyyymm}.json"
    body = {
        "header": {"resultCode": SUCCESS_RESULT_CODE, "resultMsg": "OK"},
        "body": {"items": {"item": items}},
    }
    payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
    s3_client.put_object(
        Bucket=S3_BUCKET, Key=key, Body=payload, ContentType="application/json"
    )


def collect_one_api(
    s3_client: BaseClient,
    api_id: str,
    base_url: str,
    region_codes: list[dict[str, str]],
    months: list[str],
) -> int:
    """하나의 API에 대해 `months`에 속한 각 연월을 전 지역 수집 후 S3에 업로드한다.

    scripts/collect_rtms.py와 달리 이전 실행 지점을 이어받지 않고 매번 전체
    구간을 새로 수집해 최근 등록/정정된 거래까지 반영한다."""
    uploaded_months = 0
    with requests.Session() as session:
        for yyyymm in months:
            month_items: list[dict[str, str]] = []
            try:
                for region in region_codes:
                    month_items.extend(
                        fetch_all_items_for_region_month(
                            session, base_url, region["code"], yyyymm
                        )
                    )
            except DailyLimitReached:
                logger.warning(
                    "[%s] 일일 호출 제한에 도달하여 %s부터 이번 실행을 중단합니다.",
                    api_id,
                    yyyymm,
                )
                break
            except FatalApiError:
                logger.exception(
                    "[%s] %s 수집 중 치명적 오류로 이번 실행을 중단합니다.",
                    api_id,
                    yyyymm,
                )
                break

            _upload_month(s3_client, api_id, yyyymm, month_items)
            uploaded_months += 1

    return uploaded_months
