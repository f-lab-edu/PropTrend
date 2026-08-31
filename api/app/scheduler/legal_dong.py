import xml.etree.ElementTree as ET

import requests
from defusedxml.ElementTree import fromstring as safe_xml_fromstring

from app.config import get_settings

settings = get_settings()
SERVICE_KEY = settings.data_go_kr_service_key

API_URL = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
MAX_ROWS_PER_PAGE = 10000
REQUEST_TIMEOUT_SECONDS = 10


class LegalDongCodeFetchError(Exception):
    pass


def _fetch_page(page_no: int) -> ET.Element:
    params = {
        "ServiceKey": SERVICE_KEY,
        "type": "xml",
        "pageNo": page_no,
        "numOfRows": MAX_ROWS_PER_PAGE,
        "flag": "Y",
    }
    response = requests.get(API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    root = safe_xml_fromstring(response.text)

    result_code = root.findtext("./head/RESULT/resultCode") or root.findtext(
        "./resultCode"
    )
    result_msg = root.findtext("./head/RESULT/resultMsg") or root.findtext(
        "./resultMsg"
    )
    if result_code is None or not result_code.startswith("INFO"):
        raise LegalDongCodeFetchError(f"API 오류 {result_code}: {result_msg}")

    return root


def collect_sigungu_codes() -> list[dict[str, str]]:
    """행정표준코드관리시스템(StanReginCd)에서 시군구 단위 법정동 코드를 전부 조회한다."""
    codes: dict[str, str] = {}
    page_no = 1

    while True:
        root = _fetch_page(page_no)
        rows = root.findall("./row")
        if not rows:
            break

        for row in rows:
            if (
                row.findtext("sgg_cd") == "000"
                or row.findtext("umd_cd") != "000"
                or row.findtext("ri_cd") != "00"
            ):
                continue
            region_cd = row.findtext("region_cd")
            if region_cd is None:
                continue
            codes[region_cd[:5]] = row.findtext("locatadd_nm") or ""

        total_count = int(root.findtext("./head/totalCount") or 0)
        if page_no * MAX_ROWS_PER_PAGE >= total_count:
            break
        page_no += 1

    return [{"code": code, "name": name} for code, name in sorted(codes.items())]
