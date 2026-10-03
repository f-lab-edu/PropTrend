"""행정안전부 법정동코드(getStanReginCdList)를 받아 원본과 시군구 목록 두 벌로 남긴다.

- legal_dong_code_raw.json: 응답 행을 필드 하나 안 줄이고 그대로 담은 원본. load_results.py가
  읽어 bronze 표(legal_dong_code_raw_items)에 넣는다.
- legal_dong_code.json: 시군구 코드만 추린 목록. collect_rtms.py가 LAWD_CD 목록으로 읽는다.
"""

import json
import os
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
from defusedxml.ElementTree import fromstring as safe_xml_fromstring
from dotenv import load_dotenv

API_URL = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
RESULTS_DIR = Path(__file__).resolve().parents[1] / "results"
OUTPUT_PATH = RESULTS_DIR / "legal_dong_code.json"
RAW_OUTPUT_PATH = RESULTS_DIR / "legal_dong_code_raw.json"
MAX_ROWS_PER_PAGE = 1000  # 1회 요청 최대 건수(초과 시 에러코드 336)


def fetch_page(service_key: str, page_no: int) -> ET.Element:
    params = {
        "ServiceKey": service_key,
        "type": "xml",
        "pageNo": page_no,
        "numOfRows": MAX_ROWS_PER_PAGE,
        "flag": "Y",
    }
    response = requests.get(API_URL, params=params, timeout=10)
    response.raise_for_status()
    root = safe_xml_fromstring(response.text)

    result_code = root.findtext("./head/RESULT/resultCode") or root.findtext("./resultCode")
    result_msg = root.findtext("./head/RESULT/resultMsg") or root.findtext("./resultMsg")
    if result_code is None or not result_code.startswith("INFO"):
        raise RuntimeError(f"API error {result_code}: {result_msg}")

    return root


def row_to_dict(row: ET.Element) -> dict[str, str]:
    """<row>의 자식 태그를 그대로 옮긴다. 필드를 고르거나 이름을 바꾸지 않는다."""
    return {child.tag: (child.text or "").strip() for child in row}


def is_sigungu(row: dict[str, str]) -> bool:
    """시군구 단위(SSGGG00000) 행인지 판별한다.

    api/src/jobs/collector.py의 LegalDongCodeCollector._is_sigungu와 같은 규칙이다.
    """
    return row.get("sgg_cd") != "000" and row.get("umd_cd") == "000" and row.get("ri_cd") == "00"


def collect_rows(service_key: str) -> tuple[list[dict[str, str]], dict[str, str]]:
    """응답 전 행을 페이지 순서 그대로 모은다. 읍면동·리도 거르지 않는다."""
    rows: list[dict[str, str]] = []
    header: dict[str, str] = {}
    page_no = 1

    while True:
        root = fetch_page(service_key, page_no)
        if not header:
            header = {
                "resultCode": root.findtext("./head/RESULT/resultCode") or "",
                "resultMsg": root.findtext("./head/RESULT/resultMsg") or "",
            }

        page_rows = root.findall("./row")
        if not page_rows:
            break
        rows.extend(row_to_dict(row) for row in page_rows)

        total_count = int(root.findtext("./head/totalCount"))
        if page_no * MAX_ROWS_PER_PAGE >= total_count:
            break
        page_no += 1

    return rows, header


def to_region_codes(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """시군구 행만 골라 {code, name}으로 줄인다. code는 법정동코드 앞 5자리다."""
    codes: dict[str, str] = {}
    for row in rows:
        if is_sigungu(row):
            codes[row["region_cd"][:5]] = row["locatadd_nm"]
    return [{"code": code, "name": name} for code, name in sorted(codes.items())]


def main() -> None:
    load_dotenv()
    service_key = os.environ["DATA_GO_KR_SERVICE_KEY"]

    rows, header = collect_rows(service_key)
    region_codes = to_region_codes(rows)
    if not region_codes:
        # 데이터없음(INFO-200)도 fetch_page의 접두사 검사를 통과한다. 빈 결과를 그대로 쓰면
        # collect_rtms.py가 수집할 지역을 잃고, bronze 표는 통째로 비워진 채 다시 채워진다.
        raise RuntimeError("법정동코드 응답에 시군구 행이 없습니다")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    # RTMS 원본 파일과 같은 모양이라 load_results.py가 같은 경로로 읽는다.
    raw = {"header": header, "body": {"totalCount": len(rows), "items": {"item": rows}}}
    RAW_OUTPUT_PATH.write_text(json.dumps(raw, ensure_ascii=False) + "\n", encoding="utf-8")
    OUTPUT_PATH.write_text(json.dumps(region_codes, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"Saved {len(rows)} raw rows to {RAW_OUTPUT_PATH}")
    print(f"Saved {len(region_codes)} entries to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
