import json
import os
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
from dotenv import load_dotenv

API_URL = "http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
OUTPUT_PATH = Path(__file__).resolve().parent / "legal_dong_code.json"
MAX_ROWS_PER_PAGE = 10000


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
    root = ET.fromstring(response.text)

    result_code = root.findtext("./head/RESULT/resultCode") or root.findtext(
        "./resultCode"
    )
    result_msg = root.findtext("./head/RESULT/resultMsg") or root.findtext(
        "./resultMsg"
    )
    if result_code is None or not result_code.startswith("INFO"):
        raise RuntimeError(f"API error {result_code}: {result_msg}")

    return root


def collect_sigungu_codes(service_key: str) -> list[dict]:
    codes: dict[str, str] = {}
    page_no = 1

    while True:
        root = fetch_page(service_key, page_no)
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
            name = row.findtext("locatadd_nm")
            codes[region_cd[:5]] = name

        total_count = int(root.findtext("./head/totalCount"))
        if page_no * MAX_ROWS_PER_PAGE >= total_count:
            break
        page_no += 1

    return [{"code": code, "name": name} for code, name in sorted(codes.items())]


def main() -> None:
    load_dotenv()
    service_key = os.environ["DATA_GO_KR_SERVICE_KEY"]

    data = collect_sigungu_codes(service_key)

    OUTPUT_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"Saved {len(data)} entries to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
