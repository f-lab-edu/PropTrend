import json
import os
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LEGAL_DONG_CODE_PATH = Path(__file__).resolve().parent / "legal_dong_code.json"

START_YYYYMM = "202608"
END_YYYYMM = "200601"
MAX_ROWS_PER_PAGE = 10000
REQUEST_DELAY_SECONDS = 0.1
MAX_TRANSIENT_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2.0
REQUEST_TIMEOUT_SECONDS = 10

SUCCESS_RESULT_CODE = "000"
NO_DATA_RESULT_CODE = "03"
DAILY_LIMIT_RESULT_CODE = "22"
TRANSIENT_RESULT_CODES = {"01", "02", "04", "05"}

KST = timezone(timedelta(hours=9))

API_CONFIGS = [
    ("apart_sale", "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"),
    ("apart_rent", "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"),
    ("officetel_sale", "https://apis.data.go.kr/1613000/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade"),
    ("officetel_rent", "https://apis.data.go.kr/1613000/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent"),
    ("multiflex_sale", "https://apis.data.go.kr/1613000/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade"),
    ("multiflex_rent", "https://apis.data.go.kr/1613000/RTMSDataSvcRHRent/getRTMSDataSvcRHRent"),
    ("single_multi_family_sale", "https://apis.data.go.kr/1613000/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade"),
    ("single_multi_family_rent", "https://apis.data.go.kr/1613000/RTMSDataSvcSHRent/getRTMSDataSvcSHRent"),
]


class DailyLimitReached(Exception):
    pass


class FatalApiError(Exception):
    pass


def load_region_codes() -> list[dict]:
    return json.loads(LEGAL_DONG_CODE_PATH.read_text(encoding="utf-8"))


def generate_yyyymm_range(start: str = START_YYYYMM, end: str = END_YYYYMM) -> list[str]:
    year, month = int(start[:4]), int(start[4:])
    end_year, end_month = int(end[:4]), int(end[4:])

    result = []
    while True:
        result.append(f"{year:04d}{month:02d}")
        if year == end_year and month == end_month:
            break
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return result


def build_work_items(region_codes: list[dict]) -> list[tuple[str, str]]:
    months = generate_yyyymm_range()
    return [
        (yyyymm, region["code"])
        for yyyymm in months
        for region in region_codes
    ]


def output_path(api_id: str, yyyymm: str, region_code: str) -> Path:
    return RESULTS_DIR / api_id / yyyymm / f"{region_code}.json"


def progress_path(api_id: str) -> Path:
    return RESULTS_DIR / f"{api_id}.progress.json"


def load_progress(api_id: str) -> dict | None:
    path = progress_path(api_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"[{api_id}] progress 파일을 읽을 수 없어 무시합니다: {exc}", file=sys.stderr)
        return None


def save_progress(api_id: str, yyyymm: str, region_code: str) -> None:
    path = progress_path(api_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "api_id": api_id,
        "last_completed": {"yyyymm": yyyymm, "region_code": region_code},
        "updated_at": datetime.now(KST).isoformat(),
    }
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)


def resume_start_index(work_items: list[tuple[str, str]], api_id: str) -> int:
    progress = load_progress(api_id)
    if progress is None:
        return 0

    last_completed = progress.get("last_completed") or {}
    target = (last_completed.get("yyyymm"), last_completed.get("region_code"))

    try:
        return work_items.index(target) + 1
    except ValueError:
        print(
            f"[{api_id}] progress의 마지막 완료 지점({target})을 현재 작업 목록에서 "
            "찾을 수 없어 처음부터 시작합니다.",
            file=sys.stderr,
        )
        return 0


def parse_item_element(item_el: ET.Element) -> dict:
    return {child.tag: (child.text or "") for child in item_el}


def parse_response(root: ET.Element) -> tuple[str, str, list[dict], int]:
    result_code = root.findtext("./header/resultCode") or ""
    result_msg = root.findtext("./header/resultMsg") or ""

    items_el = root.find("./body/items")
    items = [] if items_el is None else [parse_item_element(item_el) for item_el in items_el.findall("item")]

    total_count = int(root.findtext("./body/totalCount") or 0)

    return result_code, result_msg, items, total_count


def fetch_page_with_retry(
    session: requests.Session,
    base_url: str,
    service_key: str,
    region_code: str,
    yyyymm: str,
    page_no: int,
) -> tuple[str, str, list[dict], int]:
    params = {
        "serviceKey": service_key,
        "LAWD_CD": region_code,
        "DEAL_YMD": yyyymm,
        "pageNo": page_no,
        "numOfRows": MAX_ROWS_PER_PAGE,
    }

    attempt = 0
    while True:
        attempt += 1
        try:
            response = session.get(base_url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            root = ET.fromstring(response.text)
            result_code, result_msg, items, total_count = parse_response(root)
        except (requests.RequestException, ET.ParseError) as exc:
            if attempt > MAX_TRANSIENT_RETRIES:
                raise FatalApiError(f"네트워크/파싱 오류가 반복되어 중단합니다: {exc}") from exc
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
            continue

        if result_code == SUCCESS_RESULT_CODE or result_code == NO_DATA_RESULT_CODE:
            time.sleep(REQUEST_DELAY_SECONDS)
            return result_code, result_msg, items, total_count

        if result_code == DAILY_LIMIT_RESULT_CODE:
            raise DailyLimitReached(result_msg)

        if result_code in TRANSIENT_RESULT_CODES:
            if attempt > MAX_TRANSIENT_RETRIES:
                raise FatalApiError(f"API 오류 {result_code}: {result_msg}")
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
            continue

        raise FatalApiError(f"API 오류 {result_code}: {result_msg}")


def fetch_all_items_for_region_month(
    session: requests.Session,
    base_url: str,
    service_key: str,
    region_code: str,
    yyyymm: str,
) -> tuple[str, str, list[dict]]:
    page_no = 1
    result_code, result_msg, items, total_count = fetch_page_with_retry(
        session, base_url, service_key, region_code, yyyymm, page_no
    )

    if result_code == NO_DATA_RESULT_CODE:
        return result_code, result_msg, []

    all_items = list(items)
    while page_no * MAX_ROWS_PER_PAGE < total_count:
        page_no += 1
        result_code, result_msg, items, total_count = fetch_page_with_retry(
            session, base_url, service_key, region_code, yyyymm, page_no
        )
        all_items.extend(items)

    return result_code, result_msg, all_items


def save_output(
    api_id: str,
    yyyymm: str,
    region_code: str,
    result_code: str,
    result_msg: str,
    items: list[dict],
) -> None:
    path = output_path(api_id, yyyymm, region_code)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "header": {"resultCode": result_code, "resultMsg": result_msg},
        "body": {"items": {"item": items}},
    }
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_collector(api_id: str, base_url: str, service_key: str) -> dict:
    """Run one API's full collection loop. Runs inside its own thread, so it must
    never call sys.exit() (that would only kill this thread) — instead it returns
    a status dict and the caller (main) logs/aggregates it."""
    region_codes = load_region_codes()
    work_items = build_work_items(region_codes)
    start_index = resume_start_index(work_items, api_id)
    total = len(work_items)

    print(f"[{api_id}] {start_index}/{total} 지점부터 재개합니다.")

    completed = 0
    with requests.Session() as session:
        for i in range(start_index, total):
            yyyymm, region_code = work_items[i]
            path = output_path(api_id, yyyymm, region_code)

            if path.exists():
                save_progress(api_id, yyyymm, region_code)
                continue

            try:
                result_code, result_msg, items = fetch_all_items_for_region_month(
                    session, base_url, service_key, region_code, yyyymm
                )
            except DailyLimitReached:
                print(
                    f"[{api_id}] 일일 호출 제한에 도달했습니다. "
                    f"다음 실행 시 이어서 진행합니다. "
                    f"(이번 실행 {completed}건 완료, 중단 지점: {yyyymm} {region_code})"
                )
                return {"api_id": api_id, "status": "daily_limit", "completed": completed}
            except FatalApiError as exc:
                print(f"[{api_id}] 치명적 오류로 중단합니다: {exc}", file=sys.stderr)
                return {"api_id": api_id, "status": "fatal_error", "completed": completed, "error": str(exc)}

            save_output(api_id, yyyymm, region_code, result_code, result_msg, items)
            save_progress(api_id, yyyymm, region_code)
            completed += 1

    print(f"[{api_id}] 모든 지역/기간 수집이 완료되었습니다. (이번 실행 {completed}건)")
    return {"api_id": api_id, "status": "completed", "completed": completed}


def main() -> None:
    load_dotenv()
    service_key = os.environ["DATA_GO_KR_SERVICE_KEY"]

    results = []
    with ThreadPoolExecutor(max_workers=len(API_CONFIGS)) as executor:
        futures = {
            executor.submit(run_collector, api_id, base_url, service_key): api_id
            for api_id, base_url in API_CONFIGS
        }
        for future in as_completed(futures):
            api_id = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:  # unexpected bug, not an API/network error
                print(f"[{api_id}] 예상치 못한 오류로 중단되었습니다: {exc}", file=sys.stderr)
                results.append({"api_id": api_id, "status": "unexpected_error", "completed": 0, "error": str(exc)})

    print("\n=== 요약 ===")
    for result in sorted(results, key=lambda r: r["api_id"]):
        print(f"  {result['api_id']}: {result['status']} ({result['completed']}건 완료)")


if __name__ == "__main__":
    main()
