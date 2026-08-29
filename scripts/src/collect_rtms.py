import json
import os
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from defusedxml.ElementTree import fromstring as safe_xml_fromstring
from dotenv import load_dotenv

RESULTS_DIR = Path(__file__).resolve().parent / "results"
LEGAL_DONG_CODE_PATH = Path(__file__).resolve().parent / "results/legal_dong_code.json"


def _sanitize_path_segment(value: str) -> str:
    """api_id/yyyymm 등을 경로에 섞기 전에 디렉터리 구분자를 제거한다.

    이 값들은 현재는 하드코딩된 API_CONFIGS/생성된 연월 문자열이라 실질적인 위협은
    없지만, 정적 분석 도구(Sonar S2083)는 이를 신뢰할 수 없는 입력으로 간주하므로
    경로 조합 전에 os.path.basename으로 정제한다.
    """
    return os.path.basename(value)


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


def output_path(api_id: str, yyyymm: str) -> Path:
    """최종적으로 S3에 업로드될, 월 단위로 병합된 파일."""
    api_id, yyyymm = _sanitize_path_segment(api_id), _sanitize_path_segment(yyyymm)
    return RESULTS_DIR / api_id / f"{yyyymm}.json"


def partial_path(api_id: str, yyyymm: str) -> Path:
    """월 수집이 끝나기 전까지 지역별 결과를 임시로 쌓아두는 로컬 스테이징 파일.

    1회성 로컬 백필 과정에서만 쓰이는 파일이며, Lambda 등 원격 실행 환경으로
    옮겨갈 필요는 없다(그 경우엔 한 invocation이 한 달 분량을 통째로 메모리에
    누적했다가 끝나면 최종 파일 하나만 기록하는 편이 더 단순하다).
    """
    api_id, yyyymm = _sanitize_path_segment(api_id), _sanitize_path_segment(yyyymm)
    return RESULTS_DIR / api_id / f"{yyyymm}.json.partial"


def progress_path(api_id: str) -> Path:
    return RESULTS_DIR / f"{_sanitize_path_segment(api_id)}.progress.json"


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


def _retry_or_raise(attempt: int, error_message: str, cause: Exception | None = None) -> None:
    """재시도 한도 내면 백오프만큼 대기하고, 넘었으면 FatalApiError를 발생시킨다."""
    if attempt > MAX_TRANSIENT_RETRIES:
        raise FatalApiError(error_message) from cause
    time.sleep(RETRY_BACKOFF_SECONDS * attempt)


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
            root = safe_xml_fromstring(response.text)
            result_code, result_msg, items, total_count = parse_response(root)
        except (requests.RequestException, ET.ParseError) as exc:
            _retry_or_raise(attempt, f"네트워크/파싱 오류가 반복되어 중단합니다: {exc}", exc)
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


def append_partial(api_id: str, yyyymm: str, region_code: str, items: list[dict]) -> None:
    """한 지역의 수집 결과를 스테이징 파일에 한 줄(JSON Lines)로 추가한다.

    지역별로 파일 전체를 다시 쓰지 않고 append만 하므로 지역 수가 많은 달도
    저비용 I/O로 처리된다. 각 줄에 region_code를 함께 남기면, append 이후
    progress 저장 전에 프로세스가 죽어 같은 지역을 재수집하더라도 로드 시
    마지막 줄이 이전 줄을 덮어써 중복 없이 복구된다.
    """
    path = partial_path(api_id, yyyymm)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = {"region_code": region_code, "items": items}
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(line, ensure_ascii=False) + "\n")


def load_partial_items(api_id: str, yyyymm: str) -> list[dict]:
    path = partial_path(api_id, yyyymm)
    if not path.exists():
        return []

    by_region: dict[str, list[dict]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        entry = json.loads(line)
        by_region[entry["region_code"]] = entry["items"]

    items: list[dict] = []
    for region_items in by_region.values():
        items.extend(region_items)
    return items


def finalize_month(api_id: str, yyyymm: str, items: list[dict]) -> None:
    """해당 월의 모든 지역 수집이 끝난 뒤 병합 결과를 최종 파일로 확정하고
    스테이징 파일을 정리한다. S3에는 이 최종 파일 하나만 업로드하면 된다."""
    path = output_path(api_id, yyyymm)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "header": {"resultCode": SUCCESS_RESULT_CODE, "resultMsg": "OK"},
        "body": {"items": {"item": items}},
    }
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(data, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)
    partial_path(api_id, yyyymm).unlink(missing_ok=True)


def run_collector(api_id: str, base_url: str, service_key: str) -> dict:
    """Run one API's full collection loop. Runs inside its own thread, so it must
    never call sys.exit() (that would only kill this thread) — instead it returns
    a status dict and the caller (main) logs/aggregates it.

    출력 파일은 지역별이 아니라 (api_id, yyyymm) 단위로 하나만 남는다. 지역별
    결과는 스테이징 파일에 모았다가, 해당 월의 마지막 지역까지 끝나면 병합해서
    최종 파일 하나로 확정한다.
    """
    region_codes = load_region_codes()
    num_regions = len(region_codes)
    work_items = build_work_items(region_codes)
    start_index = resume_start_index(work_items, api_id)
    total = len(work_items)

    print(f"[{api_id}] {start_index}/{total} 지점부터 재개합니다.")

    completed = 0
    current_yyyymm: str | None = None
    month_items: list[dict] = []

    with requests.Session() as session:
        i = start_index
        while i < total:
            yyyymm, region_code = work_items[i]

            if yyyymm != current_yyyymm:
                final_path = output_path(api_id, yyyymm)
                if final_path.exists():
                    # 이 달은 이미 병합 완료됨 — 지역 전부 건너뛴다.
                    save_progress(api_id, yyyymm, region_codes[-1]["code"])
                    i += num_regions
                    continue
                current_yyyymm = yyyymm
                month_items = load_partial_items(api_id, yyyymm)

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

            month_items.extend(items)
            append_partial(api_id, yyyymm, region_code, items)
            save_progress(api_id, yyyymm, region_code)
            completed += 1

            is_last_region_of_month = i == total - 1 or work_items[i + 1][0] != yyyymm
            if is_last_region_of_month:
                finalize_month(api_id, yyyymm, month_items)
                current_yyyymm = None
                month_items = []

            i += 1

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
