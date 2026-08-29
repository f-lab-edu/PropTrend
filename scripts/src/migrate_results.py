"""구(舊) 포맷(카테고리/월/지역코드.json)으로 이미 수집된 결과를
collect_rtms.py의 새 포맷(카테고리/월.json)으로 병합하는 1회성 마이그레이션 스크립트.

기본은 --apply 없이 dry-run으로 동작한다: 무엇을 병합/삭제할지만 보여주고
실제 파일 쓰기/삭제는 하지 않는다. 확인 후 --apply를 붙여 실제로 실행한다.
"""

import argparse
import json
import os
import re
import shutil
import sys
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parent / "results"
YYYYMM_DIR_PATTERN = re.compile(r"^\d{6}$")


def extract_items(data: dict) -> list[dict]:
    items = data.get("body", {}).get("items", {}).get("item", [])
    if isinstance(items, dict):
        items = [items]
    return items


def find_legacy_month_dirs(api_dir: Path) -> list[Path]:
    return sorted(
        p for p in api_dir.iterdir()
        if p.is_dir() and YYYYMM_DIR_PATTERN.match(p.name)
    )


def migrate_month(api_dir: Path, month_dir: Path, apply: bool) -> dict:
    api_id = api_dir.name
    yyyymm = month_dir.name
    final_path = api_dir / f"{yyyymm}.json"

    region_files = sorted(month_dir.glob("*.json"))
    if not region_files:
        return {"api_id": api_id, "yyyymm": yyyymm, "status": "empty_dir", "items": 0, "files": 0}

    if final_path.exists():
        return {"api_id": api_id, "yyyymm": yyyymm, "status": "final_exists_skip", "items": 0, "files": len(region_files)}

    items: list[dict] = []
    for region_file in region_files:
        data = json.loads(region_file.read_text(encoding="utf-8"))
        items.extend(extract_items(data))

    if apply:
        data = {
            "header": {"resultCode": "000", "resultMsg": "OK"},
            "body": {"items": {"item": items}},
        }
        tmp_path = final_path.with_suffix(".json.tmp")
        tmp_path.write_text(json.dumps(data, ensure_ascii=False) + "\n", encoding="utf-8")
        os.replace(tmp_path, final_path)

        verify_data = json.loads(final_path.read_text(encoding="utf-8"))
        verify_items = extract_items(verify_data)
        if len(verify_items) != len(items):
            raise RuntimeError(
                f"[{api_id}/{yyyymm}] 검증 실패: 원본 {len(items)}건, 병합 파일 {len(verify_items)}건"
            )

        shutil.rmtree(month_dir)

    return {"api_id": api_id, "yyyymm": yyyymm, "status": "migrated", "items": len(items), "files": len(region_files)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="실제로 병합 파일을 쓰고 구 포맷 디렉터리를 삭제한다. 기본은 dry-run.",
    )
    args = parser.parse_args()

    if not RESULTS_DIR.exists():
        print(f"결과 디렉터리가 없습니다: {RESULTS_DIR}", file=sys.stderr)
        sys.exit(1)

    api_dirs = sorted(p for p in RESULTS_DIR.iterdir() if p.is_dir())

    total_files = 0
    total_items = 0
    total_months = 0
    skipped = 0

    for api_dir in api_dirs:
        month_dirs = find_legacy_month_dirs(api_dir)
        if not month_dirs:
            continue

        api_files = 0
        api_items = 0
        for month_dir in month_dirs:
            result = migrate_month(api_dir, month_dir, apply=args.apply)
            if result["status"] == "final_exists_skip":
                print(f"  [SKIP] {result['api_id']}/{result['yyyymm']}: 최종 파일이 이미 존재함 (수동 확인 필요)")
                skipped += 1
                continue
            if result["status"] == "empty_dir":
                continue

            api_files += result["files"]
            api_items += result["items"]
            total_months += 1

        total_files += api_files
        total_items += api_items
        print(f"[{api_dir.name}] {len(month_dirs)}개월, {api_files}개 파일 -> {api_items:,}건")

    mode = "APPLY (실제 반영됨)" if args.apply else "DRY-RUN (미리보기, 변경 없음)"
    print(f"\n=== 요약 [{mode}] ===")
    print(f"  대상 월: {total_months}개")
    print(f"  병합 대상 파일: {total_files:,}개")
    print(f"  병합된 item 수: {total_items:,}건")
    if skipped:
        print(f"  건너뜀(이미 최종 파일 존재): {skipped}개월")
    if not args.apply:
        print("\n  실제로 적용하려면 --apply 옵션을 붙여 다시 실행하세요.")


if __name__ == "__main__":
    main()
