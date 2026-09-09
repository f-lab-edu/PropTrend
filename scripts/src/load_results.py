"""scripts/results에 쌓인 국토교통부 실거래가 원본 JSON을 가공해 DB에 적재하는 일회성 배치.

collect_rtms.py가 남긴 월별 원본 응답을 api/data_collection의 가공/적재 모듈에 그대로
흘려보낸다. 적재 전 테이블이 없으면 만들고, 파일 단위로 진행 상황을 기록해 중단된
지점부터 다시 이어갈 수 있다.

가공/적재 모듈과 모델이 api 프로젝트에 있으므로 api 가상환경으로 실행한다.

    uv run --project api python scripts/src/load_results.py
"""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "api"))

# api 프로젝트를 import 경로에 올린 뒤에야 아래 모듈들을 불러올 수 있다.

from app.db import async_session_factory, engine
from app.model import Base
from data_collection.loader import (
    DataLoader,
    LegalStandardCodeLoader,
    RentTransactionLoader,
    SaleTransactionLoader,
)
from data_collection.processor import (
    DataProcessor,
    RTMSAptRentProcessor,
    RTMSAptTradeProcessor,
    RTMSOffiRentProcessor,
    RTMSOffiTradeProcessor,
    RTMSRHRentProcessor,
    RTMSRHTradeProcessor,
    RTMSSHRentProcessor,
    RTMSSHTradeProcessor,
)

logger = logging.getLogger("load_results")

RESULTS_DIR = Path(
    os.environ.get("RESULTS_DIR", str(REPO_ROOT / "scripts" / "results"))
)
LEGAL_DONG_CODE_PATH = RESULTS_DIR / "legal_dong_code.json"
PROGRESS_PATH = RESULTS_DIR / "load_results.progress.json"

YYYYMM_PATTERN = re.compile(r"^\d{6}$")
KST = timezone(timedelta(hours=9))

# api_id -> (가공 모듈, 적재 모듈). collect_rtms.py의 api_id를 그대로 쓴다.
SOURCES: dict[str, tuple[type[DataProcessor], type[DataLoader]]] = {
    "apart_sale": (RTMSAptTradeProcessor, SaleTransactionLoader),
    "apart_rent": (RTMSAptRentProcessor, RentTransactionLoader),
    "officetel_sale": (RTMSOffiTradeProcessor, SaleTransactionLoader),
    "officetel_rent": (RTMSOffiRentProcessor, RentTransactionLoader),
    "multiflex_sale": (RTMSRHTradeProcessor, SaleTransactionLoader),
    "multiflex_rent": (RTMSRHRentProcessor, RentTransactionLoader),
    "single_multi_family_sale": (RTMSSHTradeProcessor, SaleTransactionLoader),
    "single_multi_family_rent": (RTMSSHRentProcessor, RentTransactionLoader),
}


class Progress:
    """적재를 마친 파일 목록을 남겨 재실행 시 남은 파일부터 이어가게 한다.

    적재 자체가 갱신 단위로 멱등하므로 이미 처리한 파일을 다시 넣어도 결과는 같다.
    이 기록은 정합성이 아니라 수십 분짜리 작업을 처음부터 다시 돌리지 않기 위한 것이다."""

    def __init__(self, path: Path, *, resume: bool, persist: bool) -> None:
        self._path = path
        self._persist = persist
        self._completed: set[str] = set()
        if resume and path.is_file():
            data = json.loads(path.read_text(encoding="utf-8"))
            self._completed = set(data.get("completed", []))
            logger.info("이전 진행 기록 %d건을 불러왔습니다", len(self._completed))

    def is_done(self, key: str) -> bool:
        return key in self._completed

    def mark_done(self, key: str) -> None:
        self._completed.add(key)
        if self._persist:
            self._save()

    def _save(self) -> None:
        payload = {
            "completed": sorted(self._completed),
            "updated_at": datetime.now(KST).isoformat(),
        }
        # 쓰는 도중 중단돼도 기존 기록이 깨지지 않도록 임시 파일에 쓴 뒤 교체한다.
        tmp_path = self._path.with_suffix(".json.tmp")
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        os.replace(tmp_path, self._path)


def _yyyymm(value: str) -> str:
    if not YYYYMM_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError(f"YYYYMM 형식이어야 합니다: {value!r}")
    return value


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--api",
        dest="api_ids",
        action="append",
        choices=sorted(SOURCES),
        help="적재할 API. 여러 번 지정 가능하며, 생략하면 8종 전부를 적재한다.",
    )
    parser.add_argument(
        "--from",
        dest="from_month",
        type=_yyyymm,
        help="이 계약년월(YYYYMM)부터 적재한다.",
    )
    parser.add_argument(
        "--to",
        dest="to_month",
        type=_yyyymm,
        help="이 계약년월(YYYYMM)까지 적재한다.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="가공까지만 수행하고 적재는 하지 않는다. 진행 기록도 남기지 않는다.",
    )
    parser.add_argument(
        "--restart",
        action="store_true",
        help="이전 진행 기록을 무시하고 처음부터 다시 적재한다.",
    )
    parser.add_argument(
        "--skip-legal-dong-code",
        action="store_true",
        help="법정동코드 마스터 적재를 건너뛴다.",
    )
    parser.add_argument(
        "--no-create-tables",
        dest="create_tables",
        action="store_false",
        help="테이블 생성(CREATE TABLE IF NOT EXISTS)을 건너뛴다.",
    )
    return parser.parse_args()


def _load_items(path: Path) -> list[dict[str, str]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    item = data.get("body", {}).get("items", {}).get("item")
    if item is None:
        return []
    # 결과가 1건이면 리스트가 아니라 단일 객체로 저장돼 있다.
    if isinstance(item, dict):
        return [item]
    return item


def _month_files(api_id: str, args: argparse.Namespace) -> list[Path]:
    source_dir = RESULTS_DIR / api_id
    if not source_dir.is_dir():
        logger.warning("원본 디렉터리가 없습니다: %s", source_dir)
        return []

    paths = []
    for path in sorted(source_dir.glob("*.json")):
        yyyymm = path.stem
        if not YYYYMM_PATTERN.fullmatch(yyyymm):
            continue
        if args.from_month and yyyymm < args.from_month:
            continue
        if args.to_month and yyyymm > args.to_month:
            continue
        paths.append(path)
    return paths


async def _create_tables() -> None:
    # 마이그레이션 도구를 쓰지 않으므로 모델 메타데이터로 직접 테이블을 만든다.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("테이블 생성/확인 완료")


async def _load_legal_standard_codes() -> None:
    if not LEGAL_DONG_CODE_PATH.is_file():
        logger.warning("법정동코드 파일이 없습니다: %s", LEGAL_DONG_CODE_PATH)
        return

    # 수집 단계에서 이미 {code, name} 형태로 가공돼 있어 가공 모듈을 거치지 않는다.
    rows: list[dict[str, Any]] = json.loads(
        LEGAL_DONG_CODE_PATH.read_text(encoding="utf-8")
    )
    loaded = await LegalStandardCodeLoader(async_session_factory).load(rows)
    logger.info("법정동코드 마스터 %d건 적재", loaded)


async def _ingest_file(
    path: Path, processor: DataProcessor, loader: DataLoader, *, dry_run: bool
) -> int:
    items = _load_items(path)
    if not items:
        return 0
    rows = processor.process(items)
    if dry_run:
        return len(rows)
    # 적재 모듈이 (부동산 유형, 시군구, 계약년월) 단위로 기존 행을 지우고 다시 넣으므로
    # 같은 파일을 몇 번 적재해도 결과가 같다.
    return await loader.load(rows)


async def _ingest_api(
    api_id: str, args: argparse.Namespace, progress: Progress, failures: list[str]
) -> int:
    processor_class, loader_class = SOURCES[api_id]
    processor = processor_class()
    loader = loader_class(async_session_factory)

    api_rows = 0
    for path in _month_files(api_id, args):
        key = f"{api_id}/{path.stem}"
        if progress.is_done(key):
            continue

        started = time.monotonic()
        try:
            rows = await _ingest_file(path, processor, loader, dry_run=args.dry_run)
        except Exception:
            logger.exception("적재 실패: %s", key)
            failures.append(key)
            continue

        progress.mark_done(key)
        api_rows += rows
        logger.info(
            "%s: %d행 (%.1fs, %s 누적 %d행)",
            key,
            rows,
            time.monotonic() - started,
            api_id,
            api_rows,
        )

    logger.info("%s 완료: 총 %d행", api_id, api_rows)
    return api_rows


async def run() -> int:
    args = _parse_args()

    if args.create_tables and not args.dry_run:
        await _create_tables()

    if args.skip_legal_dong_code:
        logger.info("법정동코드 마스터 적재를 건너뜁니다")
    elif args.dry_run:
        logger.info("dry-run: 법정동코드 마스터 적재를 건너뜁니다")
    else:
        await _load_legal_standard_codes()

    progress = Progress(
        PROGRESS_PATH,
        # dry-run은 무엇이 적재될지 전부 보여주는 게 목적이라 이전 기록을 따르지 않는다.
        resume=not (args.restart or args.dry_run),
        persist=not args.dry_run,
    )

    failures: list[str] = []
    total_rows = 0
    started = time.monotonic()
    for api_id in args.api_ids or list(SOURCES):
        total_rows += await _ingest_api(api_id, args, progress, failures)

    logger.info(
        "전체 완료: %d행, %.1f분 소요", total_rows, (time.monotonic() - started) / 60
    )
    if failures:
        logger.error("실패한 파일 %d건: %s", len(failures), ", ".join(failures))
        logger.error("다시 실행하면 실패한 파일만 재시도합니다")
    return 1 if failures else 0


async def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    try:
        return await run()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
