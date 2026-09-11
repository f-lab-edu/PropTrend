"""scripts/results에 쌓인 국토교통부 실거래가 원본 JSON을 raw 테이블에 적재하는 일회성 배치.

collect_rtms.py가 남긴 월별 원본 응답(item)을 가공 없이 그대로 넣는다. raw 테이블은
응답 필드명을 그대로 컬럼명으로 쓰므로 변환 계층이 필요 없고, api/src/jobs/loader.py의
RawDataLoader 하나를 8종이 모델만 바꿔가며 공유한다.

적재 전 테이블이 없으면 만들고, 파일 단위로 진행 상황을 기록해 중단된 지점부터 다시
이어갈 수 있다.

적재 모듈과 모델이 api 프로젝트에 있으므로 api 가상환경으로 실행한다.

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

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "api"))

# api 프로젝트를 import 경로에 올린 뒤에야 아래 모듈들을 불러올 수 있다.

from dotenv import load_dotenv

# uv run --project api는 api/.env를 자동으로 읽지 않는다. 명시적으로 읽지 않으면
# DATABASE_URL이 src/db.py의 기본값으로 조용히 폴백한다.
load_dotenv(REPO_ROOT / "api" / ".env")

from sqlalchemy import text
from src.db import dispose_engine, get_engine, session_scope
from src.jobs.loader import RawDataLoader
from src.model import Base
from src.model.raw import (
    RawApartRent,
    RawApartSale,
    RawMultiflexRent,
    RawMultiflexSale,
    RawOfficetelRent,
    RawOfficetelSale,
    RawSingleMultiFamilyRent,
    RawSingleMultiFamilySale,
)

logger = logging.getLogger("load_results")

RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", str(REPO_ROOT / "scripts" / "results")))
# 옛 파이프라인(가공 후 sale/rent_transactions 적재)의 기록이 load_results.progress.json에
# 남아 있으므로 섞이지 않게 파일을 분리한다.
PROGRESS_PATH = RESULTS_DIR / "load_raw.progress.json"

YYYYMM_PATTERN = re.compile(r"^\d{6}$")
KST = timezone(timedelta(hours=9))

# api_id -> raw 모델. api_id는 collect_rtms.py의 것이자 결과 디렉터리 이름 그대로다.
# RawLegalDongCode는 제외한다. results/legal_dong_code.json이 수집 단계에서 이미
# {code, name}으로 축약돼 있어 13개 컬럼과 맞지 않는다(수집기 수정 후 별도 적재 필요).
SOURCES: dict[str, type[Base]] = {
    "apart_sale": RawApartSale,
    "apart_rent": RawApartRent,
    "officetel_sale": RawOfficetelSale,
    "officetel_rent": RawOfficetelRent,
    "multiflex_sale": RawMultiflexSale,
    "multiflex_rent": RawMultiflexRent,
    "single_multi_family_sale": RawSingleMultiFamilySale,
    "single_multi_family_rent": RawSingleMultiFamilyRent,
}


class Progress:
    """적재를 마친 파일 목록을 남겨 재실행 시 남은 파일부터 이어가게 한다.

    raw 테이블에는 id PK 말고 unique 제약이 없어 같은 파일을 두 번 넣으면 행이 그대로
    중복된다. 그래서 이 기록은 편의가 아니라 정합성을 책임진다. 파일 하나가 트랜잭션
    하나이고 commit이 끝난 뒤에만 여기에 기록하므로, 중간에 죽어도 '절반만 적재된 파일'이
    남지 않는다."""

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
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
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
        help="파일을 읽어 행수만 세고 적재는 하지 않는다. 진행 기록도 남기지 않는다.",
    )
    parser.add_argument(
        "--restart",
        action="store_true",
        help="이전 진행 기록을 무시하고 처음부터 다시 적재한다.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="적재 대상 raw 테이블을 비우고 진행 기록도 지운 뒤 처음부터 적재한다.",
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
    # 결과가 1건이면 리스트가 아니라 단일 객체로 저장돼 있을 수 있다.
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
    async with get_engine().begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("테이블 생성/확인 완료")


async def _truncate(api_ids: list[str]) -> None:
    tables = [SOURCES[api_id].__tablename__ for api_id in api_ids]
    logger.warning("다음 테이블을 비웁니다: %s", ", ".join(tables))
    quoted = ", ".join(f'"{table}"' for table in tables)
    async with get_engine().begin() as conn:
        await conn.execute(text(f"TRUNCATE TABLE {quoted} RESTART IDENTITY"))
    PROGRESS_PATH.unlink(missing_ok=True)
    logger.info("테이블 %d개를 비우고 진행 기록을 삭제했습니다", len(tables))


async def _ingest_file(path: Path, model: type[Base], *, dry_run: bool) -> int:
    items = _load_items(path)
    if not items or dry_run:
        return len(items)
    # 파일 하나가 트랜잭션 하나다. 중간에 죽으면 통째로 롤백되므로 부분 적재가 남지 않는다.
    async with session_scope() as session:
        return await RawDataLoader(session, model).load(items)


async def _ingest_api(api_id: str, args: argparse.Namespace, progress: Progress, failures: list[str]) -> int:
    model = SOURCES[api_id]

    api_rows = 0
    for path in _month_files(api_id, args):
        key = f"{api_id}/{path.stem}"
        if progress.is_done(key):
            continue

        started = time.monotonic()
        try:
            rows = await _ingest_file(path, model, dry_run=args.dry_run)
        except Exception:
            logger.exception("적재 실패: %s", key)
            failures.append(key)
            continue

        progress.mark_done(key)
        api_rows += rows
        elapsed = time.monotonic() - started
        logger.info(
            "%s: %d행 (%.1fs, %s행/s, %s 누적 %d행)",
            key,
            rows,
            elapsed,
            f"{rows / elapsed:,.0f}" if elapsed > 0 else "-",
            api_id,
            api_rows,
        )

    logger.info("%s 완료: 총 %d행", api_id, api_rows)
    return api_rows


async def run() -> int:
    args = _parse_args()
    api_ids = args.api_ids or list(SOURCES)

    if args.create_tables and not args.dry_run:
        await _create_tables()

    if args.truncate and not args.dry_run:
        await _truncate(api_ids)
    elif args.truncate:
        logger.info("dry-run: TRUNCATE를 건너뜁니다")

    progress = Progress(
        PROGRESS_PATH,
        # dry-run은 무엇이 적재될지 전부 보여주는 게 목적이라 이전 기록을 따르지 않는다.
        resume=not (args.restart or args.truncate or args.dry_run),
        persist=not args.dry_run,
    )

    failures: list[str] = []
    total_rows = 0
    started = time.monotonic()
    for api_id in api_ids:
        total_rows += await _ingest_api(api_id, args, progress, failures)

    logger.info("전체 완료: %d행, %.1f분 소요", total_rows, (time.monotonic() - started) / 60)
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
        await dispose_engine()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
