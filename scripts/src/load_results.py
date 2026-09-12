"""scripts/results에 쌓인 국토교통부 실거래가 원본 JSON을 raw 테이블에 적재하는 일회성 배치.

collect_rtms.py가 남긴 월별 원본 응답(item)을 가공 없이 그대로 넣는다. raw 테이블은
응답 필드명을 그대로 컬럼명으로 쓰므로 변환 계층이 필요 없고, api/src/jobs/loader.py의
RawDataLoader 하나를 8종이 모델만 바꿔가며 공유한다.

적재 전 테이블이 없으면 만들고, 적재를 마친 파일을 raw_load_progress 표에 같은
트랜잭션으로 기록해 중단된 지점부터 다시 이어갈 수 있다.

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
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "api"))

# api 프로젝트를 import 경로에 올린 뒤에야 아래 모듈들을 불러올 수 있다.

from dotenv import load_dotenv

# uv run --project api는 api/.env를 자동으로 읽지 않는다. 명시적으로 읽지 않으면
# DATABASE_URL이 src/db.py의 기본값으로 조용히 폴백한다.
load_dotenv(REPO_ROOT / "api" / ".env")

from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from src.db import dispose_engine, get_engine, session_scope
from src.jobs.loader import RawDataLoader
from src.model import Base, RawLoadProgress
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
# 진행 기록은 raw_load_progress 표가 들고 있다. 이 파일은 표로 옮기기 전의 기록이며
# --import-legacy-progress로 한 번 옮기고 나면 더 읽지 않는다.
LEGACY_PROGRESS_PATH = RESULTS_DIR / "load_raw.progress.json"

YYYYMM_PATTERN = re.compile(r"^\d{6}$")

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


async def _completed_keys(api_ids: list[str]) -> set[tuple[str, str]]:
    """이미 적재를 마친 (api_id, yyyymm)을 한 번에 읽어둔다."""
    async with session_scope() as session:
        result = await session.execute(
            select(RawLoadProgress.api_id, RawLoadProgress.yyyymm).where(RawLoadProgress.api_id.in_(api_ids))
        )
        return set(result.all())


async def _legacy_progress_unmigrated(api_ids: list[str]) -> bool:
    """옛 기록 파일은 남아 있는데 표는 비어 있는, 사람이 판단해야 하는 상태인지 본다.

    자동으로 옮기지 않는다. 그 파일이 지금 DATABASE_URL이 가리키는 DB의 기록이라는
    보장이 없어서, 빈 DB에 붙은 채 옮기면 전부 완료로 찍혀 영영 비어 있게 된다.
    """
    if not LEGACY_PROGRESS_PATH.is_file():
        return False
    return not await _completed_keys(api_ids)


async def _import_legacy_progress() -> int:
    """파일로 남아 있던 옛 진행 기록을 표로 옮긴다. 옮긴 뒤에는 이 파일을 읽지 않는다."""
    if not LEGACY_PROGRESS_PATH.is_file():
        logger.warning("옮길 진행 기록 파일이 없습니다: %s", LEGACY_PROGRESS_PATH)
        return 0

    data = json.loads(LEGACY_PROGRESS_PATH.read_text(encoding="utf-8"))
    rows = []
    for key in data.get("completed", []):
        api_id, _, yyyymm = key.partition("/")
        if api_id in SOURCES and YYYYMM_PATTERN.fullmatch(yyyymm):
            # row_count는 파일 기록에 없다. 재개 판단에는 키만 쓰이므로 0으로 둔다.
            rows.append({"api_id": api_id, "yyyymm": yyyymm, "row_count": 0})

    async with session_scope() as session:
        await session.execute(pg_insert(RawLoadProgress).on_conflict_do_nothing(), rows)

    logger.info("진행 기록 %d건을 %s 표로 옮겼습니다", len(rows), RawLoadProgress.__tablename__)
    logger.info("이제 %s는 쓰이지 않습니다. 지워도 됩니다.", LEGACY_PROGRESS_PATH)
    return len(rows)


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
        help="이번 실행에서만 진행 기록을 무시하고 전부 다시 적재한다. 기록은 지우지 않는다.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="적재 대상 raw 테이블과 해당 API의 진행 기록을 비운 뒤 처음부터 적재한다.",
    )
    parser.add_argument(
        "--no-create-tables",
        dest="create_tables",
        action="store_false",
        help="테이블 생성(CREATE TABLE IF NOT EXISTS)을 건너뛴다.",
    )
    parser.add_argument(
        "--import-legacy-progress",
        action="store_true",
        help="파일로 남은 옛 진행 기록을 raw_load_progress 표로 옮기고 적재 없이 끝낸다.",
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
    tables = [SOURCES[api_id].__table__ for api_id in api_ids]
    logger.warning("다음 테이블을 비웁니다: %s", ", ".join(table.name for table in tables))

    async with get_engine().begin() as conn:
        # 테이블 이름을 직접 문자열로 잇지 않고 방언의 식별자 인용기를 쓴다. 값의 출처가
        # 리터럴 SOURCES이고 --api가 argparse choices로 막혀 있어 주입 경로는 없지만,
        # 정적 분석이 SQL 조립으로 보는 형태다(collect_rtms.py의 S2083 대응과 같은 이유).
        preparer = conn.dialect.identifier_preparer
        quoted = ", ".join(preparer.format_table(table) for table in tables)
        await conn.execute(text(f"TRUNCATE TABLE {quoted} RESTART IDENTITY"))
        # 비운 api_id의 기록만 지운다. 함께 지우지 않으면 비운 테이블을 다시 채우지 않고,
        # 전부 지우면 비우지도 않은 API를 재적재해 행이 중복된다.
        await conn.execute(delete(RawLoadProgress).where(RawLoadProgress.api_id.in_(api_ids)))

    logger.info("테이블 %d개를 비우고 해당 API의 진행 기록을 삭제했습니다", len(tables))


async def _ingest_file(path: Path, api_id: str, model: type[Base], *, dry_run: bool) -> int:
    items = _load_items(path)
    if dry_run:
        return len(items)

    # 적재와 완료 기록이 한 트랜잭션이다. 커밋 뒤 기록 전에 죽는 창이 없어야 한다.
    # raw 테이블에는 unique 제약이 없어, 그 틈에 죽으면 재실행이 파일을 통째로 중복시킨다.
    async with session_scope() as session:
        loaded = await RawDataLoader(session, model).load(items)
        await session.execute(
            pg_insert(RawLoadProgress)
            .values(api_id=api_id, yyyymm=path.stem, row_count=loaded)
            # --restart는 기록을 지우지 않고 무시만 하므로 같은 키가 다시 올 수 있다.
            .on_conflict_do_update(
                index_elements=["api_id", "yyyymm"],
                set_={"row_count": loaded, "loaded_at": func.now()},
            )
        )
        return loaded


async def _ingest_api(
    api_id: str,
    args: argparse.Namespace,
    completed: set[tuple[str, str]],
    failures: list[str],
) -> int:
    model = SOURCES[api_id]

    api_rows = 0
    for path in _month_files(api_id, args):
        key = f"{api_id}/{path.stem}"
        if (api_id, path.stem) in completed:
            continue

        started = time.monotonic()
        try:
            rows = await _ingest_file(path, api_id, model, dry_run=args.dry_run)
        except Exception:
            logger.exception("적재 실패: %s", key)
            failures.append(key)
            continue

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

    if args.import_legacy_progress:
        await _import_legacy_progress()
        return 0

    if not args.dry_run and await _legacy_progress_unmigrated(api_ids):
        logger.error("옛 진행 기록 파일이 남아 있는데 %s 표는 비어 있습니다:", RawLoadProgress.__tablename__)
        logger.error("  %s", LEGACY_PROGRESS_PATH)
        logger.error("그대로 적재하면 이미 들어간 파일을 다시 넣어 행이 통째로 중복됩니다.")
        logger.error("이 DB가 그 기록의 대상이면 --import-legacy-progress로 기록을 먼저 옮기세요.")
        logger.error("빈 DB에 처음부터 넣는 것이면 --truncate를 쓰세요.")
        return 2

    if args.truncate and not args.dry_run:
        await _truncate(api_ids)
    elif args.truncate:
        logger.info("dry-run: TRUNCATE를 건너뜁니다")

    if args.restart and not args.truncate:
        logger.warning("--restart는 기존 행을 지우지 않습니다. 이미 적재된 파일을 다시 넣으면 행이 중복됩니다")
        logger.warning("처음부터 깨끗이 다시 넣으려면 --truncate를 쓰세요")

    # dry-run은 무엇이 적재될지 전부 보여주는 게 목적이라 이전 기록을 따르지 않는다.
    # --restart는 기록을 지우지 않고 이번 실행에서만 무시한다. 지워버리면 중간에 죽었을 때
    # 고르지 않은 API의 기록까지 사라진다.
    completed: set[tuple[str, str]] = set()
    if not (args.restart or args.dry_run):
        completed = await _completed_keys(api_ids)
        logger.info("이전 진행 기록 %d건을 불러왔습니다", len(completed))

    failures: list[str] = []
    total_rows = 0
    started = time.monotonic()
    for api_id in api_ids:
        total_rows += await _ingest_api(api_id, args, completed, failures)

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
