"""scripts/results에 쌓인 국토교통부 실거래가 원본 JSON을 bronze 표에 적재하는 일회성 배치.

collect_rtms.py가 남긴 월별 원본 응답(item)을 가공 없이 rtms_raw_items에 넣는다. item 하나가
JSONB payload 한 칸에 통째로 들어가 변환 계층이 없고, 8종이 api_id만 바꿔가며 같은 표를 쓴다.
collect_legal_dong_code.py가 남긴 법정동코드 원본은 표와 갱신 방식이 달라 따로 다룬다.

api 프로젝트의 모델과 적재기를 import하지 않는다. 표 정의와 적재를 이 파일 안에 직접 둬서
일회성 백필이 서비스 코드의 리팩터링에 끌려다니지 않게 한다. 대신 DDL이 두 벌이 되므로
api/src/model/raw.py와 api/src/model/load_progress.py가 바뀌면 이 파일도 같이 고쳐야 한다.

sqlalchemy와 asyncpg가 api 쪽 의존성이라 실행만 api 가상환경으로 한다.

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
from functools import cache
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    delete,
    func,
    insert,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

REPO_ROOT = Path(__file__).resolve().parents[2]

# uv run --project api는 api/.env를 자동으로 읽지 않는다. 명시적으로 읽지 않으면
# DATABASE_URL이 아래 기본값으로 조용히 폴백한다.
load_dotenv(REPO_ROOT / "api" / ".env")

logger = logging.getLogger("load_results")

RESULTS_DIR = Path(os.environ.get("RESULTS_DIR", str(REPO_ROOT / "scripts" / "results")))
DEFAULT_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/prop_trend"

YYYYMM_PATTERN = re.compile(r"^\d{6}$")
LAWD_CD_PATTERN = re.compile(r"^\d{5}$")

# 한 번에 보내는 행 수. 42만 행짜리 달을 한 문장으로 보내면 파라미터 한도에 걸린다.
CHUNK_SIZE = 1000

# 충돌 단위를 몇 개까지 나열할지. 전부 찍으면 수백 줄이 나와 안내 문구가 묻힌다.
CONFLICT_SAMPLE = 10

# 결과 디렉터리 이름이자 rtms_raw_items.api_id에 그대로 들어가는 슬러그. api 쪽
# RTMS_KNOWN_FIELDS의 키와 같은 어휘여야 한다. 다르면 갱신 파이프라인의 단위 삭제가
# 이 스크립트가 넣은 행을 영영 못 찾아 매일 중복을 쌓는다.
# legal_dong_code는 여기 없다. 표와 갱신 방식이 달라 _ingest_legal_dong_code가 따로 맡는다.
API_IDS = (
    "apart_sale",
    "apart_rent",
    "officetel_sale",
    "officetel_rent",
    "multiflex_sale",
    "multiflex_rent",
    "single_multi_family_sale",
    "single_multi_family_rent",
)

# 실거래가 8종과 달리 월 단위가 없어 (api_id, yyyymm) 진행 기록에 얹히지 않는다.
LEGAL_DONG_CODE_API_ID = "legal_dong_code"
LEGAL_DONG_CODE_PATH = RESULTS_DIR / "legal_dong_code_raw.json"

METADATA = MetaData()

# api/src/model/raw.py의 RTMSRawItem과 같은 DDL이어야 한다.
RTMS_RAW_ITEMS = Table(
    "rtms_raw_items",
    METADATA,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("api_id", String(40), nullable=False),
    Column("lawd_cd", String(5), nullable=False),
    Column("deal_ymd", String(6), nullable=False),
    Column("collected_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column("payload", JSONB, nullable=False),
    Index("ix_rtms_raw_items_refresh_unit", "api_id", "deal_ymd", "lawd_cd"),
)

# api/src/model/raw.py의 LegalDongCodeRawItem과 같은 DDL이어야 한다.
LEGAL_DONG_CODE_RAW_ITEMS = Table(
    "legal_dong_code_raw_items",
    METADATA,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("collected_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
    Column("payload", JSONB, nullable=False),
)

# api/src/model/load_progress.py의 RawLoadProgress와 같은 DDL이어야 한다.
RAW_LOAD_PROGRESS = Table(
    "raw_load_progress",
    METADATA,
    Column("api_id", String(40), primary_key=True),
    Column("yyyymm", String(6), primary_key=True),
    Column("row_count", Integer, nullable=False),
    Column("loaded_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
)


@cache
def _get_engine() -> AsyncEngine:
    """엔진은 프로세스당 하나다. 인자가 없어 cache가 곧 싱글턴이 된다."""
    return create_async_engine(os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL), pool_pre_ping=True)


async def _dispose_engine() -> None:
    # 엔진을 한 번도 만들지 않고 끝난 실행(인자 오류 등)에서 풀을 만들지 않으려고 캐시를 먼저 본다.
    if _get_engine.cache_info().currsize:
        await _get_engine().dispose()
        _get_engine.cache_clear()


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
        choices=sorted([*API_IDS, LEGAL_DONG_CODE_API_ID]),
        help="적재할 API. 여러 번 지정 가능하며, 생략하면 실거래가 8종과 법정동코드를 전부 적재한다.",
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
        help="적재 대상 api_id의 bronze 행과 진행 기록을 지운 뒤 처음부터 적재한다(--from/--to 범위 한정).",
    )
    parser.add_argument(
        "--no-create-tables",
        dest="create_tables",
        action="store_false",
        help="테이블 생성(CREATE TABLE IF NOT EXISTS)을 건너뛴다.",
    )
    return parser.parse_args()


def _load_items(path: Path) -> list[dict[str, Any]]:
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


def _to_rows(api_id: str, deal_ymd: str, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """응답 item을 bronze 행으로 바꾼다.

    갱신 단위 키(lawd_cd)는 원래 요청 파라미터에서 오지만, 수집기가 한 달치 지역을 한 파일로
    합치면서 파라미터를 버렸다. 남은 출처가 payload의 sggCd뿐이라 거기서 되찾는다.

    sggCd가 없거나 5자리가 아니면 그 파일을 통째로 실패시킨다. 임의의 값으로 채우면 갱신
    파이프라인의 단위 삭제가 그 행을 못 찾아 매일 중복이 쌓인다. 조용히 버려도 같은 달을
    다시 받을 때 빠진 만큼이 아니라 전부가 다시 들어온다.
    """
    rows = []
    broken = 0
    for item in items:
        lawd_cd = str(item.get("sggCd", "")).strip()
        if not LAWD_CD_PATTERN.fullmatch(lawd_cd):
            broken += 1
            continue
        # collected_at은 server_default에 맡기려고 키 자체를 넣지 않는다.
        rows.append({"api_id": api_id, "lawd_cd": lawd_cd, "deal_ymd": deal_ymd, "payload": item})

    if broken:
        raise ValueError(f"sggCd가 없거나 5자리가 아닌 item {broken}건 (전체 {len(items)}건)")
    return rows


async def _insert_chunked(conn: AsyncConnection, table: Table, rows: list[dict[str, Any]]) -> int:
    for start in range(0, len(rows), CHUNK_SIZE):
        await conn.execute(insert(table), rows[start : start + CHUNK_SIZE])
    return len(rows)


def _is_sigungu(row: dict[str, Any]) -> bool:
    """시군구 단위(SSGGG00000) 행인지 판별한다.

    api/src/jobs/collector.py의 LegalDongCodeCollector._is_sigungu와 같은 규칙이다. 갱신
    파이프라인이 이 표에 시군구 행만 넣고 sigungu_codes()가 그것을 LAWD_CD 목록으로 읽으므로,
    원본 파일에 함께 들어 있는 읍면동·리까지 넣으면 다음 수집이 없는 지역을 긁는다.
    """
    return row.get("sgg_cd") != "000" and row.get("umd_cd") == "000" and row.get("ri_cd") == "00"


async def _ingest_legal_dong_code(*, dry_run: bool) -> int:
    """법정동코드 bronze 표를 통째로 갈아끼운다.

    월 단위가 없어 진행 기록에 얹히지 않는 대신 늘 비우고 다시 채운다. 몇 번을 돌려도 결과가
    같아 이어갈 지점이 필요 없다. api/src/jobs/pipeline.py의 refresh_legal_dong_codes와 같다.
    """
    if not LEGAL_DONG_CODE_PATH.is_file():
        logger.warning("법정동코드 원본이 없습니다. 건너뜁니다: %s", LEGAL_DONG_CODE_PATH)
        return 0

    items = _load_items(LEGAL_DONG_CODE_PATH)
    rows = [{"payload": row} for row in items if _is_sigungu(row)]
    if not rows:
        # 비우고 채우는 구조라 빈 목록을 정상으로 받으면 시군구 목록이 그대로 사라진다.
        raise ValueError(f"시군구 행이 없습니다: {LEGAL_DONG_CODE_PATH} (전체 {len(items)}행)")

    if dry_run:
        return len(rows)

    # 비우기와 채우기가 한 트랜잭션이어야 한다. 사이에서 끊기면 시군구 목록이 사라진다.
    async with _get_engine().begin() as conn:
        deleted = (await conn.execute(delete(LEGAL_DONG_CODE_RAW_ITEMS))).rowcount
        loaded = await _insert_chunked(conn, LEGAL_DONG_CODE_RAW_ITEMS, rows)

    logger.info("%s: %d행 (원본 %d행 중 시군구, 기존 %d행 교체)", LEGAL_DONG_CODE_API_ID, loaded, len(items), deleted)
    return loaded


async def _completed_keys(api_ids: list[str]) -> set[tuple[str, str]]:
    """이미 적재를 마친 (api_id, yyyymm)을 한 번에 읽어둔다."""
    async with _get_engine().connect() as conn:
        result = await conn.execute(
            select(RAW_LOAD_PROGRESS.c.api_id, RAW_LOAD_PROGRESS.c.yyyymm).where(
                RAW_LOAD_PROGRESS.c.api_id.in_(api_ids)
            )
        )
        return {(row.api_id, row.yyyymm) for row in result}


async def _stored_units(api_ids: list[str]) -> set[tuple[str, str]]:
    """bronze에 이미 행이 있는 (api_id, deal_ymd). ix_rtms_raw_items_refresh_unit을 탄다."""
    async with _get_engine().connect() as conn:
        result = await conn.execute(
            select(RTMS_RAW_ITEMS.c.api_id, RTMS_RAW_ITEMS.c.deal_ymd)
            .where(RTMS_RAW_ITEMS.c.api_id.in_(api_ids))
            .distinct()
        )
        return {(row.api_id, row.deal_ymd) for row in result}


async def _create_tables() -> None:
    # 마이그레이션 도구를 쓰지 않으므로 이 파일의 메타데이터로 직접 테이블을 만든다.
    async with _get_engine().begin() as conn:
        await conn.run_sync(METADATA.create_all)
    logger.info("테이블 생성/확인 완료")


async def _clear(api_ids: list[str], args: argparse.Namespace) -> None:
    """적재 대상 구간의 bronze 행과 진행 기록을 지운다.

    8종이 rtms_raw_items 하나를 나눠 쓰므로 TRUNCATE를 쓸 수 없다. 고르지 않은 API의 행까지
    날아간다. api_id와 월 범위로 좁힌 DELETE만 쓴다. 범위를 좁히지 않으면 --from/--to로
    일부만 다시 넣는 실행이 범위 밖 데이터를 지워버린다.
    """
    rows_stmt = delete(RTMS_RAW_ITEMS).where(RTMS_RAW_ITEMS.c.api_id.in_(api_ids))
    progress_stmt = delete(RAW_LOAD_PROGRESS).where(RAW_LOAD_PROGRESS.c.api_id.in_(api_ids))
    if args.from_month:
        rows_stmt = rows_stmt.where(RTMS_RAW_ITEMS.c.deal_ymd >= args.from_month)
        progress_stmt = progress_stmt.where(RAW_LOAD_PROGRESS.c.yyyymm >= args.from_month)
    if args.to_month:
        rows_stmt = rows_stmt.where(RTMS_RAW_ITEMS.c.deal_ymd <= args.to_month)
        progress_stmt = progress_stmt.where(RAW_LOAD_PROGRESS.c.yyyymm <= args.to_month)

    logger.warning("다음 API의 bronze 행을 지웁니다: %s", ", ".join(api_ids))
    async with _get_engine().begin() as conn:
        deleted = (await conn.execute(rows_stmt)).rowcount
        await conn.execute(progress_stmt)

    logger.info("bronze %d행과 해당 구간의 진행 기록을 삭제했습니다", deleted)


async def _ingest_file(path: Path, api_id: str, *, dry_run: bool) -> int:
    rows = _to_rows(api_id, path.stem, _load_items(path))
    if dry_run:
        return len(rows)

    # 적재와 완료 기록이 한 트랜잭션이다. 커밋 뒤 기록 전에 죽는 창이 없어야 한다.
    # bronze 표에는 unique 제약이 없어, 그 틈에 죽으면 재실행이 파일을 통째로 중복시킨다.
    async with _get_engine().begin() as conn:
        loaded = await _insert_chunked(conn, RTMS_RAW_ITEMS, rows)
        await conn.execute(
            pg_insert(RAW_LOAD_PROGRESS)
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
    api_rows = 0
    for path in _month_files(api_id, args):
        key = f"{api_id}/{path.stem}"
        if (api_id, path.stem) in completed:
            continue

        started = time.monotonic()
        try:
            rows = await _ingest_file(path, api_id, dry_run=args.dry_run)
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


def _report_conflicts(conflicts: set[tuple[str, str]]) -> None:
    logger.error("진행 기록에 없는데 bronze에 이미 행이 있는 단위가 %d개 있습니다:", len(conflicts))
    for api_id, deal_ymd in sorted(conflicts)[:CONFLICT_SAMPLE]:
        logger.error("  %s/%s", api_id, deal_ymd)
    if len(conflicts) > CONFLICT_SAMPLE:
        logger.error("  ... 외 %d개", len(conflicts) - CONFLICT_SAMPLE)
    logger.error("그대로 적재하면 같은 달을 두 번 넣어 행이 통째로 중복됩니다.")
    logger.error("갱신 파이프라인이 이미 채운 달이면 --from/--to로 그 달을 빼고 적재하세요.")
    logger.error("그 행을 지우고 다시 넣으려면 --truncate를, 중복을 감수하려면 --restart를 쓰세요.")


async def _conflicting_units(args: argparse.Namespace, api_ids: list[str]) -> set[tuple[str, str]]:
    """이번에 넣으려는 단위 중 bronze에 이미 행이 있는데 진행 기록에는 없는 것을 찾는다.

    갱신 파이프라인이 최근 달을 이미 채워둔 상태에서 백필을 돌리면 그 달이 그대로 두 배가 된다.
    진행 기록만으로는 못 막는다. 그 행을 넣은 건 이 스크립트가 아니라 파이프라인이기 때문이다.
    """
    planned = {(api_id, path.stem) for api_id in api_ids for path in _month_files(api_id, args)}
    if not planned:
        return set()
    return (planned & await _stored_units(api_ids)) - await _completed_keys(api_ids)


async def _prepare(args: argparse.Namespace, api_ids: list[str]) -> int | None:
    """적재 전 준비를 끝낸다. 적재를 시작하면 안 되는 상태면 종료 코드를 돌려준다."""
    if args.create_tables and not args.dry_run:
        await _create_tables()

    if args.truncate and not args.dry_run and api_ids:
        await _clear(api_ids, args)
    elif args.truncate:
        logger.info("dry-run: 삭제를 건너뜁니다")

    if args.restart and not args.truncate:
        logger.warning("--restart는 기존 행을 지우지 않습니다. 이미 적재된 파일을 다시 넣으면 행이 중복됩니다")
        logger.warning("처음부터 깨끗이 다시 넣으려면 --truncate를 쓰세요")

    # --truncate는 방금 지웠고, --restart는 중복을 감수하겠다는 선언이라 검사하지 않는다.
    skip_check = args.dry_run or args.truncate or args.restart
    if not skip_check and (conflicts := await _conflicting_units(args, api_ids)):
        _report_conflicts(conflicts)
        return 2

    return None


async def _resume_point(args: argparse.Namespace, api_ids: list[str]) -> set[tuple[str, str]]:
    """이어갈 지점을 정한다.

    dry-run은 무엇이 적재될지 전부 보여주는 게 목적이라 이전 기록을 따르지 않는다.
    --restart는 기록을 지우지 않고 이번 실행에서만 무시한다. 지워버리면 중간에 죽었을 때
    고르지 않은 API의 기록까지 사라진다.
    """
    if args.restart or args.dry_run:
        return set()

    completed = await _completed_keys(api_ids)
    logger.info("이전 진행 기록 %d건을 불러왔습니다", len(completed))
    return completed


async def run() -> int:
    args = _parse_args()
    selected = args.api_ids or [*API_IDS, LEGAL_DONG_CODE_API_ID]
    api_ids = [api_id for api_id in selected if api_id != LEGAL_DONG_CODE_API_ID]

    exit_code = await _prepare(args, api_ids)
    if exit_code is not None:
        return exit_code

    failures: list[str] = []
    total_rows = 0
    started = time.monotonic()

    # 실거래가보다 먼저 넣는다. 이 표가 다음 수집의 시군구 목록이라 파이프라인도 같은 순서다.
    if LEGAL_DONG_CODE_API_ID in selected:
        try:
            total_rows += await _ingest_legal_dong_code(dry_run=args.dry_run)
        except Exception:
            logger.exception("적재 실패: %s", LEGAL_DONG_CODE_API_ID)
            failures.append(LEGAL_DONG_CODE_API_ID)

    completed = await _resume_point(args, api_ids) if api_ids else set()
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
        await _dispose_engine()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
