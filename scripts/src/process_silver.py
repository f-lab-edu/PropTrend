"""bronze 표(rtms_raw_items)에 쌓인 실거래가 전량을 정제 표로 옮기는 일회성 배치.

load_results.py로 bronze 백필을 마친 뒤 한 번 돌린다. 갱신 파이프라인의 정제 단계(process_unit)를
(유형, 계약년월) 전국 단위로 넓혀 돌린다. 오픈API는 부르지 않는다.

load_results.py와 달리 api 프로젝트의 전처리기·정리기·적재기를 그대로 import한다. bronze 적재는
payload를 가공 없이 넣을 뿐이라 DDL만 맞추면 되지만, 정제는 변환 규칙 자체가 계약이다. 규칙을
두 벌로 두면 백필한 달과 파이프라인이 갱신한 달의 값이 조용히 어긋난다.

    uv run --project api python scripts/src/process_silver.py
"""

import argparse
import asyncio
import logging
import re
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "api"))

# uv run --project api는 api/.env를 자동으로 읽지 않는다. 명시적으로 읽지 않으면
# DATABASE_URL이 src/db.py의 기본값으로 조용히 폴백한다.
load_dotenv(REPO_ROOT / "api" / ".env")

# api 프로젝트를 import 경로에 올린 뒤에야 아래 모듈들을 불러올 수 있다.
from src.db import create_tables, dispose_engine, session_scope  # noqa: E402
from src.jobs.collector import RTMSRawItemCollector  # noqa: E402
from src.jobs.lock import advisory_lock  # noqa: E402
from src.jobs.pipeline import PIPELINES, SPEC_BY_API_ID, PipelineSpec  # noqa: E402
from src.model import RefreshUnitState, RTMSRawItem, UnitStatus  # noqa: E402

logger = logging.getLogger("process_silver")

YYYYMM_PATTERN = re.compile(r"^\d{6}$")

# 갱신 파이프라인이 돌고 있어 시작하지 않았다. 실패(1)와 구분해 다시 돌리면 된다는 뜻이다.
EXIT_LOCK_BUSY = 2


def _yyyymm(value: str) -> str:
    if not YYYYMM_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError(f"YYYYMM 형식이어야 합니다: {value!r}")
    return value


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--api",
        dest="api_ids",
        action="append",
        choices=list(SPEC_BY_API_ID),
        help="정제할 API. 여러 번 지정 가능하며, 생략하면 실거래가 8종을 전부 정제한다.",
    )
    parser.add_argument("--from", dest="from_month", type=_yyyymm, help="이 계약년월(YYYYMM)부터 정제한다.")
    parser.add_argument("--to", dest="to_month", type=_yyyymm, help="이 계약년월(YYYYMM)까지 정제한다.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="bronze를 읽어 가공까지만 하고 정제 표는 건드리지 않는다. 가공 실패를 미리 찾는 용도다.",
    )
    return parser.parse_args()


async def _planned_months(specs: list[PipelineSpec], args: argparse.Namespace) -> dict[str, list[str]]:
    """bronze에 행이 있는 (api_id, deal_ymd)를 API별 월 오름차순으로 모은다."""
    # 파일이 아니라 bronze를 본다. 갱신 파이프라인이 채운 달도 이 표에 함께 있다.
    table = RTMSRawItem.__table__
    statement = (
        select(table.c.api_id, table.c.deal_ymd).where(table.c.api_id.in_([spec.api_id for spec in specs])).distinct()
    )
    if args.from_month:
        statement = statement.where(table.c.deal_ymd >= args.from_month)
    if args.to_month:
        statement = statement.where(table.c.deal_ymd <= args.to_month)

    async with session_scope() as session:
        rows = (await session.execute(statement)).all()

    months: dict[str, list[str]] = {spec.api_id: [] for spec in specs}
    for api_id, deal_ymd in sorted(rows):
        months[api_id].append(deal_ymd)
    return months


async def _sigungu_codes(session: AsyncSession, api_id: str, deal_ymd: str) -> list[str]:
    """그 달 bronze에 있는 시군구. ix_rtms_raw_items_refresh_unit만으로 답이 나온다."""
    table = RTMSRawItem.__table__
    result = await session.execute(
        select(table.c.lawd_cd)
        .where(table.c.api_id == api_id, table.c.deal_ymd == deal_ymd)
        .distinct()
        .order_by(table.c.lawd_cd)
    )
    return list(result.scalars())


async def _clear_collected(session: AsyncSession, api_id: str, deal_ymd: str) -> None:
    """그 달의 '정제 미완' 기록을 지운다. 정제 표가 방금 bronze와 맞춰졌기 때문이다."""
    # FAILED는 bronze부터 커밋되지 않은 단위라 정제로는 풀리지 않는다. 수집 재시도 몫으로 남긴다.
    table = RefreshUnitState.__table__
    await session.execute(
        delete(table).where(
            table.c.api_id == api_id,
            table.c.deal_ymd == deal_ymd,
            table.c.status == UnitStatus.COLLECTED,
        )
    )


async def _process_month(spec: PipelineSpec, deal_ymd: str, *, dry_run: bool) -> tuple[int, int]:
    """(유형, 계약년월) 하나의 전국 bronze를 정제 표로 옮긴다. 전부 들어가거나 전부 롤백된다."""
    preprocessor = spec.build_preprocessor()
    async with session_scope() as session:
        # 시군구를 생략한 전국 삭제다. bronze에 없는 시군구의 정제 행까지 지워야
        # 이 달의 정제 표가 bronze만으로 다시 만든 결과와 같아진다.
        deleted = 0 if dry_run else await spec.cleaner(session).clean(spec.property_type, deal_ymd)

        collector = RTMSRawItemCollector(session, spec.api_id)
        loader = spec.loader(session)
        loaded = 0
        # 전국 한 달치(아파트 전월세는 수십만 행)를 한 번에 읽으면 메모리가 버티지 못한다.
        # 파이프라인과 같은 시군구 단위로 끊어 읽되, 트랜잭션은 달 하나로 묶는다.
        for lawd_cd in await _sigungu_codes(session, spec.api_id, deal_ymd):
            payload = preprocessor.preprocess(await collector.collect(deal_ymd, lawd_cd))
            loaded += len(payload) if dry_run else await loader.load(payload)

        if not dry_run:
            await _clear_collected(session, spec.api_id, deal_ymd)
    return deleted, loaded


async def _process_spec(spec: PipelineSpec, months: list[str], *, dry_run: bool, failures: list[str]) -> int:
    spec_rows = 0
    for deal_ymd in months:
        key = f"{spec.api_id}/{deal_ymd}"
        started = time.monotonic()
        try:
            deleted, loaded = await _process_month(spec, deal_ymd, dry_run=dry_run)
        except Exception:
            # 달 하나는 트랜잭션째 되돌아가므로 나머지를 멈추지 않고 넘어간다.
            logger.exception("정제 실패: %s", key)
            failures.append(key)
            continue

        spec_rows += loaded
        elapsed = time.monotonic() - started
        logger.info(
            "%s: %d행 (기존 %d행 교체, %.1fs, %s행/s, %s 누적 %d행)",
            key,
            loaded,
            deleted,
            elapsed,
            f"{loaded / elapsed:,.0f}" if elapsed > 0 else "-",
            spec.api_id,
            spec_rows,
        )

    logger.info("%s 완료: 총 %d행", spec.api_id, spec_rows)
    return spec_rows


async def _process_all(specs: list[PipelineSpec], args: argparse.Namespace) -> int:
    if not args.dry_run:
        # 정제 표는 API 서버가 기동하면서 만든다. 서버를 띄운 적 없는 DB에서도 돌도록 확인만 한다.
        await create_tables()

    planned = await _planned_months(specs, args)
    logger.info("정제 대상 %d개월(API × 계약년월)", sum(len(months) for months in planned.values()))

    failures: list[str] = []
    total_rows = 0
    started = time.monotonic()
    for spec in specs:
        total_rows += await _process_spec(spec, planned[spec.api_id], dry_run=args.dry_run, failures=failures)

    logger.info("전체 완료: %d행, %.1f분 소요", total_rows, (time.monotonic() - started) / 60)
    if failures:
        logger.error("실패한 달 %d건: %s", len(failures), ", ".join(failures))
        logger.error("달마다 지우고 다시 넣으므로 --api/--from/--to로 그 달만 다시 돌리면 됩니다")
    return 1 if failures else 0


async def run() -> int:
    args = _parse_args()
    specs = [spec for spec in PIPELINES if not args.api_ids or spec.api_id in args.api_ids]

    if args.dry_run:
        return await _process_all(specs, args)

    # 갱신 파이프라인과 같은 락을 잡는다. 같은 달을 동시에 지우고 넣으면 한쪽 결과가 중복되거나 사라진다.
    async with advisory_lock() as acquired:
        if not acquired:
            logger.error("갱신 파이프라인이 돌고 있어 시작하지 않습니다. 끝난 뒤 다시 실행하세요")
            return EXIT_LOCK_BUSY
        return await _process_all(specs, args)


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
