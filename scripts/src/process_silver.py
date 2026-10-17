"""bronze 표(rtms_raw_items)에 쌓인 실거래가 전량을 정제 표로 옮기는 일회성 배치.

load_results.py로 bronze 백필을 마친 뒤 한 번 돌린다. 정기 갱신(airflow/dags/rtms_daily.py)의 정제 단계를
(유형, 계약년월) 전국 단위로 넓혀 돌린다. 오픈API는 부르지 않는다.

전처리 규칙과 유형 목록은 정기 갱신의 것(airflow/dags/rtms/preprocess.py, rtms/apis.py)을 그대로 import한다.
두 벌로 두면 한쪽만 고쳤을 때 백필한 달과 정기 갱신이 다시 만든 달의 값이 조용히 달라진다. 두 모듈은 표준
라이브러리만 써서 Airflow가 없는 api 가상환경에서도 import된다(lint.sh가 확인한다).

같은 달을 동시에 지우고 넣으면 한쪽 결과가 중복되거나 사라지므로, rtms_daily DAG를 일시정지한 뒤 돌린다.

    uv run --project api python scripts/src/process_silver.py
"""

import argparse
import asyncio
import logging
import re
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast

from dotenv import load_dotenv
from sqlalchemy import CursorResult, delete, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "api"))
# DAG 폴더를 올려 정기 갱신의 rtms 패키지를 import한다. Airflow가 DAG 파일을 읽을 때와 같은 기준 경로다.
sys.path.insert(0, str(REPO_ROOT / "airflow" / "dags"))

# uv run --project api는 api/.env를 자동으로 읽지 않는다. 명시적으로 읽지 않으면
# DATABASE_URL이 src/db.py의 기본값으로 조용히 폴백한다.
load_dotenv(REPO_ROOT / "api" / ".env")

# api 프로젝트와 DAG 폴더를 import 경로에 올린 뒤에야 아래 모듈들을 불러올 수 있다.
from rtms.apis import RTMS_APIS  # noqa: E402
from rtms.preprocess import preprocess_rtms  # noqa: E402
from src.db import create_tables, dispose_engine, session_scope  # noqa: E402
from src.model import Base, PropertyType, RentTransaction, RTMSRawItem, SaleTransaction  # noqa: E402

logger = logging.getLogger("process_silver")

YYYYMM_PATTERN = re.compile(r"^\d{6}$")

DECEMBER = 12
CHUNK_SIZE = 1000


@dataclass(frozen=True)
class SilverSpec:
    """실거래가 API 하나를 정제 표로 옮기는 설정을 묶는다."""

    # rtms_raw_items.api_id에 그대로 들어가 있는 슬러그.
    api_id: str
    property_type: PropertyType
    model: type[Base]


# 유형 목록은 정기 갱신의 RTMS_APIS 하나로 둔다. 건물명 필드 같은 유형별 차이는 preprocess_rtms가 거기서 읽는다.
SPECS = tuple(
    SilverSpec(api_id, PropertyType(api.property_type), SaleTransaction if api.is_sale else RentTransaction)
    for api_id, api in RTMS_APIS.items()
)
SPEC_BY_API_ID = {spec.api_id: spec for spec in SPECS}


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


async def _planned_months(specs: list[SilverSpec], args: argparse.Namespace) -> dict[str, list[str]]:
    """bronze에 행이 있는 (api_id, deal_ymd)를 API별 월 오름차순으로 모은다."""
    # 파일이 아니라 bronze를 본다. 정기 갱신이 채운 달도 이 표에 함께 있다.
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


async def _process_month(spec: SilverSpec, deal_ymd: str, *, dry_run: bool) -> tuple[int, int]:
    """(유형, 계약년월) 하나의 전국 bronze를 정제 표로 옮긴다. 전부 들어가거나 전부 롤백된다."""
    bronze = RTMSRawItem.__table__
    silver = spec.model.__table__
    columns = frozenset(column.name for column in silver.columns if column.name not in {"id", "created_at"})
    year, month = int(deal_ymd[:4]), int(deal_ymd[4:])
    start = date(year, month, 1)
    end = date(year + 1, 1, 1) if month == DECEMBER else date(year, month + 1, 1)

    async with session_scope() as session:
        deleted = 0
        if not dry_run:
            # 시군구를 생략한 전국 삭제다. bronze에 없는 시군구의 정제 행까지 지워야
            # 이 달의 정제 표가 bronze만으로 다시 만든 결과와 같아진다.
            result = await session.execute(
                delete(silver).where(
                    silver.c.property_type == spec.property_type,
                    silver.c.deal_date >= start,
                    silver.c.deal_date < end,
                )
            )
            deleted = cast(CursorResult[Any], result).rowcount

        loaded = 0
        # 전국 한 달치(아파트 전월세는 수십만 행)를 한 번에 읽으면 메모리가 버티지 못한다.
        # 정기 갱신과 같은 시군구 단위로 끊어 읽되, 트랜잭션은 달 하나로 묶는다.
        for lawd_cd in await _sigungu_codes(session, spec.api_id, deal_ymd):
            # payload를 펼치지 않고 그대로 넘긴다. 전처리기가 오류 행을 짚으려면 bronze id가
            # payload 밖에 따로 있어야 한다.
            raw_rows = await session.execute(
                select(bronze.c.id, bronze.c.payload).where(
                    bronze.c.api_id == spec.api_id,
                    bronze.c.deal_ymd == deal_ymd,
                    bronze.c.lawd_cd == lawd_cd,
                )
            )
            rows = [preprocess_rtms(spec.api_id, row["id"], row["payload"]) for row in raw_rows.mappings()]
            # preprocess_rtms는 API 하나의 모든 행을 같은 키로 만들므로 첫 행만 검사하면 충분하다.
            if rows and rows[0].keys() != columns:
                raise ValueError(
                    f"정제 row의 키가 {silver.name} 컬럼과 다르다: "
                    f"모르는 키={sorted(rows[0].keys() - columns)}, 빠진 키={sorted(columns - rows[0].keys())}"
                )
            if not dry_run:
                # 모델이 아니라 Table을 넘긴다. 모델을 넘기면 ORM 경로를 타서 느리다.
                for chunk_start in range(0, len(rows), CHUNK_SIZE):
                    await session.execute(insert(silver), rows[chunk_start : chunk_start + CHUNK_SIZE])
            loaded += len(rows)
    return deleted, loaded


async def _process_spec(spec: SilverSpec, months: list[str], *, dry_run: bool, failures: list[str]) -> int:
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


async def run() -> int:
    args = _parse_args()
    specs = [spec for spec in SPECS if not args.api_ids or spec.api_id in args.api_ids]

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
