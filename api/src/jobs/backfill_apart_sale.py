"""아파트 매매 bronze·정제 테이블을 상세 자료 API(RTMSDataSvcAptTradeDev) 응답으로 갈아끼우는 임시 배치.

기존 API로 적재한 단위에는 단지 일련번호(aptSeq)가 없다. 일일 호출 한도 때문에 한 번에 바꾸지 못하므로
최신 월부터 한 단위씩 갈아끼우다가 일일 제한이든 다른 오류든 처음 실패한 자리에서 멈춘다.
마지막으로 성공한 단위를 PROGRESS_PATH에 남기고, 다음 실행은 그 다음 단위부터 이어간다.
남은 단위가 0이 되면 이 파일과 진행 기록을 지운다.

    python -m src.jobs.backfill_apart_sale --limit 10000
"""

import argparse
import asyncio
import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import and_, func, or_, select

from ..db import dispose_engine, session_scope
from ..logging_config import configure_logging
from ..model import RTMSRawItem
from .__main__ import EXIT_FAILED, EXIT_OK, bounded_int
from .context import UnitContextFilter, unit_context
from .lock import advisory_lock
from .pipeline import SPEC_BY_API_ID, refresh_unit
from .utils import parse_deal_ymd, split_sgg_cd

logger = logging.getLogger(__name__)

API_ID = "apart_sale"

# 상세 자료 API의 일일 한도. 단위 하나가 대부분 1회 호출(페이지당 10,000건)이라 단위 수로 센다.
DEFAULT_LIMIT = 10000

# results/는 .gitignore 대상이다. 실행 위치와 상관없이 api/results에 남긴다.
PROGRESS_PATH = Path(__file__).resolve().parents[2] / "results" / "backfill_apart_sale.json"


def read_progress() -> dict[str, str] | None:
    """마지막으로 성공한 단위의 좌표를 읽는다. 처음 실행이면 None."""
    if not PROGRESS_PATH.exists():
        return None
    last = json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))["last_collected"]
    # 진행 기록은 손으로 고칠 수 있는 파일이다. 형식을 확인한 뒤 숫자에서 다시 만들어
    # 파일 원문이 SQL 비교·로그·다음 진행 기록에 그대로 흘러가지 않게 한다.
    split_sgg_cd(last["lawd_cd"])
    parse_deal_ymd(last["deal_ymd"])
    return {"lawd_cd": f"{int(last['lawd_cd']):05d}", "deal_ymd": f"{int(last['deal_ymd']):06d}"}


def write_progress(lawd_cd: str, deal_ymd: str, collected: int, stopped_reason: str | None = None) -> None:
    """마지막 성공 단위를 남긴다. 쓰는 도중 죽어도 이전 기록이 깨지지 않도록 임시 파일을 바꿔 끼운다."""
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "last_collected": {"api_id": API_ID, "lawd_cd": lawd_cd, "deal_ymd": deal_ymd},
        "collected_in_last_run": collected,
        "stopped_reason": stopped_reason,
        "updated_at": datetime.now(UTC).isoformat(),
    }
    temp_path = PROGRESS_PATH.with_suffix(".tmp")
    temp_path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(PROGRESS_PATH)


async def pending_units(limit: int, after: dict[str, str] | None) -> tuple[int, list[tuple[str, str]]]:
    """aptSeq 없이 적재된 단위 중 `after` 다음부터 최신 월 순으로 `limit`개와, 그 범위의 전체 수."""
    table = RTMSRawItem.__table__
    conditions = [table.c.api_id == API_ID]
    if after is not None:
        # 처리 순서(deal_ymd 내림차순, lawd_cd 오름차순)에서 마지막 성공 단위 뒤만 남긴다.
        conditions.append(
            or_(
                table.c.deal_ymd < after["deal_ymd"],
                and_(table.c.deal_ymd == after["deal_ymd"], table.c.lawd_cd > after["lawd_cd"]),
            )
        )
    statement = (
        select(table.c.lawd_cd, table.c.deal_ymd)
        .where(*conditions)
        .group_by(table.c.lawd_cd, table.c.deal_ymd)
        # 한 행이라도 aptSeq가 있으면 이미 상세 자료 API로 받은 단위다.
        .having(func.bool_or(table.c.payload.has_key("aptSeq")).is_(False))
        .subquery()
    )
    async with session_scope() as session:
        total = await session.scalar(select(func.count()).select_from(statement))
        result = await session.execute(
            select(statement.c.lawd_cd, statement.c.deal_ymd)
            .order_by(statement.c.deal_ymd.desc(), statement.c.lawd_cd)
            .limit(limit)
        )
        return total, [(row.lawd_cd, row.deal_ymd) for row in result]


async def backfill(coordinates: list[tuple[str, str]]) -> tuple[int, str | None]:
    """순서대로 한 단위씩 갈아끼운다. 성공한 단위 수와, 중간에 멈췄다면 그 사유를 돌려준다."""
    spec = SPEC_BY_API_ID[API_ID]
    collected = 0
    for lawd_cd, deal_ymd in coordinates:
        with unit_context(spec.api_id, lawd_cd, deal_ymd):
            started = time.perf_counter()
            try:
                result = await refresh_unit(spec, lawd_cd, deal_ymd)
            except Exception as error:
                # 일일 제한이면 남은 단위도 모두 같은 응답을 받는다. 다른 오류도 원인을 보기 전엔 넘기지 않는다.
                # 진행 기록은 마지막 성공 단위에 머물러 있으므로 다음 실행이 이 단위부터 다시 시도한다.
                logger.exception("재수집 중단", extra={"stage": "backfill_stop"})
                return collected, f"{type(error).__name__}: {error}"

            collected += 1
            write_progress(lawd_cd, deal_ymd, collected)
            logger.info(
                "단위 완료",
                extra={
                    "elapsed_ms": round((time.perf_counter() - started) * 1000),
                    "raw_loaded": result.raw_loaded,
                    "loaded": result.loaded,
                    "collected": collected,
                },
            )
    return collected, None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m src.jobs.backfill_apart_sale", description="아파트 매매를 상세 자료 API로 재수집"
    )
    parser.add_argument(
        "--limit",
        type=bounded_int(1, DEFAULT_LIMIT),
        default=DEFAULT_LIMIT,
        help=f"이번 실행에서 갈아끼울 최대 단위 수 (1~{DEFAULT_LIMIT}, 기본 {DEFAULT_LIMIT})",
    )
    return parser.parse_args()


async def main() -> int:
    args = parse_args()
    configure_logging("pipeline", filters=[UnitContextFilter()])
    try:
        # 정기 갱신과 같은 락을 쓴다. 같은 구간의 DELETE/INSERT가 교차하지 않게 한다.
        async with advisory_lock() as acquired:
            if not acquired:
                logger.error("다른 프로세스가 갱신 중이라 재수집을 하지 못했다", extra={"stage": "lock_busy"})
                return EXIT_FAILED

            after = read_progress()
            total, coordinates = await pending_units(args.limit, after)
            logger.info(
                "aptSeq 없는 아파트 매매 %d단위 중 %d단위를 재수집한다 (이어서 시작: %s)",
                total,
                len(coordinates),
                after,
                extra={"stage": "backfill_start", "pending_units": total, "units": len(coordinates)},
            )

            collected, stopped_reason = await backfill(coordinates)
            if stopped_reason is not None and after is not None and collected == 0:
                # 첫 단위부터 실패했다. 위치는 그대로 두고 사유만 남긴다.
                write_progress(after["lawd_cd"], after["deal_ymd"], 0, stopped_reason)
            elif stopped_reason is not None and collected:
                last_lawd_cd, last_deal_ymd = coordinates[collected - 1]
                write_progress(last_lawd_cd, last_deal_ymd, collected, stopped_reason)
    except Exception:
        logger.exception("재수집이 예외로 중단됐다", extra={"stage": "backfill_aborted"})
        return EXIT_FAILED
    finally:
        await dispose_engine()

    logger.info(
        "재수집 종료: %d단위 성공, 남은 단위 %d, 중단 사유 %s",
        collected,
        total - collected,
        stopped_reason,
        extra={"stage": "backfill_end", "collected": collected, "remaining_units": total - collected},
    )
    # 일일 제한은 내일 이어가면 되는 정상 종료다. 그 밖의 중단은 사람이 봐야 한다.
    if stopped_reason is None or stopped_reason.startswith("DailyLimitReachedError"):
        return EXIT_OK
    return EXIT_FAILED


if __name__ == "__main__":
    load_dotenv()
    raise SystemExit(asyncio.run(main()))
