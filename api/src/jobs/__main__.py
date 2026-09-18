"""갱신 파이프라인을 API 서버와 별개의 프로세스로 돌리는 진입점.

    python -m src.jobs --months 3

테이블은 API 서버가 기동하면서 만든다. 여기서는 이미 있다고 전제한다.
"""

import argparse
import asyncio
import logging
from collections.abc import Callable

from dotenv import load_dotenv

from ..db import dispose_engine
from ..logging_config import configure_logging
from .context import UnitContextFilter
from .lock import advisory_lock
from .pipeline import (
    DEFAULT_CONCURRENCY,
    DEFAULT_MONTHS,
    MAX_CONCURRENCY,
    MAX_MONTHS,
    refresh_all,
)

logger = logging.getLogger(__name__)


def bounded_int(low: int, high: int) -> Callable[[str], int]:
    """`low`~`high` 범위의 정수만 통과시키는 argparse 타입을 만든다."""

    def parse(value: str) -> int:
        number = int(value)
        if not low <= number <= high:
            raise argparse.ArgumentTypeError(f"{low}~{high} 범위여야 한다: {number}")
        return number

    return parse


def parse_args() -> argparse.Namespace:
    """생략하면 예약 실행과 같은 조건으로 돈다."""
    parser = argparse.ArgumentParser(prog="python -m src.jobs", description="부동산 실거래 데이터 갱신")
    parser.add_argument(
        "--months",
        type=bounded_int(1, MAX_MONTHS),
        default=DEFAULT_MONTHS,
        help=f"이번 달부터 거슬러 갱신할 개월 수 (1~{MAX_MONTHS}, 기본 {DEFAULT_MONTHS})",
    )
    parser.add_argument(
        "--concurrency",
        type=bounded_int(1, MAX_CONCURRENCY),
        default=DEFAULT_CONCURRENCY,
        help=f"동시에 처리할 갱신 단위 수 (1~{MAX_CONCURRENCY}, 기본 {DEFAULT_CONCURRENCY})",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    configure_logging("pipeline", filters=[UnitContextFilter()])
    try:
        async with advisory_lock() as acquired:
            if not acquired:
                logger.warning("다른 프로세스가 갱신 중이라 이번 실행을 건너뛴다", extra={"stage": "lock_busy"})
                return
            await refresh_all(months=args.months, concurrency=args.concurrency)
    finally:
        await dispose_engine()


if __name__ == "__main__":
    load_dotenv()
    asyncio.run(main())
