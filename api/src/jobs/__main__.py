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

# 쿠버네티스 Job은 종료 코드를 0/비0으로만 본다. 값을 더 쪼개도 재시도 여부는 같으므로
# 실패는 1 하나로 모은다.
EXIT_OK = 0
EXIT_FAILED = 1


def decide_exit_code(summary: dict[str, int]) -> int:
    """갱신 결과를 스케줄러가 볼 수 있는 값 하나로 줄인다."""
    # 비0은 backoffLimit이 붙는 순간 곧 전량 재실행 지시가 된다. 즉시 재시도해서
    # 나아지는 실패만 비0으로 둔다는 것이 아래 두 분기의 기준이다.
    if summary["daily_limit"]:
        # 남은 단위를 내일 회차로 넘기는 것이 정상 경로다. 재실행해 봐야 같은 응답을
        # 받으면서 다음 날 호출량만 축낸다. 이 회차의 failed도 함께 내일로 넘어간다.
        return EXIT_OK
    if summary["failed"]:
        # bronze 단계 실패는 1건이라도 전량 재실행 대상이다. 4,000단위가 호출 한도에
        # 한참 못 미치고 한 회차가 짧아, 부분 복구보다 통째로 다시 도는 편이 단순하다.
        return EXIT_FAILED
    # silver_failed는 여기 없다. 2차 패스까지 실패한 정제는 일시 오류가 아니라서
    # 프로세스를 다시 띄워도 결과가 같고, bronze가 남아 코드를 고친 뒤 재처리하면 된다.
    return EXIT_OK


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


async def main() -> int:
    args = parse_args()
    configure_logging("pipeline", filters=[UnitContextFilter()])
    try:
        try:
            async with advisory_lock() as acquired:
                if not acquired:
                    # 겹침은 이상 징후가 아니다. 예약 실행끼리는 concurrencyPolicy가 막고,
                    # 락이 막는 것은 사람의 수동 실행과 겹치는 경로뿐이다.
                    logger.warning("다른 프로세스가 갱신 중이라 이번 실행을 건너뛴다", extra={"stage": "lock_busy"})
                    return EXIT_OK
                summary = await refresh_all(months=args.months, concurrency=args.concurrency)
        except Exception:
            # 여기서 잡지 않으면 트레이스백을 인터프리터가 stderr에 직접 찍어 logging을 거치지
            # 않는다. 가장 심각한 실패가 JSON 로그에서만 빠지는 역전을 막는다. 락 획득 자체가
            # 터지는 경우(DB 접속 실패 등)도 같은 자리에서 걸리도록 try를 락 바깥에 둔다.
            logger.exception("갱신이 예외로 중단됐다", extra={"stage": "refresh_aborted"})
            return EXIT_FAILED
        else:
            return decide_exit_code(summary)
    finally:
        await dispose_engine()


if __name__ == "__main__":
    load_dotenv()
    # main() 안에서 sys.exit을 부르지 않는다. 종료 코드가 정해지는 자리를 여기 하나로 묶어야
    # finally의 dispose_engine()이 그대로 살고, 판정 로직도 따로 부를 수 있다.
    raise SystemExit(asyncio.run(main()))
