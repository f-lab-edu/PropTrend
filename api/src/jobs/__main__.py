"""갱신 파이프라인을 API 서버와 별개의 프로세스로 돌리는 진입점.

    python -m src.jobs --months 3
    python -m src.jobs --retry-failed
    python -m src.jobs --only apart_sale:11110:202602

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
    PENDING_LOG_LIMIT,
    SPEC_BY_API_ID,
    PipelineSpec,
    failed_units,
    refresh_all,
    refresh_units,
    stuck_units,
)
from .utils import parse_deal_ymd, split_sgg_cd

logger = logging.getLogger(__name__)

# 쿠버네티스 Job은 종료 코드를 0/비0으로만 본다. 값을 더 쪼개도 재시도 여부는 같으므로
# 실패는 1 하나로 모은다.
EXIT_OK = 0
EXIT_FAILED = 1

# api_id:lawd_cd:deal_ymd
UNIT_PARTS = 3


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


def decide_targeted_exit_code(summary: dict[str, int]) -> int:
    """지목 재실행 결과를 종료 코드로 줄인다."""
    # 사람이 결과를 기다리는 실행이고 뒤에 backoffLimit도 없다. 전량 재실행을 부를 걱정이
    # 없으므로, 정기 회차와 달리 정제 실패도 숨기지 않는다.
    if summary["failed"] or summary["silver_failed"] or summary["no_bronze"]:
        return EXIT_FAILED
    return EXIT_OK


def unit_coordinate(value: str) -> tuple[PipelineSpec, str, str]:
    """`api_id:lawd_cd:deal_ymd`를 갱신 단위 좌표로 바꾼다."""
    parts = value.split(":")
    if len(parts) != UNIT_PARTS:
        raise argparse.ArgumentTypeError(f"api_id:lawd_cd:deal_ymd 형식이어야 한다: {value!r}")

    api_id, lawd_cd, deal_ymd = parts
    spec = SPEC_BY_API_ID.get(api_id)
    if spec is None:
        raise argparse.ArgumentTypeError(f"모르는 api_id다: {api_id!r} (가능: {', '.join(SPEC_BY_API_ID)})")
    try:
        # 검증은 파이프라인이 쓰는 것과 같은 함수여야 한다. 여기서만 통과하는 값이 생기면 안 된다.
        split_sgg_cd(lawd_cd)
        parse_deal_ymd(deal_ymd)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error
    return spec, lawd_cd, deal_ymd


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
        # 지정 재실행과 함께 줬는지 가리려면 "준 적 없음"이 기본값과 구분돼야 한다.
        default=None,
        help=f"이번 달부터 거슬러 갱신할 개월 수 (1~{MAX_MONTHS}, 기본 {DEFAULT_MONTHS})",
    )
    parser.add_argument(
        "--concurrency",
        type=bounded_int(1, MAX_CONCURRENCY),
        default=DEFAULT_CONCURRENCY,
        help=f"동시에 처리할 갱신 단위 수 (1~{MAX_CONCURRENCY}, 기본 {DEFAULT_CONCURRENCY})",
    )
    parser.add_argument(
        "--only",
        action="append",
        type=unit_coordinate,
        metavar="API_ID:LAWD_CD:DEAL_YMD",
        help="이 단위만 다시 돌린다. 여러 번 줄 수 있다 (예: apart_sale:11110:202602)",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="상태 표에 남은 미완 단위를 다시 돌린다",
    )
    parser.add_argument(
        "--with-collect",
        action="store_true",
        help="--only/--retry-failed를 오픈API 수집부터 다시 돌린다 (기본은 정제만)",
    )

    args = parser.parse_args()
    args.targeted = bool(args.only or args.retry_failed)
    # 조용히 무시하지 않는다. 무시된 --months 12는 사람의 한 시간을 태운다.
    if args.targeted and args.months is not None:
        parser.error("--months는 --only/--retry-failed와 함께 쓸 수 없다")
    if args.with_collect and not args.targeted:
        parser.error("--with-collect는 --only 또는 --retry-failed와 함께 쓴다")
    args.months = DEFAULT_MONTHS if args.months is None else args.months
    return args


def _busy_lock_exit_code(*, targeted: bool) -> int:
    """락을 못 잡았을 때의 종료 코드. 모드마다 읽는 사람이 다르다."""
    if targeted:
        # 사람이 결과를 기다리는 실행이다. 아무것도 안 하고 0을 내면 성공으로 읽힌다.
        logger.error("다른 프로세스가 갱신 중이라 지정 재실행을 하지 못했다", extra={"stage": "lock_busy"})
        return EXIT_FAILED
    # 겹침은 이상 징후가 아니다. 예약 실행끼리는 concurrencyPolicy가 막고,
    # 락이 막는 것은 사람의 수동 실행과 겹치는 경로뿐이다.
    logger.warning("다른 프로세스가 갱신 중이라 이번 실행을 건너뛴다", extra={"stage": "lock_busy"})
    return EXIT_OK


async def _targeted_units(args: argparse.Namespace) -> list[tuple[PipelineSpec, str, str]]:
    """--only와 --retry-failed가 가리키는 단위를 좌표로 중복 없이 모은다."""
    units = list(args.only or ())
    if args.retry_failed:
        units += await failed_units(with_collect=args.with_collect)

    seen: set[tuple[str, str, str]] = set()
    unique: list[tuple[PipelineSpec, str, str]] = []
    for spec, lawd_cd, deal_ymd in units:
        key = (spec.api_id, lawd_cd, deal_ymd)
        if key not in seen:
            seen.add(key)
            unique.append((spec, lawd_cd, deal_ymd))
    return unique


async def _run_targeted(args: argparse.Namespace) -> int:
    """지목한 단위만 다시 돌린다."""
    units = await _targeted_units(args)
    if not units:
        # 빈 목록이 곧 "밀린 것이 없다"는 뜻은 아니다. 상한을 넘겨 빠진 단위가 있으면
        # 여기서 짚어 주지 않는 한 운영자는 다 끝났다고 읽는다.
        if stuck := await stuck_units():
            logger.warning(
                "시도 상한을 넘겨 자동 재시도에서 빠진 %d단위가 있다. --only로 지목해야 한다",
                len(stuck),
                extra={
                    "stage": "replay_stuck",
                    "stuck_units": len(stuck),
                    "units": [record.coordinate() for record in stuck[:PENDING_LOG_LIMIT]],
                },
            )
            return EXIT_OK
        logger.info("다시 돌릴 단위가 없다", extra={"stage": "replay_empty"})
        return EXIT_OK
    summary = await refresh_units(units, concurrency=args.concurrency, with_collect=args.with_collect)
    return decide_targeted_exit_code(summary)


async def main() -> int:
    args = parse_args()
    configure_logging("pipeline", filters=[UnitContextFilter()])
    try:
        try:
            # 지정 재실행도 같은 키를 두고 겨룬다. 정기 회차와 같은 구간을 건드리면
            # DELETE/INSERT가 교차하는데, 그게 애초에 이 락이 막으려던 경합이다.
            async with advisory_lock() as acquired:
                if not acquired:
                    return _busy_lock_exit_code(targeted=args.targeted)
                if args.targeted:
                    return await _run_targeted(args)
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
