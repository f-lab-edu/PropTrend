"""갱신 파이프라인을 API 서버와 별개의 프로세스로 돌리는 진입점.

    python -m src.jobs --months 3
    python -m src.jobs --api-id apart_sale --summary-json
    python -m src.jobs --legal-dong-only
    python -m src.jobs --retry-failed
    python -m src.jobs --only apart_sale:11110:202602

테이블은 API 서버가 기동하면서 만든다. 여기서는 이미 있다고 전제한다.
"""

import argparse
import asyncio
import json
import logging
from collections.abc import Callable

from dotenv import load_dotenv

from ..db import dispose_engine
from ..logging_config import configure_logging
from .context import UnitContextFilter
from .lock import LEGAL_DONG_SCOPE, advisory_lock
from .pipeline import (
    DEFAULT_CONCURRENCY,
    DEFAULT_MONTHS,
    MAX_CONCURRENCY,
    MAX_MONTHS,
    PENDING_LOG_LIMIT,
    PIPELINES,
    SPEC_BY_API_ID,
    PipelineSpec,
    failed_units,
    refresh_all,
    refresh_legal_dong_codes,
    refresh_specs,
    refresh_units,
    stuck_units,
)
from .utils import parse_deal_ymd, split_sgg_cd

logger = logging.getLogger(__name__)

# Airflow는 비0을 실패로 보고 재시도하되, skip_on_exit_code로 지정한 값만 건너뜀으로 바꾼다.
# 실패는 1 하나로 모으고, 재시도해 봐야 결과가 같은 경우만 99로 가른다.
EXIT_OK = 0
EXIT_FAILED = 1
EXIT_SKIPPED = 99

# api_id:lawd_cd:deal_ymd
UNIT_PARTS = 3


def decide_exit_code(summary: dict[str, int]) -> int:
    """갱신 결과를 스케줄러가 볼 수 있는 값 하나로 줄인다."""
    # 재시도는 이 프로세스가 맡은 범위(정기 실행이면 API 하나)를 통째로 다시 돈다.
    # 즉시 재시도해서 나아지는 실패만 EXIT_FAILED로 둔다는 것이 아래 두 분기의 기준이다.
    if summary["daily_limit"]:
        # 남은 단위를 내일 회차로 넘기는 것이 정상 경로다. 재실행해 봐야 같은 응답을
        # 받으면서 다음 날 호출량만 축낸다. 이 회차의 failed도 함께 내일로 넘어간다.
        # 0으로 두면 "다 끝났다"와 구분되지 않으므로 건너뜀으로 드러낸다.
        return EXIT_SKIPPED
    if summary["failed"]:
        # bronze 단계 실패는 1건이라도 재실행 대상이다. 한 API의 단위 수가 호출 한도에
        # 한참 못 미치고 한 회차가 짧아, 부분 복구보다 통째로 다시 도는 편이 단순하다.
        return EXIT_FAILED
    # silver_failed는 여기 없다. 2차 패스까지 실패한 정제는 일시 오류가 아니라서
    # 프로세스를 다시 띄워도 결과가 같고, bronze가 남아 코드를 고친 뒤 재처리하면 된다.
    return EXIT_OK


def decide_targeted_exit_code(summary: dict[str, int]) -> int:
    """지목 재실행 결과를 종료 코드로 줄인다."""
    # 사람이 결과를 기다리는 실행이고 뒤에 자동 재시도도 없다. 재실행을 부를 걱정이
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
    parser = argparse.ArgumentParser(
        prog="python -m src.jobs",
        description="부동산 실거래 데이터 갱신",
        epilog=(
            f"종료 코드: {EXIT_OK} 성공, {EXIT_FAILED} 실패(재시도 대상), "
            f"{EXIT_SKIPPED} 건너뜀(일일 호출 한도 도달 또는 다른 실행이 락을 잡음, 재시도해도 같음)"
        ),
    )
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
        "--api-id",
        action="append",
        choices=list(SPEC_BY_API_ID),
        help="이 유형만 정기 갱신한다. 법정동코드는 받지 않고 있는 목록을 쓴다. 여러 번 줄 수 있다",
    )
    parser.add_argument(
        "--legal-dong-only",
        action="store_true",
        help="법정동코드와 지역 목록만 교체한다",
    )
    parser.add_argument(
        "--summary-json",
        action="store_true",
        help="정기 갱신이 끝나면 stdout 마지막 줄에 회차 요약을 JSON으로 찍는다",
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
    # 유형별 지정 재실행은 재실행 DAG가 생길 때 연다. 지금 열면 검증할 조합만 늘어난다.
    if args.api_id and (args.targeted or args.legal_dong_only):
        parser.error("--api-id는 --only/--retry-failed/--legal-dong-only와 함께 쓸 수 없다")
    if args.legal_dong_only and (args.targeted or args.months is not None):
        parser.error("--legal-dong-only는 --only/--retry-failed/--months와 함께 쓸 수 없다")
    # 요약은 정기 갱신만 낸다. 다른 모드에서 받아 두고 아무것도 찍지 않으면 읽는 쪽이 빈 값을 기다린다.
    if args.summary_json and (args.targeted or args.legal_dong_only):
        parser.error("--summary-json은 --only/--retry-failed/--legal-dong-only와 함께 쓸 수 없다")
    # 같은 유형을 두 번 주면 단위가 두 번씩 돈다.
    args.api_id = list(dict.fromkeys(args.api_id or ()))
    args.months = DEFAULT_MONTHS if args.months is None else args.months
    return args


def lock_scopes(args: argparse.Namespace) -> tuple[str, ...]:
    """이번 실행이 겨룰 락 범위를 고른다."""
    if args.legal_dong_only:
        # 실거래 갱신은 시작할 때 시군구 목록을 한 번 읽을 뿐이고, 교체는 한 트랜잭션이라
        # 이전 목록이나 새 목록 중 하나를 본다. 실거래 범위까지 잡을 이유가 없다.
        return (LEGAL_DONG_SCOPE,)
    if args.api_id:
        return tuple(args.api_id)
    api_scopes = tuple(spec.api_id for spec in PIPELINES)
    if args.targeted:
        # --retry-failed는 좌표를 락을 잡은 뒤에 읽으므로 어느 유형을 건드릴지 미리 알 수 없다.
        return api_scopes
    return (LEGAL_DONG_SCOPE, *api_scopes)


def _busy_lock_exit_code(*, targeted: bool) -> int:
    """락을 못 잡았을 때의 종료 코드. 모드마다 읽는 사람이 다르다."""
    if targeted:
        # 사람이 결과를 기다리는 실행이다. 아무것도 안 하고 0을 내면 성공으로 읽힌다.
        logger.error("다른 프로세스가 갱신 중이라 지정 재실행을 하지 못했다", extra={"stage": "lock_busy"})
        return EXIT_FAILED
    # 겹침은 이상 징후가 아니다. 예약 실행끼리는 스케줄러의 pool이 줄 세우고, 락이 막는 것은
    # 사람의 수동 실행과 겹치는 경로뿐이다. 다만 0을 내면 "왜 안 돌았나"가 보이지 않는다.
    logger.warning("다른 프로세스가 갱신 중이라 이번 실행을 건너뛴다", extra={"stage": "lock_busy"})
    return EXIT_SKIPPED


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
            # 지정 재실행도 정기 회차와 같은 유형 범위를 두고 겨룬다. 같은 구간을 건드리면
            # DELETE/INSERT가 교차하는데, 그게 애초에 이 락이 막으려던 경합이다.
            async with advisory_lock(*lock_scopes(args)) as acquired:
                if not acquired:
                    return _busy_lock_exit_code(targeted=args.targeted)
                if args.targeted:
                    return await _run_targeted(args)
                if args.legal_dong_only:
                    await refresh_legal_dong_codes()
                    return EXIT_OK
                if args.api_id:
                    specs = [SPEC_BY_API_ID[api_id] for api_id in args.api_id]
                    summary = await refresh_specs(specs, months=args.months, concurrency=args.concurrency)
                else:
                    summary = await refresh_all(months=args.months, concurrency=args.concurrency)
        except Exception:
            # 여기서 잡지 않으면 트레이스백을 인터프리터가 stderr에 직접 찍어 logging을 거치지
            # 않는다. 가장 심각한 실패가 JSON 로그에서만 빠지는 역전을 막는다. 락 획득 자체가
            # 터지는 경우(DB 접속 실패 등)도 같은 자리에서 걸리도록 try를 락 바깥에 둔다.
            logger.exception("갱신이 예외로 중단됐다", extra={"stage": "refresh_aborted"})
            return EXIT_FAILED
        else:
            if args.summary_json:
                # 로그도 stdout으로 나가므로 마지막 로그 뒤에 한 번만 찍는다. DockerOperator가
                # stdout 마지막 줄만 XCom으로 올린다.
                print(json.dumps(summary), flush=True)
            return decide_exit_code(summary)
    finally:
        await dispose_engine()


if __name__ == "__main__":
    load_dotenv()
    # main() 안에서 sys.exit을 부르지 않는다. 종료 코드가 정해지는 자리를 여기 하나로 묶어야
    # finally의 dispose_engine()이 그대로 살고, 판정 로직도 따로 부를 수 있다.
    raise SystemExit(asyncio.run(main()))
