"""갱신 단위(유형·시군구·계약년월) 하나를 오픈API부터 정제 테이블까지 관통시킨다."""

import asyncio
import logging
import time
from collections import Counter
from collections.abc import AsyncGenerator, Iterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any, ClassVar

from sqlalchemy import select

from ..db import session_scope
from ..model import LegalDongCodeRawItem, PropertyType, UnitStatus
from .cleaner import (
    LegalDongCodeRawItemCleaner,
    RentTransactionCleaner,
    RTMSRawItemCleaner,
    SaleTransactionCleaner,
    TransactionCleaner,
)
from .collector import (
    DailyLimitReachedError,
    LegalDongCodeCollector,
    RtmsDataCollector,
    RTMSRawItemCollector,
)
from .context import unit_context
from .loader import (
    LegalDongCodeRawItemLoader,
    RentTransactionLoader,
    RTMSRawItemLoader,
    SaleTransactionLoader,
    TransactionLoader,
)
from .preprocessor import RawTablePreprocessor, RentPreprocessor, SalePreprocessor
from .state import MAX_ATTEMPTS, RefreshUnitStateStore, UnitRecord
from .utils import parse_deal_ymd, today_kst

logger = logging.getLogger(__name__)

# 신고 기한 30일과 뒤늦은 해제·정정 때문에 지난 달 데이터도 계속 바뀐다.
DEFAULT_MONTHS = 2

# 갱신 단위 수가 개월 수에 비례하므로 상한을 둔다. --months 인자도 이 값으로 검증된다.
MAX_MONTHS = 24

# 단위마다 세션을 하나씩 쓰고 advisory lock이 커넥션 1개를 갱신 내내 붙들고 있으므로,
# 둘을 더한 값이 커넥션 풀 크기(기본 pool_size=5)를 넘기면 안 된다.
DEFAULT_CONCURRENCY = 4

# 풀을 넉넉히 잡아도 오픈API 쪽이 먼저 막힌다. --concurrency 인자도 이 값으로 검증된다.
MAX_CONCURRENCY = 8


def _elapsed_ms(started: float) -> dict[str, int]:
    """로그 extra에 실을 경과 시간."""
    return {"elapsed_ms": round((time.perf_counter() - started) * 1000)}


class SilverStageError(RuntimeError):
    """bronze 적재는 커밋됐고 정제 단계에서 실패했다. 재수집 없이 replay할 수 있다."""


class MissingBronzeError(RuntimeError):
    """정제만 돌리라고 했는데 bronze에 응답이 없다. 실버를 지우지 않고 멈춘다."""


@dataclass(frozen=True)
class PipelineSpec:
    """유형 × 매매/전월세 조합 하나의 설정을 묶는다. 유형과 대상 테이블이 여기에만 적힌다."""

    name: str
    property_type: PropertyType
    api_url: str
    # rtms_raw_items.api_id에 그대로 들어가는 슬러그. RTMS_KNOWN_FIELDS의 키와 같아야 한다.
    api_id: str
    # 건물명 필드 이름은 유형마다 다르고, 단독·다가구에는 아예 없다.
    building_name_field: str | None = None

    preprocessor: ClassVar[type[RawTablePreprocessor]]
    cleaner: ClassVar[type[TransactionCleaner]]
    loader: ClassVar[type[TransactionLoader]]

    def build_preprocessor(self) -> RawTablePreprocessor:
        """이 조합 전용 전처리기를 만든다."""
        return self.preprocessor(self.property_type, self.api_id, self.building_name_field)


@dataclass(frozen=True)
class SaleSpec(PipelineSpec):
    """매매 조합. 전처리기·정리기·적재기는 유형과 무관하므로 여기서 고정한다."""

    preprocessor: ClassVar[type[RawTablePreprocessor]] = SalePreprocessor
    cleaner: ClassVar[type[TransactionCleaner]] = SaleTransactionCleaner
    loader: ClassVar[type[TransactionLoader]] = SaleTransactionLoader


@dataclass(frozen=True)
class RentSpec(PipelineSpec):
    """전월세 조합. 전처리기·정리기·적재기는 유형과 무관하므로 여기서 고정한다."""

    preprocessor: ClassVar[type[RawTablePreprocessor]] = RentPreprocessor
    cleaner: ClassVar[type[TransactionCleaner]] = RentTransactionCleaner
    loader: ClassVar[type[TransactionLoader]] = RentTransactionLoader


# 실거래가 오픈API 8종은 같은 서비스(1613000) 아래 오퍼레이션 이름만 다르다.
RTMS_API = "https://apis.data.go.kr/1613000"

PIPELINES: tuple[PipelineSpec, ...] = (
    SaleSpec(
        "아파트 매매",
        PropertyType.APT,
        # 단지 일련번호(aptSeq)는 상세 자료 API에만 있다. 나머지 필드는 기본 API와 같다(apart-sale-detail.md).
        f"{RTMS_API}/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
        "apart_sale",
        "aptNm",
    ),
    RentSpec(
        "아파트 전월세",
        PropertyType.APT,
        f"{RTMS_API}/RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
        "apart_rent",
        "aptNm",
    ),
    SaleSpec(
        "오피스텔 매매",
        PropertyType.OFFICETEL,
        f"{RTMS_API}/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",
        "officetel_sale",
        "offiNm",
    ),
    RentSpec(
        "오피스텔 전월세",
        PropertyType.OFFICETEL,
        f"{RTMS_API}/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent",
        "officetel_rent",
        "offiNm",
    ),
    SaleSpec(
        "연립다세대 매매",
        PropertyType.ROW_HOUSE,
        f"{RTMS_API}/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",
        "multiflex_sale",
        "mhouseNm",
    ),
    RentSpec(
        "연립다세대 전월세",
        PropertyType.ROW_HOUSE,
        f"{RTMS_API}/RTMSDataSvcRHRent/getRTMSDataSvcRHRent",
        "multiflex_rent",
        "mhouseNm",
    ),
    # 단독·다가구는 건물명 컬럼이 없다.
    SaleSpec(
        "단독다가구 매매",
        PropertyType.SINGLE_MULTI,
        f"{RTMS_API}/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade",
        "single_multi_family_sale",
    ),
    RentSpec(
        "단독다가구 전월세",
        PropertyType.SINGLE_MULTI,
        f"{RTMS_API}/RTMSDataSvcSHRent/getRTMSDataSvcSHRent",
        "single_multi_family_rent",
    ),
)

# 명령줄이 주는 좌표를 스펙으로 되돌리는 유일한 자리다.
SPEC_BY_API_ID: dict[str, PipelineSpec] = {spec.api_id: spec for spec in PIPELINES}

# 회차 종료 경고에 실을 좌표 수. 전량은 상태 표를 직접 조회한다.
PENDING_LOG_LIMIT = 20


@dataclass(frozen=True)
class SilverResult:
    """정제 단계(T2) 1건의 처리 결과."""

    deleted: int
    loaded: int


@dataclass(frozen=True)
class UnitResult:
    """갱신 단위 1건의 처리 결과."""

    spec_name: str
    lawd_cd: str
    deal_ymd: str
    raw_deleted: int
    raw_loaded: int
    deleted: int
    loaded: int


async def refresh_unit(spec: PipelineSpec, lawd_cd: str, deal_ymd: str) -> UnitResult:
    """갱신 단위 1건을 오픈API부터 정제 테이블까지 처리한다."""
    parse_deal_ymd(deal_ymd)  # API를 부르기 전에 형식부터 막는다.

    # 응답을 기다리는 동안 커넥션과 삭제 락을 쥐지 않도록 트랜잭션 밖에서 호출한다.
    # 오픈API가 오류를 돌려주면 여기서 예외가 나므로 삭제는 시작조차 하지 않는다.
    items = await RtmsDataCollector(spec.api_url, lawd_cd, deal_ymd).collect()

    # T1. 응답부터 확정한다. 일일 호출 제한과 30일 신고 기한 때문에 같은 파라미터로 다시
    # 불러도 같은 응답이 오지 않아, 가공과 한 트랜잭션으로 묶으면 가공 실패가 응답을 지운다.
    async with session_scope() as session:
        raw_deleted = await RTMSRawItemCleaner(session, spec.api_id).clean(deal_ymd, lawd_cd)
        raw_loaded = await RTMSRawItemLoader(session, spec.api_id).load(lawd_cd, deal_ymd, items)
        # bronze와 같은 트랜잭션이어야 "응답은 커밋됐는데 기록은 없다"가 생기지 않는다.
        # T1과 T2 사이에서 프로세스가 죽어도 이 행이 남아 다음 재시도가 단위를 찾아낸다.
        await RefreshUnitStateStore(session).mark_collected(spec.api_id, lawd_cd, deal_ymd)
    # 하위 모듈의 로그는 커밋 전에 나간다. 이 줄이 있어야 T1이 실제로 확정됐다는 뜻이 된다.
    logger.debug("bronze 확정", extra={"stage": "commit_bronze", "deleted": raw_deleted, "loaded": raw_loaded})

    # T2. 여기서 터져도 bronze는 남아 API 재호출 없이 다시 돌릴 수 있다.
    silver = await process_unit(spec, lawd_cd, deal_ymd)
    return UnitResult(spec.name, lawd_cd, deal_ymd, raw_deleted, raw_loaded, silver.deleted, silver.loaded)


def _reject_empty_bronze(rows: Sequence[Any], api_id: str, lawd_cd: str, deal_ymd: str) -> None:
    """정제만 돌리는 경로에서 bronze가 비었으면 멈춘다."""
    # 그냥 두면 cleaner가 구간을 지우고 loader가 0건을 넣어 실버가 조용히 비워진다.
    # 회차 안에서는 응답 0건이 곧 빈 구간이라 정상이지만, 사람이 찍은 좌표에서는 오타가 삭제가 된다.
    if not rows:
        raise MissingBronzeError(f"bronze에 응답이 없다: {api_id}:{lawd_cd}:{deal_ymd}")


async def process_unit(
    spec: PipelineSpec,
    lawd_cd: str,
    deal_ymd: str,
    *,
    require_bronze: bool = False,
) -> SilverResult:
    """bronze에 남은 응답만으로 정제 단계를 돌린다. 오픈API를 부르지 않아 단독 재실행이 된다."""
    try:
        async with session_scope() as session:
            # 메모리의 items가 아니라 표에서 다시 읽는다. 트랜잭션이 갈려 있기도 하고,
            # 전처리기가 오류 행을 짚으려면 적재로만 얻어지는 bronze id가 필요하다.
            rows = await RTMSRawItemCollector(session, spec.api_id).collect(deal_ymd, lawd_cd)
            if require_bronze:
                _reject_empty_bronze(rows, spec.api_id, lawd_cd, deal_ymd)
            payload = spec.build_preprocessor().preprocess(rows)

            deleted = await spec.cleaner(session).clean(spec.property_type, deal_ymd, lawd_cd)
            loaded = await spec.loader(session).load(payload)
            # 성공 기록은 행을 지우는 것이다. T2가 롤백되면 이 삭제도 함께 되돌아가
            # COLLECTED가 그대로 남으므로, 실패를 따로 적을 필요가 없다.
            await RefreshUnitStateStore(session).clear(spec.api_id, lawd_cd, deal_ymd)
    except MissingBronzeError:
        raise
    except Exception as error:
        raise SilverStageError(f"정제 단계 실패(bronze는 남아 있다): {error}") from error

    logger.debug("정제 확정", extra={"stage": "commit_silver", "deleted": deleted, "loaded": loaded})
    return SilverResult(deleted, loaded)


async def refresh_legal_dong_codes() -> int:
    """시군구 목록의 출처인 법정동코드를 통째로 갱신한다."""
    # 표를 통째로 비우므로, 오류나 빈 응답이 여기까지 올라오지 않는 것이 전제다.
    items = await LegalDongCodeCollector().collect()
    # 비우기와 채우기는 한 트랜잭션이어야 한다. 사이에서 끊기면 시군구 목록이 사라진다.
    async with session_scope() as session:
        deleted = await LegalDongCodeRawItemCleaner(session).clean()
        loaded = await LegalDongCodeRawItemLoader(session).load(items)
    logger.info("법정동코드 %d건 갱신", loaded, extra={"stage": "legal_dong", "deleted": deleted, "loaded": loaded})
    return loaded


async def sigungu_codes() -> list[str]:
    """실거래가 API의 LAWD_CD로 쓸 시군구 5자리 목록."""
    # payload ->> 'region_cd'. 수집기의 _is_sigungu를 통과한 250건 규모라 인덱스는 두지 않는다.
    region_cd = LegalDongCodeRawItem.__table__.c.payload["region_cd"].astext
    async with session_scope() as session:
        result = await session.execute(select(region_cd).order_by(region_cd))
        return [code[:5] for code in result.scalars() if code]


def recent_months(months: int = DEFAULT_MONTHS, today: date | None = None) -> list[str]:
    """이번 달부터 과거로 `months`개월의 계약년월을 최신순으로 만든다."""
    # 명령줄 인자가 흘러드는 값이다. 지금은 __main__이 같은 범위로 막고 있지만,
    # 반복 횟수를 정하는 값의 범위는 쓰는 쪽이 아니라 여기서 보장해야 한다.
    if not 1 <= months <= MAX_MONTHS:
        raise ValueError(f"months는 1~{MAX_MONTHS} 범위여야 한다: {months}")

    today = today or today_kst()
    year, month = today.year, today.month
    result = []
    for _ in range(months):
        result.append(f"{year:04d}{month:02d}")
        month -= 1
        if month == 0:
            year, month = year - 1, 12
    return result


def iter_units(sgg_list: Sequence[str], month_list: Sequence[str]) -> Iterator[tuple[PipelineSpec, str, str]]:
    """갱신 단위를 최신 월부터 훑는다. 중간에 잘려도 최근 데이터가 먼저 반영된다."""
    for deal_ymd in month_list:
        for lawd_cd in sgg_list:
            for spec in PIPELINES:
                yield spec, lawd_cd, deal_ymd


@asynccontextmanager
async def _unit_slot(
    semaphore: asyncio.Semaphore,
    spec: PipelineSpec,
    lawd_cd: str,
    deal_ymd: str,
) -> AsyncGenerator[float]:
    """단위 하나가 도는 동안의 동시 실행 제한·좌표 컨텍스트·시작 시각을 한자리에 묶는다."""
    async with semaphore:
        # 이 블록 안의 로그에는 좌표가 자동으로 붙는다. 수집기·적재기처럼 좌표를 모르는
        # 하위 모듈의 로그도 함께 짚을 수 있다.
        with unit_context(spec.api_id, lawd_cd, deal_ymd):
            yield time.perf_counter()


def _new_summary(units: int) -> dict[str, int]:
    """회차 집계의 초기값. 이 키 집합이 종료 코드 판정의 계약이다."""
    return {
        "units": units,
        "succeeded": 0,
        "failed": 0,
        "silver_failed": 0,
        "silver_recovered": 0,
        "no_bronze": 0,
        "skipped": 0,
        "daily_limit": 0,
        "loaded": 0,
    }


class RunTally:
    """회차 집계. 전체 합계와 API별 분해를 같은 자리에서 올려 둘이 어긋나지 않게 한다."""

    def __init__(self, units: Sequence[tuple[PipelineSpec, str, str]]) -> None:
        # summary는 종료 코드 판정의 계약이라 모양을 그대로 둔다. 분해는 옆에 따로 쌓는다.
        self.summary = _new_summary(len(units))
        # 활동이 없던 spec도 0으로 드러나야 "안 돈 것"과 "다 성공한 것"이 구분된다.
        self.by_api: dict[str, Counter[str]] = {}
        for spec, _, _ in units:
            self.by_api.setdefault(spec.api_id, Counter())["units"] += 1

    def bump(self, spec: PipelineSpec, key: str, amount: int = 1) -> None:
        """합계와 API별 분해를 함께 올린다. 둘을 따로 올리면 언젠가 반드시 어긋난다."""
        self.summary[key] += amount
        self.by_api[spec.api_id][key] += amount

    def by_api_log(self) -> dict[str, dict[str, int]]:
        """로그에 실을 모양. PIPELINES 순서를 따라야 사람이 읽을 때 유형이 섞이지 않는다."""
        # Counter를 그대로 두면 TextFormatter가 repr을 찍는다.
        return {spec.api_id: dict(self.by_api[spec.api_id]) for spec in PIPELINES if spec.api_id in self.by_api}


async def _record_failure(
    api_id: str,
    lawd_cd: str,
    deal_ymd: str,
    status: UnitStatus,
    error: BaseException,
) -> None:
    """실패 기록은 롤백이 끝난 뒤 별도 트랜잭션으로 남긴다. 실패해도 원래 예외를 덮지 않는다."""
    try:
        async with session_scope() as session:
            await RefreshUnitStateStore(session).mark_failed(api_id, lawd_cd, deal_ymd, status, error)
    except Exception:
        # 단위가 터진 이유가 DB 자체일 수 있다. 기록에 실패했다고 예외를 올리면 원래 원인이
        # 이 예외에 묻힌다. 여기서 끊고 로그만 남긴다.
        logger.exception("실패 기록을 남기지 못했다", extra={"stage": "state_write_failed"})


async def _collect_pass(
    units: Sequence[tuple[PipelineSpec, str, str]],
    tally: RunTally,
    semaphore: asyncio.Semaphore,
) -> list[tuple[PipelineSpec, str, str]]:
    """수집부터 도는 패스. 정제만 실패해 재수집이 필요 없는 단위 목록을 돌려준다."""
    stop = asyncio.Event()
    silver_retry: list[tuple[PipelineSpec, str, str]] = []

    async def run(spec: PipelineSpec, lawd_cd: str, deal_ymd: str) -> None:
        async with _unit_slot(semaphore, spec, lawd_cd, deal_ymd) as started:
            # 슬롯을 기다리는 사이에 제한에 걸렸을 수 있다. 남은 단위는 슬롯만 스치고 끝난다.
            if stop.is_set():
                tally.bump(spec, "skipped")
                logger.debug("건너뜀(일일 제한)", extra={"stage": "skip"})
                return

            try:
                result = await refresh_unit(spec, lawd_cd, deal_ymd)
            except DailyLimitReachedError:
                # 남은 단위도 전부 같은 응답을 받는다. 여기서 접고 내일 회차에 맡긴다.
                # 아무것도 커밋되지 않았으므로 상태 표에도 남기지 않는다.
                stop.set()
                tally.bump(spec, "daily_limit")
                logger.warning("일일 호출 제한 도달, 남은 단위를 건너뛴다", extra=_elapsed_ms(started))
            except SilverStageError:
                # bronze에는 응답이 남았고 COLLECTED 행도 T1이 이미 남겼다. 2차 패스로 넘긴다.
                silver_retry.append((spec, lawd_cd, deal_ymd))
                logger.exception("정제 실패(bronze 보존)", extra=_elapsed_ms(started))
            except Exception as error:
                # 단위 1건은 트랜잭션째 되돌아가므로 나머지를 멈추지 않고 넘어간다.
                tally.bump(spec, "failed")
                logger.exception("갱신 실패", extra=_elapsed_ms(started))
                await _record_failure(spec.api_id, lawd_cd, deal_ymd, UnitStatus.FAILED, error)
            else:
                tally.bump(spec, "succeeded")
                tally.bump(spec, "loaded", result.loaded)
                logger.info(
                    "단위 완료",
                    extra=_elapsed_ms(started)
                    | {
                        "raw_deleted": result.raw_deleted,
                        "raw_loaded": result.raw_loaded,
                        "deleted": result.deleted,
                        "loaded": result.loaded,
                    },
                )

    await asyncio.gather(*(run(*unit) for unit in units))
    return silver_retry


async def _silver_pass(
    units: Sequence[tuple[PipelineSpec, str, str]],
    tally: RunTally,
    semaphore: asyncio.Semaphore,
    *,
    require_bronze: bool = False,
) -> None:
    """오픈API 없이 정제만 돌린다. 회차 안 2차 패스와 지정 재실행이 함께 쓴다."""
    logger.info("정제 %d단위", len(units), extra={"stage": "silver_retry_start", "units": len(units)})

    async def retry(spec: PipelineSpec, lawd_cd: str, deal_ymd: str) -> None:
        async with _unit_slot(semaphore, spec, lawd_cd, deal_ymd) as started:
            try:
                result = await process_unit(spec, lawd_cd, deal_ymd, require_bronze=require_bronze)
            except MissingBronzeError:
                # 파이프라인이 실패한 게 아니라 좌표가 가리키는 응답이 없는 것이다.
                # 상태 표는 건드리지 않고, 사람이 알아채도록 집계에만 올린다.
                tally.bump(spec, "no_bronze")
                logger.error("bronze가 없어 정제를 건너뛴다", extra=_elapsed_ms(started))  # noqa: TRY400
            except SilverStageError as error:
                # 같은 자리에서 또 터졌다면 일시 오류가 아니라 전처리 로직 쪽이다.
                # 프로세스를 다시 띄워도 결과가 같으므로 정기 회차의 종료 코드로는 올리지 않는다.
                tally.bump(spec, "silver_failed")
                logger.exception("정제 실패(bronze 보존)", extra=_elapsed_ms(started))
                await _record_failure(spec.api_id, lawd_cd, deal_ymd, UnitStatus.COLLECTED, error)
            else:
                tally.bump(spec, "silver_recovered")
                tally.bump(spec, "succeeded")
                tally.bump(spec, "loaded", result.loaded)
                logger.info("정제 성공", extra=_elapsed_ms(started) | {"loaded": result.loaded})

    await asyncio.gather(*(retry(*unit) for unit in units))


async def _leftover_units() -> list[UnitRecord]:
    """상태 표에 남은 단위 전부."""
    async with session_scope() as session:
        return await RefreshUnitStateStore(session).leftovers()


async def stuck_units() -> list[UnitRecord]:
    """시도 상한을 넘겨 자동 재시도에서 빠진 단위. --only로만 다시 돌릴 수 있다."""
    return [record for record in await _leftover_units() if record.attempts >= MAX_ATTEMPTS]


async def _finish_run(tally: RunTally, run_started: float) -> None:
    """상태 표를 세어 요약에 붙이고 회차 종료 로그를 남긴다."""
    summary = tally.summary
    leftovers: list[UnitRecord] = []
    try:
        leftovers = await _leftover_units()
    except Exception:
        # 다 끝난 회차를 집계 실패로 뒤집지 않는다. 두 키가 빠진 것이 곧 "못 셌다"는 표시다.
        logger.exception("상태 표를 세지 못했다", extra={"stage": "state_count_failed"})
    else:
        summary["pending_units"] = len(leftovers)
        summary["stuck_units"] = sum(1 for record in leftovers if record.attempts >= MAX_ATTEMPTS)

    logger.info(
        "갱신 종료: 성공 %d / 실패 %d / 정제실패 %d(복구 %d) / 건너뜀 %d / 적재 %d건",
        summary["succeeded"],
        summary["failed"],
        summary["silver_failed"],
        summary["silver_recovered"],
        summary["skipped"],
        summary["loaded"],
        extra=summary | {"stage": "refresh_end"} | _elapsed_ms(run_started),
    )
    # 합계만으로는 "어느 API가 유독 실패하는가"가 안 나온다. 분해는 이벤트를 따로 낸다.
    logger.info("API별 집계", extra={"stage": "api_summary", "by_api": tally.by_api_log()})

    if summary["daily_limit"]:
        # 사유는 사람이 읽는 로그에 남긴다. summary는 dict[str, int]라 문자열을 담을 수 없다.
        logger.warning(
            "일일 호출 제한으로 %d단위를 남기고 중단했다",
            summary["skipped"],
            extra={"stage": "daily_limit", "skipped": summary["skipped"], "daily_limit": summary["daily_limit"]},
        )
    if leftovers:
        # 좌표가 남는 유일한 자리다. units 값은 --only에 그대로 복사해 넣을 수 있는 모양이다.
        logger.warning(
            "정제가 끝나지 않은 %d단위가 남아 있다",
            len(leftovers),
            extra={
                "stage": "unit_state_pending",
                "pending_units": len(leftovers),
                "stuck_units": summary.get("stuck_units", 0),
                "units": [record.coordinate() for record in leftovers[:PENDING_LOG_LIMIT]],
            },
        )


async def refresh_all(months: int = DEFAULT_MONTHS, concurrency: int = DEFAULT_CONCURRENCY) -> dict[str, int]:
    """하루치 갱신 전체. 스케줄러에 등록되는 작업은 이 함수 하나다."""
    run_started = time.perf_counter()
    await refresh_legal_dong_codes()
    sgg_list = await sigungu_codes()
    month_list = recent_months(months)

    units = list(iter_units(sgg_list, month_list))
    logger.info(
        "갱신 시작: 시군구 %d × %d개월 × %d종 = %d단위",
        len(sgg_list),
        len(month_list),
        len(PIPELINES),
        len(units),
        extra={
            "stage": "refresh_start",
            "sigungu": len(sgg_list),
            "months": len(month_list),
            "specs": len(PIPELINES),
            "units": len(units),
            "concurrency": concurrency,
        },
    )

    tally = RunTally(units)
    semaphore = asyncio.Semaphore(concurrency)
    silver_retry = await _collect_pass(units, tally, semaphore)

    # 수집을 부르지 않으므로 일일 제한으로 접힌 회차에서도 그대로 돌린다.
    if silver_retry:
        await _silver_pass(silver_retry, tally, semaphore)

    await _finish_run(tally, run_started)
    return tally.summary


async def refresh_units(
    units: Sequence[tuple[PipelineSpec, str, str]],
    concurrency: int = DEFAULT_CONCURRENCY,
    *,
    with_collect: bool = False,
) -> dict[str, int]:
    """지목한 단위만 다시 돌린다. 기본은 정제만, with_collect면 수집부터."""
    run_started = time.perf_counter()
    logger.info(
        "지정 재실행 %d단위",
        len(units),
        extra={"stage": "replay_start", "units": len(units), "with_collect": with_collect},
    )

    tally = RunTally(units)
    semaphore = asyncio.Semaphore(concurrency)
    if with_collect:
        silver_retry = await _collect_pass(units, tally, semaphore)
        if silver_retry:
            await _silver_pass(silver_retry, tally, semaphore)
    else:
        # 사람이 좌표를 찍은 경로다. bronze가 없으면 실버를 지우지 말고 멈춰야 한다.
        await _silver_pass(units, tally, semaphore, require_bronze=True)

    await _finish_run(tally, run_started)
    return tally.summary


async def failed_units(*, with_collect: bool) -> list[tuple[PipelineSpec, str, str]]:
    """상태 표에서 다시 돌릴 단위를 읽어 스펙으로 해석한다."""
    async with session_scope() as session:
        records = await RefreshUnitStateStore(session).pending((UnitStatus.COLLECTED, UnitStatus.FAILED))

    if not with_collect:
        # 수집이 필요한 단위를 조용히 빼면 "왜 안 돌았지"가 된다.
        if blocked := [record for record in records if record.status == UnitStatus.FAILED]:
            logger.warning(
                "수집이 필요한 %d단위는 --with-collect 없이는 건너뛴다",
                len(blocked),
                extra={"stage": "replay_blocked", "units": [record.coordinate() for record in blocked]},
            )
        records = [record for record in records if record.status == UnitStatus.COLLECTED]

    units: list[tuple[PipelineSpec, str, str]] = []
    for record in records:
        spec = SPEC_BY_API_ID.get(record.api_id)
        if spec is None:
            # 슬러그가 바뀌면 해석되지 않는 행이 남는다. 조용히 지우지 않고 경고만 남긴다.
            logger.warning(
                "모르는 api_id라 건너뛴다: %s",
                record.coordinate(),
                extra={"stage": "replay_unknown_spec"},
            )
            continue
        units.append((spec, record.lawd_cd, record.deal_ymd))
    return units
