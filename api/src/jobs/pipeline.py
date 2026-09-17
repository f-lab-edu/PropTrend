"""갱신 단위(유형·시군구·계약년월) 하나를 오픈API부터 정제 테이블까지 관통시킨다."""

import asyncio
import logging
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import date
from typing import ClassVar

from sqlalchemy import select

from ..db import session_scope
from ..model import LegalDongCodeRawItem, PropertyType
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
from .utils import parse_deal_ymd, today_kst

logger = logging.getLogger(__name__)

# 신고 기한 30일과 뒤늦은 해제·정정 때문에 지난 달 데이터도 계속 바뀐다.
DEFAULT_MONTHS = 2

# 갱신 단위 수가 개월 수에 비례하므로 상한을 둔다. --months 인자도 이 값으로 검증된다.
MAX_MONTHS = 24

# 단위마다 세션을 하나씩 쓰므로 커넥션 풀 크기(기본 pool_size=5)를 넘기면 안 된다.
DEFAULT_CONCURRENCY = 4

# 풀을 넉넉히 잡아도 오픈API 쪽이 먼저 막힌다. --concurrency 인자도 이 값으로 검증된다.
MAX_CONCURRENCY = 8


class SilverStageError(RuntimeError):
    """bronze 적재는 커밋됐고 정제 단계에서 실패했다. 재수집 없이 replay할 수 있다."""


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
        f"{RTMS_API}/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade",
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

    # T2. 여기서 터져도 bronze는 남아 API 재호출 없이 다시 돌릴 수 있다.
    try:
        async with session_scope() as session:
            # 메모리의 items가 아니라 표에서 다시 읽는다. 트랜잭션이 갈려 있기도 하고,
            # 전처리기가 오류 행을 짚으려면 적재로만 얻어지는 bronze id가 필요하다.
            rows = await RTMSRawItemCollector(session, spec.api_id).collect(deal_ymd, lawd_cd)
            payload = spec.build_preprocessor().preprocess(rows)

            deleted = await spec.cleaner(session).clean(spec.property_type, deal_ymd, lawd_cd)
            loaded = await spec.loader(session).load(payload)
    except Exception as error:
        raise SilverStageError(f"정제 단계 실패(bronze {raw_loaded}건은 남아 있다): {error}") from error

    return UnitResult(spec.name, lawd_cd, deal_ymd, raw_deleted, raw_loaded, deleted, loaded)


async def refresh_legal_dong_codes() -> int:
    """시군구 목록의 출처인 법정동코드를 통째로 갱신한다."""
    # 표를 통째로 비우므로, 오류나 빈 응답이 여기까지 올라오지 않는 것이 전제다.
    items = await LegalDongCodeCollector().collect()
    # 비우기와 채우기는 한 트랜잭션이어야 한다. 사이에서 끊기면 시군구 목록이 사라진다.
    async with session_scope() as session:
        await LegalDongCodeRawItemCleaner(session).clean()
        loaded = await LegalDongCodeRawItemLoader(session).load(items)
    logger.info("법정동코드 %d건 갱신", loaded)
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


async def refresh_all(months: int = DEFAULT_MONTHS, concurrency: int = DEFAULT_CONCURRENCY) -> dict[str, int]:
    """하루치 갱신 전체. 스케줄러에 등록되는 작업은 이 함수 하나다."""
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
    )

    semaphore = asyncio.Semaphore(concurrency)
    stop = asyncio.Event()
    summary = {
        "units": len(units),
        "succeeded": 0,
        "failed": 0,
        "silver_failed": 0,
        "skipped": 0,
        "daily_limit": 0,
        "loaded": 0,
    }

    async def run(spec: PipelineSpec, lawd_cd: str, deal_ymd: str) -> None:
        async with semaphore:
            # 슬롯을 기다리는 사이에 제한에 걸렸을 수 있다. 남은 단위는 슬롯만 스치고 끝난다.
            if stop.is_set():
                summary["skipped"] += 1
                return
            # 이 블록 안의 로그에는 좌표가 자동으로 붙는다. 수집기·적재기처럼 좌표를 모르는
            # 하위 모듈의 로그도 함께 짚을 수 있다.
            with unit_context(spec.api_id, lawd_cd, deal_ymd):
                try:
                    result = await refresh_unit(spec, lawd_cd, deal_ymd)
                except DailyLimitReachedError:
                    # 남은 단위도 전부 같은 응답을 받는다. 여기서 접고 내일 회차에 맡긴다.
                    stop.set()
                    summary["daily_limit"] += 1
                    logger.warning("일일 호출 제한 도달, 남은 단위를 건너뛴다")
                except SilverStageError:
                    # bronze에는 응답이 남았다. 재수집 없이 다시 돌리면 되므로 따로 센다.
                    summary["silver_failed"] += 1
                    logger.exception("정제 실패(bronze 보존)")
                except Exception:
                    # 단위 1건은 트랜잭션째 되돌아가므로 나머지를 멈추지 않고 넘어간다.
                    summary["failed"] += 1
                    logger.exception("갱신 실패")
                else:
                    summary["succeeded"] += 1
                    summary["loaded"] += result.loaded

    await asyncio.gather(*(run(*unit) for unit in units))

    logger.info(
        "갱신 종료: 성공 %d / 실패 %d / 정제실패 %d / 건너뜀 %d / 적재 %d건",
        summary["succeeded"],
        summary["failed"],
        summary["silver_failed"],
        summary["skipped"],
        summary["loaded"],
    )
    if summary["daily_limit"]:
        # 사유는 사람이 읽는 로그에 남긴다. summary는 dict[str, int]라 문자열을 담을 수 없다.
        logger.warning("일일 호출 제한으로 %d단위를 남기고 중단했다", summary["skipped"])
    return summary
