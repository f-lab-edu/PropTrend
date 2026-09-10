"""갱신 단위(유형·시군구·계약년월) 하나를 오픈API부터 정제 테이블까지 관통시킨다."""

import asyncio
import logging
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import date
from typing import ClassVar

from sqlalchemy import delete, select

from ..db import session_scope
from ..model import (
    Base,
    PropertyType,
    RawApartRent,
    RawApartSale,
    RawLegalDongCode,
    RawMultiflexRent,
    RawMultiflexSale,
    RawOfficetelRent,
    RawOfficetelSale,
    RawSingleMultiFamilyRent,
    RawSingleMultiFamilySale,
)
from . import collector as C
from . import preprocessor as P
from .cleaner import (
    RawTableCleaner,
    RentTransactionCleaner,
    SaleTransactionCleaner,
    TransactionCleaner,
)
from .loader import (
    RawDataLoader,
    RentTransactionLoader,
    SaleTransactionLoader,
    TransactionLoader,
)
from .utils import parse_deal_ymd, today_kst

logger = logging.getLogger(__name__)

# 신고 기한 30일과 뒤늦은 해제·정정 때문에 지난 달 데이터도 계속 바뀐다.
DEFAULT_MONTHS = 2

# 단위마다 세션을 하나씩 쓰므로 커넥션 풀 크기(기본 pool_size=5)를 넘기면 안 된다.
DEFAULT_CONCURRENCY = 4


@dataclass(frozen=True)
class PipelineSpec:
    """유형 × 매매/전월세 조합 하나가 쓰는 클래스들을 묶는다."""

    name: str
    property_type: PropertyType
    api_collector: type[C.RtmsDataCollector]
    raw_model: type[Base]
    raw_collector: type[C.RawTableCollector]
    preprocessor: type[P.RawTablePreprocessor]

    cleaner: ClassVar[type[TransactionCleaner]]
    loader: ClassVar[type[TransactionLoader]]


@dataclass(frozen=True)
class SaleSpec(PipelineSpec):
    """매매 조합. 정리기·적재기는 유형과 무관하므로 여기서 고정한다."""

    cleaner = SaleTransactionCleaner
    loader = SaleTransactionLoader


@dataclass(frozen=True)
class RentSpec(PipelineSpec):
    """전월세 조합. 정리기·적재기는 유형과 무관하므로 여기서 고정한다."""

    cleaner = RentTransactionCleaner
    loader = RentTransactionLoader


PIPELINES: tuple[PipelineSpec, ...] = (
    SaleSpec(
        "아파트 매매",
        PropertyType.APT,
        C.ApartSaleCollector,
        RawApartSale,
        C.RawApartSaleCollector,
        P.ApartSalePreprocessor,
    ),
    RentSpec(
        "아파트 전월세",
        PropertyType.APT,
        C.ApartRentCollector,
        RawApartRent,
        C.RawApartRentCollector,
        P.ApartRentPreprocessor,
    ),
    SaleSpec(
        "오피스텔 매매",
        PropertyType.OFFICETEL,
        C.OfficetelSaleCollector,
        RawOfficetelSale,
        C.RawOfficetelSaleCollector,
        P.OfficetelSalePreprocessor,
    ),
    RentSpec(
        "오피스텔 전월세",
        PropertyType.OFFICETEL,
        C.OfficetelRentCollector,
        RawOfficetelRent,
        C.RawOfficetelRentCollector,
        P.OfficetelRentPreprocessor,
    ),
    SaleSpec(
        "연립다세대 매매",
        PropertyType.ROW_HOUSE,
        C.MultiflexSaleCollector,
        RawMultiflexSale,
        C.RawMultiflexSaleCollector,
        P.MultiflexSalePreprocessor,
    ),
    RentSpec(
        "연립다세대 전월세",
        PropertyType.ROW_HOUSE,
        C.MultiflexRentCollector,
        RawMultiflexRent,
        C.RawMultiflexRentCollector,
        P.MultiflexRentPreprocessor,
    ),
    SaleSpec(
        "단독다가구 매매",
        PropertyType.SINGLE_MULTI,
        C.SingleMultiFamilySaleCollector,
        RawSingleMultiFamilySale,
        C.RawSingleMultiFamilySaleCollector,
        P.SingleMultiFamilySalePreprocessor,
    ),
    RentSpec(
        "단독다가구 전월세",
        PropertyType.SINGLE_MULTI,
        C.SingleMultiFamilyRentCollector,
        RawSingleMultiFamilyRent,
        C.RawSingleMultiFamilyRentCollector,
        P.SingleMultiFamilyRentPreprocessor,
    ),
)


@dataclass(frozen=True)
class UnitResult:
    """갱신 단위 1건의 처리 결과."""

    spec_name: str
    sgg_cd: str
    deal_ymd: str
    raw_deleted: int
    raw_loaded: int
    deleted: int
    loaded: int


async def refresh_unit(spec: PipelineSpec, sgg_cd: str, deal_ymd: str) -> UnitResult:
    """갱신 단위 1건을 오픈API부터 정제 테이블까지 처리한다."""
    parse_deal_ymd(deal_ymd)  # API를 부르기 전에 형식부터 막는다.

    # 응답을 기다리는 동안 커넥션과 삭제 락을 쥐지 않도록 트랜잭션 밖에서 호출한다.
    response = await spec.api_collector(sgg_cd, deal_ymd).collect()

    async with session_scope() as session:
        raw_deleted = await RawTableCleaner(session, spec.raw_model).clean(
            deal_ymd, sgg_cd
        )
        raw_loaded = await RawDataLoader(session, spec.raw_model).load(response["rows"])

        # 응답이 아니라 raw를 다시 읽는다. 적재 과정의 키 정리를 거친 모습이 필요하고,
        # 같은 트랜잭션이라 방금 넣은 행이 그대로 보인다.
        rows = await spec.raw_collector(session).collect(deal_ymd, sgg_cd)
        payload = spec.preprocessor().preprocess(rows)

        deleted = await spec.cleaner(session).clean(
            spec.property_type, deal_ymd, sgg_cd
        )
        loaded = await spec.loader(session).load(payload)

    return UnitResult(
        spec.name, sgg_cd, deal_ymd, raw_deleted, raw_loaded, deleted, loaded
    )


async def refresh_legal_dong_codes() -> int:
    """시군구 목록의 출처인 법정동코드를 통째로 갱신한다."""
    response = await C.LegalDongCodeCollector().collect()
    async with session_scope() as session:
        await session.execute(delete(RawLegalDongCode.__table__))
        loaded = await RawDataLoader(session, RawLegalDongCode).load(response["rows"])
    logger.info("법정동코드 %d건 갱신", loaded)
    return loaded


async def sigungu_codes() -> list[str]:
    """실거래가 API의 LAWD_CD로 쓸 시군구 5자리 목록."""
    table = RawLegalDongCode.__table__
    async with session_scope() as session:
        result = await session.execute(
            select(table.c.region_cd).order_by(table.c.region_cd)
        )
        return [code[:5] for code in result.scalars() if code]


def recent_months(months: int = DEFAULT_MONTHS, today: date | None = None) -> list[str]:
    """이번 달부터 과거로 `months`개월의 계약년월을 최신순으로 만든다."""
    today = today or today_kst()
    year, month = today.year, today.month
    result = []
    for _ in range(months):
        result.append(f"{year:04d}{month:02d}")
        month -= 1
        if month == 0:
            year, month = year - 1, 12
    return result


def iter_units(
    sgg_list: Sequence[str], month_list: Sequence[str]
) -> Iterator[tuple[PipelineSpec, str, str]]:
    """갱신 단위를 최신 월부터 훑는다. 중간에 잘려도 최근 데이터가 먼저 반영된다."""
    for deal_ymd in month_list:
        for sgg_cd in sgg_list:
            for spec in PIPELINES:
                yield spec, sgg_cd, deal_ymd


async def refresh_all(
    months: int = DEFAULT_MONTHS, concurrency: int = DEFAULT_CONCURRENCY
) -> dict[str, int]:
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
    summary = {"units": len(units), "succeeded": 0, "failed": 0, "loaded": 0}

    async def run(spec: PipelineSpec, sgg_cd: str, deal_ymd: str) -> None:
        async with semaphore:
            try:
                result = await refresh_unit(spec, sgg_cd, deal_ymd)
            except Exception:
                # 단위 1건은 트랜잭션째 되돌아가므로 나머지를 멈추지 않고 넘어간다.
                summary["failed"] += 1
                logger.exception("갱신 실패: %s %s %s", spec.name, sgg_cd, deal_ymd)
            else:
                summary["succeeded"] += 1
                summary["loaded"] += result.loaded

    await asyncio.gather(*(run(*unit) for unit in units))

    logger.info(
        "갱신 종료: 성공 %d / 실패 %d / 적재 %d건",
        summary["succeeded"],
        summary["failed"],
        summary["loaded"],
    )
    return summary
