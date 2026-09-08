import contextlib
import logging
import os
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from app.db import async_session_factory, engine
from data_collection.collectors import (
    COLLECTION_MONTHS,
    DailyLimitReachedError,
    DataCollector,
    RTMSAptRentCollector,
    RTMSAptTradeCollector,
    RTMSOffiRentCollector,
    RTMSOffiTradeCollector,
    RTMSRHRentCollector,
    RTMSRHTradeCollector,
    RTMSSHRentCollector,
    RTMSSHTradeCollector,
    StanReginCdCollector,
)
from data_collection.loader import (
    DataLoader,
    LegalStandardCodeLoader,
    RentTransactionLoader,
    SaleTransactionLoader,
)
from data_collection.processor import (
    DataProcessor,
    LegalDongCodeProcessor,
    RTMSAptRentProcessor,
    RTMSAptTradeProcessor,
    RTMSOffiRentProcessor,
    RTMSOffiTradeProcessor,
    RTMSRHRentProcessor,
    RTMSRHTradeProcessor,
    RTMSSHRentProcessor,
    RTMSSHTradeProcessor,
)

logger = logging.getLogger(__name__)

SERVICE_KEY_ENV = "DATA_GO_KR_SERVICE_KEY"

KST = timezone(timedelta(hours=9))

# 실거래 신고는 계약 후 30일 이내라 변경분 대부분이 최근 몇 달에 몰린다. 매일은 그
# 구간만 짧게 훑고, 뒤늦은 계약 해제까지 반영하는 전체 구간은 주 1회만 돈다.
RECENT_COLLECTION_MONTHS = 3
FULL_COLLECTION_MONTHS = COLLECTION_MONTHS

EXTRACTION_JOB_HOUR = 4
EXTRACTION_JOB_MINUTE = 0

# 두 작업은 같은 갱신 단위를 건드리므로 절대 겹쳐서는 안 된다. 요일을 나눠 배타적으로
# 돌게 하면 잠금 없이도 동시 실행이 원천적으로 불가능하다.
RECENT_JOB_DAYS = "mon-sat"
FULL_JOB_DAYS = "sun"


@dataclass(frozen=True)
class Pipeline:
    """한 데이터 소스의 수집 -> 가공 -> 적재 모듈 묶음."""

    name: str
    collector: type[DataCollector]
    processor: type[DataProcessor]
    loader: type[DataLoader]


# 법정동코드를 맨 앞에 둔다. 실거래가 수집 모듈이 요청 파라미터로 쓰는 시군구 코드
# 목록을 법정동코드 마스터 테이블에서 읽으므로, 행정구역 개편이 있어도 같은 실행 안에서
# 마스터가 먼저 갱신되고 새 시군구까지 수집된다.
PIPELINES: tuple[Pipeline, ...] = (
    Pipeline(
        "legal_dong_code",
        StanReginCdCollector,
        LegalDongCodeProcessor,
        LegalStandardCodeLoader,
    ),
    Pipeline(
        "apart_sale",
        RTMSAptTradeCollector,
        RTMSAptTradeProcessor,
        SaleTransactionLoader,
    ),
    Pipeline(
        "apart_rent",
        RTMSAptRentCollector,
        RTMSAptRentProcessor,
        RentTransactionLoader,
    ),
    Pipeline(
        "officetel_sale",
        RTMSOffiTradeCollector,
        RTMSOffiTradeProcessor,
        SaleTransactionLoader,
    ),
    Pipeline(
        "officetel_rent",
        RTMSOffiRentCollector,
        RTMSOffiRentProcessor,
        RentTransactionLoader,
    ),
    Pipeline(
        "multiflex_sale",
        RTMSRHTradeCollector,
        RTMSRHTradeProcessor,
        SaleTransactionLoader,
    ),
    Pipeline(
        "multiflex_rent",
        RTMSRHRentCollector,
        RTMSRHRentProcessor,
        RentTransactionLoader,
    ),
    Pipeline(
        "single_multi_family_sale",
        RTMSSHTradeCollector,
        RTMSSHTradeProcessor,
        SaleTransactionLoader,
    ),
    Pipeline(
        "single_multi_family_rent",
        RTMSSHRentCollector,
        RTMSSHRentProcessor,
        RentTransactionLoader,
    ),
)


async def run_pipeline(
    pipeline: Pipeline, service_key: str, collection_months: int
) -> int:
    """수집 -> 가공 -> 적재를 순서대로 수행하고 적재된 행 수를 반환한다."""
    started = time.monotonic()

    collector = pipeline.collector(
        service_key, async_session_factory, collection_months=collection_months
    )
    raw_items = await collector.collect()
    logger.info("%s: 원본 %d건 수집", pipeline.name, len(raw_items))

    rows = pipeline.processor().process(raw_items)
    logger.info("%s: %d행 가공", pipeline.name, len(rows))

    # 적재 모듈이 (부동산 유형, 시군구, 계약년월) 단위로 기존 행을 지우고 다시 넣으므로,
    # 같은 구간을 몇 번 수집해도 결과가 같고 나중에 갱신된 신고분도 그대로 반영된다.
    loaded = await pipeline.loader(async_session_factory).load(rows)
    logger.info(
        "%s: %d행 적재 (%.1f분)",
        pipeline.name,
        loaded,
        (time.monotonic() - started) / 60,
    )
    return loaded


async def run_data_extraction_job(collection_months: int) -> None:
    """공공 데이터 API에서 최근 collection_months개월 실거래가를 수집해 DB에 적재한다."""
    service_key = os.environ.get(SERVICE_KEY_ENV)
    if not service_key:
        logger.error("%s가 설정되지 않아 수집을 건너뜁니다", SERVICE_KEY_ENV)
        return

    logger.info("데이터 추출 작업 시작: 최근 %d개월", collection_months)
    started = time.monotonic()
    total = 0
    failures: list[str] = []
    for pipeline in PIPELINES:
        try:
            total += await run_pipeline(pipeline, service_key, collection_months)
        except DailyLimitReachedError:
            # 일일 호출 한도는 서비스 키가 아니라 API별로 걸리므로, 한 소스가 한도에
            # 도달해도 나머지 소스는 자기 몫을 그대로 쓸 수 있다. 다음 소스로 넘어간다.
            logger.warning(
                "%s: 일일 호출 한도에 도달해 이 소스를 건너뜁니다", pipeline.name
            )
            failures.append(pipeline.name)
        except Exception:
            # 한 소스의 실패가 나머지 소스까지 막지 않도록 기록만 남기고 넘어간다.
            # 적재가 멱등하므로 다음 실행에서 그대로 복구된다.
            logger.exception("%s: 처리 실패", pipeline.name)
            failures.append(pipeline.name)

    logger.info(
        "데이터 추출 작업 종료: 최근 %d개월, %d행 적재, %.1f분 소요",
        collection_months,
        total,
        (time.monotonic() - started) / 60,
    )
    if failures:
        logger.error("실패한 소스 %d건: %s", len(failures), ", ".join(failures))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    # 수집 대상이 국내 실거래가이고 수집 모듈도 KST 기준으로 대상 월을 정하므로,
    # 스케줄도 서버 로컬 시간이 아닌 KST로 고정한다.
    scheduler = AsyncIOScheduler(timezone=KST)
    for job_id, day_of_week, collection_months in (
        ("data_extraction_recent", RECENT_JOB_DAYS, RECENT_COLLECTION_MONTHS),
        ("data_extraction_full", FULL_JOB_DAYS, FULL_COLLECTION_MONTHS),
    ):
        scheduler.add_job(
            run_data_extraction_job,
            "cron",
            day_of_week=day_of_week,
            hour=EXTRACTION_JOB_HOUR,
            minute=EXTRACTION_JOB_MINUTE,
            args=(collection_months,),
            id=job_id,
            # 한 번 도는 데 오래 걸리는 작업이라 다음 실행 시각을 넘길 수 있다. 겹쳐 돌면
            # 같은 갱신 단위를 동시에 지우고 넣게 되므로 한 번에 하나만 돌린다.
            max_instances=1,
            coalesce=True,
        )
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        with contextlib.suppress(Exception):
            await engine.dispose()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
