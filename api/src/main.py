import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from .db import create_tables, dispose_engine
from .jobs.pipeline import DEFAULT_CONCURRENCY, DEFAULT_MONTHS, MAX_MONTHS
from .jobs.runner import RefreshAlreadyRunningError, RefreshRunner, RefreshState
from .jobs.utils import KST
from .security import API_KEY_ENV, require_api_key

load_dotenv()

logger = logging.getLogger(__name__)

# httpx는 INFO에서 요청 URL을 통째로 남기는데, 실거래가 API는 serviceKey를
# 쿼리스트링으로 받으므로 그대로 두면 인증키가 로그에 찍힌다.
logging.getLogger("httpx").setLevel(logging.WARNING)

scheduler = AsyncIOScheduler(timezone=KST)
runner = RefreshRunner()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await create_tables()

    if not os.environ.get(API_KEY_ENV):
        # 기동은 막지 않는다. 키가 없다는 건 수동 실행을 못 쓴다는 뜻이지,
        # 매일 03시 갱신까지 멈춰야 한다는 뜻은 아니다.
        logger.warning("%s가 없어 /jobs/refresh를 잠근다", API_KEY_ENV)

    scheduler.add_job(
        runner.run_scheduled,
        CronTrigger(hour=3, minute=0, timezone=KST),
        id="refresh_all",
        name="부동산 실거래 데이터 갱신 작업",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
        replace_existing=True,
    )
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown()
        await dispose_engine()


app = FastAPI(title="prop-trend API", lifespan=lifespan)


class RefreshRequest(BaseModel):
    """수동 실행에서 조정할 수 있는 값. 생략하면 예약 실행과 같은 조건으로 돈다."""

    months: int = Field(DEFAULT_MONTHS, ge=1, le=MAX_MONTHS, description="이번 달부터 거슬러 갱신할 개월 수")
    concurrency: int = Field(
        DEFAULT_CONCURRENCY,
        ge=1,
        le=8,
        description="동시에 처리할 갱신 단위 수",
    )


class RefreshStatus(BaseModel):
    """진행 중이거나 마지막으로 끝난 갱신 1회의 상태."""

    running: bool
    trigger: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    summary: dict[str, int] | None = None
    error: str | None = None

    @classmethod
    def of(cls, state: RefreshState) -> RefreshStatus:
        return cls(**vars(state))


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/jobs/refresh",
    status_code=status.HTTP_202_ACCEPTED,
    summary="갱신 파이프라인 강제 실행",
    dependencies=[Depends(require_api_key)],
)
async def trigger_refresh(request: RefreshRequest | None = None) -> RefreshStatus:
    """예약을 기다리지 않고 갱신을 지금 시작한다."""
    request = request or RefreshRequest()
    try:
        state = runner.start("manual", request.months, request.concurrency)
    except RefreshAlreadyRunningError as exc:
        # 겹쳐 돌리면 같은 갱신 단위를 두 트랜잭션이 동시에 삭제·적재한다.
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    return RefreshStatus.of(state)


# 실행 상태에는 마지막 실패의 예외 메시지가 담기므로 조회도 함께 막는다.
@app.get(
    "/jobs/refresh",
    summary="갱신 파이프라인 실행 상태",
    dependencies=[Depends(require_api_key)],
)
async def refresh_status() -> RefreshStatus:
    """진행 중이면 시작 시각을, 끝났으면 마지막 실행의 집계나 오류를 돌려준다."""
    return RefreshStatus.of(runner.state)
