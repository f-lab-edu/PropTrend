import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from .db import create_tables, dispose_engine
from .jobs.pipeline import DEFAULT_CONCURRENCY, DEFAULT_MONTHS
from .jobs.runner import RefreshAlreadyRunning, RefreshRunner, RefreshState
from .jobs.utils import KST

load_dotenv()

# httpx는 INFO에서 요청 URL을 통째로 남기는데, 실거래가 API는 serviceKey를
# 쿼리스트링으로 받으므로 그대로 두면 인증키가 로그에 찍힌다.
logging.getLogger("httpx").setLevel(logging.WARNING)

scheduler = AsyncIOScheduler(timezone=KST)
runner = RefreshRunner()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # 갱신이 첫 삽을 뜨기 전에 테이블이 있어야 한다. DB가 비어 있는 새 환경에서도
    # 컴포즈만으로 뜨게 하려는 것이므로, 실패하면 그대로 기동을 멈춘다.
    await create_tables()

    # 등록하는 작업은 하나뿐이다. 수집·적재·가공의 순서는 refresh_all 안의 await
    # 순서로 표현되며, 스케줄러는 "언제 시작할지"만 정한다. 단계를 개별 작업으로
    # 쪼개 등록하면 세션이 갈라져 삭제와 적재가 다른 트랜잭션이 된다.
    scheduler.add_job(
        runner.run_scheduled,
        CronTrigger(hour=3, minute=0, timezone=KST),
        id="refresh_all",
        name="실거래가 수집 → raw 적재 → 정제 갱신",
        # 하루치가 24시간 안에 끝나지 않아도 다음 실행이 겹치지 않게 한다. 겹치면
        # 같은 갱신 단위를 두 트랜잭션이 동시에 삭제·적재한다. 수동 실행과의
        # 겹침은 스케줄러가 모르므로 runner가 따로 막는다.
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

    months: int = Field(
        DEFAULT_MONTHS, ge=1, le=24, description="이번 달부터 거슬러 갱신할 개월 수"
    )
    concurrency: int = Field(
        DEFAULT_CONCURRENCY,
        ge=1,
        le=8,
        # 단위마다 세션을 하나씩 쓰므로 커넥션 풀 크기를 넘기면 대기만 길어진다.
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
    def of(cls, state: RefreshState) -> "RefreshStatus":
        return cls(**vars(state))


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/jobs/refresh",
    status_code=status.HTTP_202_ACCEPTED,
    summary="갱신 파이프라인 강제 실행",
)
async def trigger_refresh(request: RefreshRequest | None = None) -> RefreshStatus:
    """예약을 기다리지 않고 갱신을 지금 시작한다."""
    request = request or RefreshRequest()
    try:
        state = runner.start("manual", request.months, request.concurrency)
    except RefreshAlreadyRunning as exc:
        # 겹쳐 돌리면 같은 갱신 단위를 두 트랜잭션이 동시에 삭제·적재한다.
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    return RefreshStatus.of(state)


@app.get("/jobs/refresh", summary="갱신 파이프라인 실행 상태")
async def refresh_status() -> RefreshStatus:
    """진행 중이면 시작 시각을, 끝났으면 마지막 실행의 집계나 오류를 돌려준다."""
    return RefreshStatus.of(runner.state)
