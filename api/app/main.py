import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.db import engine

logger = logging.getLogger(__name__)

BACKGROUND_TASK_INTERVAL_SECONDS = 60


async def run_periodic_job() -> None:
    """일정 주기로 실행될 작업. 실제 로직은 이후 구현."""
    logger.info("periodic job executed")


async def periodic_job_loop() -> None:
    while True:
        try:
            await run_periodic_job()
        except Exception:
            logger.exception("periodic job failed")
        await asyncio.sleep(BACKGROUND_TASK_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    task = asyncio.create_task(periodic_job_loop())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await engine.dispose()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
