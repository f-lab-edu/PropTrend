import contextlib
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from app.db import engine
from app.scheduler import run_scheduled_pipeline


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_scheduled_pipeline, "cron", hour=4, minute=0)
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
