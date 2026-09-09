from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from fastapi import FastAPI

from .db import dispose_engine

load_dotenv()

KST = ZoneInfo("Asia/Seoul")

scheduler = AsyncIOScheduler(timezone=KST)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown()
        await dispose_engine()


app = FastAPI(title="prop-trend API", lifespan=lifespan)


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}
