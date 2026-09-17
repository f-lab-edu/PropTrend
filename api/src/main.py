from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI

from .db import create_tables, dispose_engine

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    # 스키마 생성은 API 서버가 맡는다. 갱신 파이프라인은 별도 프로세스로 돌면서
    # 테이블이 이미 있다고 전제하므로, 만드는 주체가 여기 하나로 고정된다.
    await create_tables()
    try:
        yield
    finally:
        await dispose_engine()


app = FastAPI(title="prop-trend API", lifespan=lifespan)


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}
