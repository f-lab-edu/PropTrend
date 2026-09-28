from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI

from .db import create_tables, dispose_engine
from .logging_config import configure_logging
from .middlewares import RequestIdFilter, log_requests
from .routers.prop_transaction import router as prop_transaction_router

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    # 요청 로그에 request_id가 붙도록 필터를 핸들러에 단다.
    configure_logging("api", filters=[RequestIdFilter()])
    # 스키마 생성은 API 서버가 맡는다. 갱신 파이프라인은 별도 프로세스로 돌면서
    # 테이블이 이미 있다고 전제하므로, 만드는 주체가 여기 하나로 고정된다.
    await create_tables()
    try:
        yield
    finally:
        await dispose_engine()


app = FastAPI(title="prop-trend API", lifespan=lifespan)

app.middleware("http")(log_requests)

app.include_router(prop_transaction_router, prefix="/api/prop-transactions")


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}
