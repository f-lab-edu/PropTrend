from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import Depends, FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from .db import create_tables, dispose_engine
from .dependencies import verify_api_key
from .exception_handlers import (
    http_exception_handler,
    prop_trend_error_handler,
    request_validation_error_handler,
    unhandled_exception_handler,
)
from .exceptions import PropTrendError
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


app = FastAPI(
    title="prop-trend API",
    lifespan=lifespan,
    exception_handlers={
        PropTrendError: prop_trend_error_handler,
        StarletteHTTPException: http_exception_handler,
        RequestValidationError: request_validation_error_handler,
        Exception: unhandled_exception_handler,
    },
)

app.middleware("http")(log_requests)

app.include_router(
    prop_transaction_router,
    prefix="/api/prop-transactions",
    dependencies=[Depends(verify_api_key)],
)


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}
