from fastapi import Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .exceptions import PropTrendError
from .middlewares import TRACE_ID_HEADER
from .schemas import ErrorDetail, ErrorResponse


async def prop_trend_error_handler(request: Request, exc: PropTrendError) -> JSONResponse:
    """애플리케이션 예외를 공통 오류 응답으로 바꾼다."""
    trace_id = getattr(request.state, "request_id", None)
    content = ErrorResponse(message=str(exc), trace_id=trace_id).model_dump(mode="json")
    return JSONResponse(status_code=exc.status_code, content=content)


async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    """라우팅 404·405와 HTTPException을 공통 오류 응답으로 바꾼다."""
    trace_id = getattr(request.state, "request_id", None)
    content = ErrorResponse(message=str(exc.detail), trace_id=trace_id).model_dump(mode="json")
    # 405의 Allow처럼 HTTP 규약상 함께 가야 하는 헤더를 잃지 않는다.
    return JSONResponse(status_code=exc.status_code, content=content, headers=exc.headers)


async def request_validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """요청 검증 실패를 항목별 사유와 함께 공통 오류 응답으로 바꾼다."""
    errors = [ErrorDetail.model_validate(error) for error in exc.errors()]
    trace_id = getattr(request.state, "request_id", None)
    content = ErrorResponse(message="요청 값이 올바르지 않습니다", errors=errors, trace_id=trace_id).model_dump(
        mode="json"
    )
    return JSONResponse(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, content=content)


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """처리되지 않은 예외를 공통 오류 응답으로 바꾼다."""
    # 내부 사유는 응답에 싣지 않는다. 핸들러가 끝나면 Starlette가 예외를 다시 올려 서버 로그에 남는다.
    trace_id = getattr(request.state, "request_id", None)
    content = ErrorResponse(message="서버 내부 오류가 발생했습니다", trace_id=trace_id).model_dump(mode="json")
    # 이 핸들러의 응답은 로그 미들웨어를 거치지 않으므로 trace_id 헤더를 여기서 직접 단다.
    headers = {TRACE_ID_HEADER: trace_id} if trace_id else None
    return JSONResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=content, headers=headers)
