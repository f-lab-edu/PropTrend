"""API 요청·응답 로그 미들웨어"""

import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from contextvars import ContextVar

from fastapi import Request, Response

logger = logging.getLogger(__name__)

TRACE_ID_HEADER = "X-Trace-ID"

# 요청마다 하위 태스크가 컨텍스트를 복사해 가므로, 동시에 들어온 요청끼리 값을 덮어쓰지 않는다.
_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)


class RequestIdFilter(logging.Filter):
    """요청 처리 중에 만들어진 레코드에 request_id 필드를 붙인다."""

    def filter(self, record: logging.LogRecord) -> bool:
        request_id = _request_id.get()
        if request_id:
            # 요청 밖(기동·종료 등)에서는 아무것도 붙이지 않는다.
            record.request_id = request_id
        return True


async def log_requests(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
    """요청 1건에 request_id를 부여하고 메서드·경로·상태 코드·처리 시간을 남긴다."""
    request_id = uuid.uuid4().hex
    token = _request_id.set(request_id)
    # 처리되지 않은 예외의 핸들러는 이 미들웨어 바깥에서 돌아 ContextVar가 이미 되돌려진 뒤다.
    # scope에 딸린 request.state는 그 핸들러까지 이어지므로 예외 핸들러는 여기서 값을 읽는다.
    request.state.request_id = request_id
    started = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        # 클라이언트가 문의할 때 서버 로그와 맞춰볼 수 있게 응답 헤더로 돌려준다.
        response.headers[TRACE_ID_HEADER] = request_id
        return response
    finally:
        # 처리되지 않은 예외는 전역 핸들러가 이 미들웨어 바깥에서 500으로 바꾸므로 기본값 500으로 남긴다.
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        logger.info(
            "%s %s %s %.2fms",
            request.method,
            request.url.path,
            status_code,
            elapsed_ms,
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": status_code,
                "elapsed_ms": elapsed_ms,
                "client_ip": request.client.host if request.client else None,
            },
        )
        _request_id.reset(token)
