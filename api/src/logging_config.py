"""프로세스 공통 로깅 설정. 터미널에는 사람이 읽는 한 줄을, 파일에는 수집기가 읽는 JSON을 남긴다."""

import json
import logging
import logging.handlers
import os
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

DEFAULT_LEVEL = "INFO"

# 터미널 기본값. 수집기가 stdout을 보는 환경(ECS awslogs 등)에서는 json으로 바꾼다.
DEFAULT_STREAM_FORMAT = "text"

TEXT_FORMAT = "%(asctime)s %(levelname)-8s %(name)s %(message)s"

# 갱신 1회가 4,000단위라 로테이션이 없으면 파일이 계속 커진다.
MAX_BYTES = 10 * 1024 * 1024
BACKUP_COUNT = 5

# LogRecord가 기본으로 들고 있는 속성. 여기에 없는 것만 extra나 필터가 붙인 값으로 본다.
_RESERVED = frozenset(
    (
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "message",
        "module",
        "msecs",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "taskName",
        "thread",
        "threadName",
    )
)


def _extra_fields(record: logging.LogRecord) -> dict[str, object]:
    """필터와 `extra=`가 붙인 값만 고른다."""
    return {key: value for key, value in record.__dict__.items() if key not in _RESERVED}


class JsonFormatter(logging.Formatter):
    """로그 1건을 JSON 한 줄로 만든다. 수집기가 읽는 형식이다."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        # 필터와 extra=가 붙인 값을 먼저 깔고 기본 필드로 덮는다. 이름이 겹치면 기본 필드가
        # 이겨야 timestamp나 level의 의미가 흔들리지 않는다.
        payload: dict[str, object] = _extra_fields(record)

        payload |= {
            # 컨테이너 TZ(Asia/Seoul) 기준 오프셋이 붙는다. 수집기가 UTC로 환산할 수 있다.
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).astimezone().isoformat(),
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            exc_type, exc_value, _ = record.exc_info
            payload["error"] = {
                "type": exc_type.__name__ if exc_type else None,
                "message": str(exc_value),
                "stack": self.formatException(record.exc_info),
            }
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        # ensure_ascii=False여야 한글 메시지가 \uXXXX로 깨지지 않는다. default=str은 직렬화할
        # 수 없는 값이 extra로 들어와도 로깅이 죽지 않게 받아준다.
        return json.dumps(payload, ensure_ascii=False, default=str)


class TextFormatter(logging.Formatter):
    """사람이 읽는 한 줄. 필터가 붙인 컨텍스트를 메시지 끝에 덧붙인다."""

    def format(self, record: logging.LogRecord) -> str:
        text = super().format(record)
        extras = _extra_fields(record)
        if not extras:
            return text
        context = " ".join(f"{key}={value}" for key, value in extras.items())
        # 예외 트레이스백이 이미 붙어 있으면 그 앞, 메시지 줄 끝에 컨텍스트를 단다.
        head, newline, traceback = text.partition("\n")
        return f"{head} [{context}]{newline}{traceback}"


def _level() -> int:
    """`LOG_LEVEL`을 읽는다. 알 수 없는 이름이면 기본값으로 떨어진다."""
    name = os.environ.get("LOG_LEVEL", DEFAULT_LEVEL).upper()
    return logging.getLevelNamesMapping().get(name, logging.INFO)


def _stream_handler(service: str) -> logging.Handler:
    """터미널용 핸들러. `LOG_FORMAT=json`이면 파일과 같은 JSON으로 낸다."""
    handler = logging.StreamHandler(sys.stdout)
    if os.environ.get("LOG_FORMAT", DEFAULT_STREAM_FORMAT).lower() == "json":
        handler.setFormatter(JsonFormatter(service))
    else:
        handler.setFormatter(TextFormatter(TEXT_FORMAT))
    return handler


def _file_handler(path: str, service: str) -> logging.Handler | None:
    """파일용 핸들러. 형식은 항상 JSON이다. 경로를 쓸 수 없으면 None을 돌려준다."""
    try:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        handler = logging.handlers.RotatingFileHandler(
            path,
            maxBytes=MAX_BYTES,
            backupCount=BACKUP_COUNT,
            encoding="utf-8",
        )
    except OSError:
        # 로그 파일을 못 여는 것이 갱신을 통째로 못 도는 것보다 낫다. stdout에는 계속 나간다.
        logging.getLogger(__name__).warning("로그 파일을 열 수 없어 stdout에만 남긴다: %s", path)
        return None
    handler.setFormatter(JsonFormatter(service))
    return handler


def configure_logging(
    service: str,
    filters: Sequence[logging.Filter] = (),
    log_file: str | None = None,
) -> None:
    """루트 로거에 핸들러를 붙인다. 프로세스 진입점에서 한 번만 부른다."""
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(_level())

    stream = _stream_handler(service)
    root.addHandler(stream)

    # 파일 핸들러를 만들다 실패하면 경고를 남기는데, stdout 핸들러가 먼저 붙어 있어야 그 경고가 보인다.
    path = log_file or os.environ.get("LOG_FILE")
    file_handler = _file_handler(path, service) if path else None
    if file_handler is not None:
        root.addHandler(file_handler)

    # 필터는 로거가 아니라 핸들러에 단다. 로거에 달면 하위 로거에서 전파된 레코드에는
    # 적용되지 않아 정작 파이프라인 모듈들의 로그에 컨텍스트가 빠진다.
    for handler in root.handlers:
        for log_filter in filters:
            handler.addFilter(log_filter)

    # httpx는 INFO에서 요청 URL을 통째로 남기는데, 실거래가 API는 serviceKey를
    # 쿼리스트링으로 받으므로 그대로 두면 인증키가 로그에 찍힌다.
    logging.getLogger("httpx").setLevel(logging.WARNING)
