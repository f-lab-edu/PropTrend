"""갱신 단위 좌표를 로그에 자동으로 싣기 위한 실행 컨텍스트."""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar

# 단위마다 태스크가 따로 돌고, 태스크는 만들어질 때 컨텍스트를 복사해 간다. 그래서 동시에
# 4개가 돌아도 서로의 좌표를 덮어쓰지 않는다.
_unit: ContextVar[dict[str, str] | None] = ContextVar("refresh_unit", default=None)


@contextmanager
def unit_context(api_id: str, lawd_cd: str, deal_ymd: str) -> Generator[None]:
    """이 블록 안에서 남긴 모든 로그에 갱신 단위 좌표를 붙인다."""
    token = _unit.set({"api_id": api_id, "lawd_cd": lawd_cd, "deal_ymd": deal_ymd})
    try:
        yield
    finally:
        _unit.reset(token)


class UnitContextFilter(logging.Filter):
    """`unit_context` 안에서 만들어진 레코드에 좌표 필드를 붙인다."""

    def filter(self, record: logging.LogRecord) -> bool:
        unit = _unit.get()
        if unit:
            # 단위 밖(법정동코드 갱신, 시작·종료 요약)에서는 아무것도 붙이지 않는다.
            for key, value in unit.items():
                setattr(record, key, value)
        return True
