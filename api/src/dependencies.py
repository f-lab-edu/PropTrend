"""라우터가 공유하는 의존성"""

import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from .config import Settings, get_settings
from .db import get_session_factory
from .exceptions import InvalidApiKeyError

# auto_error를 끄고 직접 예외를 올려 다른 오류와 같은 응답 구조로 내보낸다.
api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)


async def verify_api_key(
    api_key: Annotated[str | None, Security(api_key_header)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> None:
    """X-API-KEY 헤더가 설정의 API_KEY와 같은지 확인한다."""
    expected = settings.api_key.get_secret_value()
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected.encode()):
        raise InvalidApiKeyError("API 키가 올바르지 않습니다")


async def get_session() -> AsyncIterator[AsyncSession]:
    """데이터베이스 세션 의존성 주입 함수"""
    async with get_session_factory()() as session:
        yield session
