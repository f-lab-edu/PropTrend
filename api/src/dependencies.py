"""라우터가 공유하는 의존성"""

import os
import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Security
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from .db import get_session_factory
from .exceptions import InvalidApiKeyError

# auto_error를 끄고 직접 예외를 올려 다른 오류와 같은 응답 구조로 내보낸다.
api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)


async def verify_api_key(api_key: Annotated[str | None, Security(api_key_header)]) -> None:
    """X-API-KEY 헤더가 .env의 API_KEY와 같은지 확인한다."""
    # 설정이 빠지면 KeyError로 500을 내 모든 요청을 막는다.
    expected = os.environ["API_KEY"]
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected.encode()):
        raise InvalidApiKeyError("API 키가 올바르지 않습니다")


async def get_session() -> AsyncIterator[AsyncSession]:
    """데이터베이스 세션 의존성 주입 함수"""
    async with get_session_factory()() as session:
        yield session
