"""라우터가 공유하는 의존성"""

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Security
from fastapi.security import APIKeyCookie
from sqlalchemy.ext.asyncio import AsyncSession

from .db import get_session_factory
from .model.user import User
from .services.user import get_user_by_session_token

SESSION_COOKIE_NAME = "session_id"

# auto_error를 끄고 직접 예외를 올려 다른 오류와 같은 응답 구조로 내보낸다.
session_cookie = APIKeyCookie(name=SESSION_COOKIE_NAME, auto_error=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    """데이터베이스 세션 의존성 주입 함수"""
    async with get_session_factory()() as session:
        yield session


async def get_current_user(
    token: Annotated[str | None, Security(session_cookie)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> User:
    """세션 쿠키로 로그인한 사용자를 찾는다."""
    # 인증은 여러 라우터가 공유하는 관문이라 라우터 대신 여기서 짧은 읽기 트랜잭션을 연다.
    # expire_on_commit=False라 트랜잭션이 끝난 뒤에도 사용자 속성을 읽을 수 있고,
    # 라우터는 같은 세션으로 자기 트랜잭션을 새로 열 수 있다.
    async with session.begin():
        return await get_user_by_session_token(session, token)
