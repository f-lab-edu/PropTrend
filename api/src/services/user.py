import asyncio
import hashlib
import secrets
from datetime import UTC, datetime, timedelta

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..exceptions import DuplicateEmailError, InvalidCredentialsError, UnauthenticatedError
from ..model.user import User, UserSession

# Argon2id. 파라미터는 라이브러리 기본값(RFC 9106 저메모리 권장값: m=64MiB, t=3, p=4)을 쓴다.
password_hasher = PasswordHasher()
# 가입되지 않은 이메일로 로그인해도 실제 사용자와 같은 시간이 걸리도록 대신 검증할 해시.
# 파라미터가 다르면 검증 시간도 달라지므로 하드코딩하지 않고 같은 hasher로 만든다.
DUMMY_PASSWORD_HASH = password_hasher.hash(secrets.token_urlsafe())

# 활동과 무관하게 발급 시점부터 고정으로 만료된다. 쿠키 Max-Age도 이 값을 쓴다.
SESSION_TTL = timedelta(days=14)


def hash_session_token(token: str) -> str:
    """세션 토큰을 DB 저장·조회용 SHA-256 hex로 바꾼다."""
    # 256비트 무작위 토큰이라 대입 공격이 불가능하므로 비밀번호처럼 느린 해시를 쓸 필요가 없다.
    return hashlib.sha256(token.encode()).hexdigest()


async def create_user(session: AsyncSession, *, email: str, nickname: str, password: str) -> User:
    """요청 데이터로 사용자를 생성한다."""
    # 해싱은 수십 ms 걸리는 CPU 작업이라 이벤트 루프를 막지 않도록 스레드에서 돌린다.
    hashed_password = await asyncio.to_thread(password_hasher.hash, password)
    user = User(email=email, nickname=nickname, password=hashed_password)
    session.add(user)
    # 미리 조회하면 동시에 같은 이메일로 가입할 때 둘 다 통과하므로 UNIQUE 제약 위반으로 판단한다.
    try:
        await session.flush()
    except IntegrityError as e:
        raise DuplicateEmailError("이미 가입된 이메일입니다") from e
    return user


async def authenticate_user(session: AsyncSession, *, email: str, password: str) -> User:
    """이메일과 비밀번호가 저장된 사용자와 일치하는지 검증한다."""
    user = await session.scalar(select(User).where(User.email == email))
    # 가입 여부가 드러나지 않도록 사용자가 없어도 더미 해시로 검증을 돌려 응답 시간을 맞추고,
    # 이메일이 없을 때와 비밀번호가 틀렸을 때 같은 예외를 낸다.
    hashed_password = user.password if user is not None else DUMMY_PASSWORD_HASH
    try:
        await asyncio.to_thread(password_hasher.verify, hashed_password, password)
    except VerifyMismatchError as e:
        raise InvalidCredentialsError("이메일 또는 비밀번호가 올바르지 않습니다") from e
    if user is None:
        raise InvalidCredentialsError("이메일 또는 비밀번호가 올바르지 않습니다")
    return user


async def create_user_session(session: AsyncSession, *, user_id: int) -> str:
    """사용자의 새 세션을 만들고 쿠키에 담을 원본 토큰을 돌려준다."""
    # 만료된 세션은 조회 조건에서 걸러지지만 행이 쌓이지 않도록 로그인할 때 그 사용자 것만 지운다.
    await session.execute(
        delete(UserSession).where(UserSession.user_id == user_id, UserSession.expires_at <= func.now())
    )
    # 로그인마다 새 토큰을 발급해 로그인 전에 심어진 세션 ID를 이어 쓰는 세션 고정 공격을 막는다.
    token = secrets.token_urlsafe(32)
    session.add(
        UserSession(token_hash=hash_session_token(token), user_id=user_id, expires_at=datetime.now(UTC) + SESSION_TTL)
    )
    return token


async def get_user_by_session_token(session: AsyncSession, *, token: str | None) -> User:
    """세션 토큰에 해당하는 만료되지 않은 세션의 사용자를 찾는다."""
    if token is None:
        raise UnauthenticatedError("로그인이 필요합니다")
    user = await session.scalar(
        select(User)
        .join(UserSession, UserSession.user_id == User.id)
        .where(UserSession.token_hash == hash_session_token(token), UserSession.expires_at > func.now())
    )
    if user is None:
        raise UnauthenticatedError("로그인이 필요합니다")
    return user


async def delete_user_session(session: AsyncSession, *, token: str) -> None:
    """세션 토큰에 해당하는 세션을 지운다. 없으면 아무것도 하지 않는다."""
    await session.execute(delete(UserSession).where(UserSession.token_hash == hash_session_token(token)))
