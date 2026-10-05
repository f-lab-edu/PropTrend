from typing import Annotated

from fastapi import APIRouter, Depends, Response, Security, status
from sqlalchemy.ext.asyncio import AsyncSession

from ..dependencies import SESSION_COOKIE_NAME, get_current_user, get_session, session_cookie
from ..model.user import User
from ..schemas.user import LoginRequest, SignUpRequest, UserResponse
from ..services.user import SESSION_TTL, authenticate_user, create_user, create_user_session, delete_user_session

router = APIRouter()

# 발급과 삭제의 속성이 다르면 브라우저가 다른 쿠키로 보고 지우지 않으므로 같은 값을 쓴다.
# HttpOnly: JS에서 읽지 못하게 해 XSS로 토큰이 새지 않게 한다. Secure: HTTPS로만 보낸다(localhost는 예외).
# SameSite=Lax: 다른 사이트에서 보내는 POST 등에는 쿠키가 실리지 않아 CSRF를 막는다.
SESSION_COOKIE_OPTIONS = {"path": "/", "httponly": True, "secure": True, "samesite": "lax"}


@router.post(
    "/sign-up",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="회원가입",
    description=(
        "이메일, 닉네임, 비밀번호로 사용자를 생성합니다.\n\n"
        "- 생성된 사용자 정보를 `password`를 제외하고 반환합니다.\n"
        "- 이미 가입된 이메일이면 409를 반환합니다."
    ),
)
async def sign_up(
    body: SignUpRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    """사용자를 생성한다."""
    # TODO: API 호출 제한. 전체 API 공통 제한에 더해 IP 기준으로 더 엄격한 제한을 건다.
    async with session.begin():
        return await create_user(session, body)


@router.post(
    "/login",
    response_class=Response,
    status_code=status.HTTP_200_OK,
    summary="로그인",
    description=(
        "이메일과 비밀번호를 검증하고 세션 쿠키를 발급합니다.\n\n"
        f"- 성공하면 응답 본문 없이 200과 함께 `{SESSION_COOKIE_NAME}` 쿠키(HttpOnly, 14일)를 내려줍니다.\n"
        "- 이메일이 없거나 비밀번호가 틀리면 구분 없이 401을 반환합니다."
    ),
)
async def login(
    body: LoginRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
    response: Response,
) -> None:
    """이메일과 비밀번호를 검증하고 세션 쿠키를 발급한다."""
    # TODO: API 호출 제한. 전체 API 공통 제한에 더해 IP 기준과 이메일(계정) 기준 제한을 건다.
    async with session.begin():
        user = await authenticate_user(session, body)
        token = await create_user_session(session, user)
    response.set_cookie(SESSION_COOKIE_NAME, token, max_age=int(SESSION_TTL.total_seconds()), **SESSION_COOKIE_OPTIONS)


@router.post(
    "/logout",
    response_class=Response,
    status_code=status.HTTP_204_NO_CONTENT,
    summary="로그아웃",
    description=(
        "현재 세션을 삭제하고 세션 쿠키를 지웁니다.\n\n- 쿠키가 없거나 이미 만료·삭제된 세션이어도 204를 반환합니다."
    ),
)
async def logout(
    token: Annotated[str | None, Security(session_cookie)],
    session: Annotated[AsyncSession, Depends(get_session)],
    response: Response,
) -> None:
    """현재 세션을 삭제하고 세션 쿠키를 지운다."""
    if token is not None:
        async with session.begin():
            await delete_user_session(session, token)
    response.delete_cookie(SESSION_COOKIE_NAME, **SESSION_COOKIE_OPTIONS)


@router.get(
    "/me",
    response_model=UserResponse,
    summary="내 정보 조회",
    description=(
        "세션 쿠키로 로그인한 사용자 정보를 반환합니다.\n\n- 쿠키가 없거나 세션이 만료·삭제되었으면 401을 반환합니다."
    ),
)
async def get_me(user: Annotated[User, Depends(get_current_user)]):
    """로그인한 사용자 정보를 반환한다."""
    return user
