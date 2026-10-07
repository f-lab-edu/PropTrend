from typing import Annotated

from fastapi import APIRouter, Depends, Path, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from ..dependencies import get_current_user, get_session
from ..model.user import User
from ..schemas.favorite import FavoriteComplexAddRequest, FavoriteComplexAddResponse, FavoriteComplexResponse
from ..services.favorite import add_favorite_complex, get_favorite_complexes, remove_favorite_complex

router = APIRouter()


@router.get(
    "/complexes",
    summary="즐겨찾기한 단지 목록 조회",
    description=(
        "로그인한 사용자가 즐겨찾기한 단지를 최근에 추가한 순으로 반환합니다.\n\n"
        "- 즐겨찾기한 단지가 없으면 빈 목록을 반환합니다.\n"
        "- 쿠키가 없거나 세션이 만료·삭제되었으면 401을 반환합니다."
    ),
)
async def get_favorite_complexes_route(
    user: Annotated[User, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> list[FavoriteComplexResponse]:
    """즐겨찾기한 단지 목록을 조회한다."""
    async with session.begin():
        return await get_favorite_complexes(session, user_id=user.id)


@router.post(
    "/complexes",
    status_code=status.HTTP_201_CREATED,
    summary="단지 즐겨찾기 추가",
    description=(
        "로그인한 사용자의 단지 즐겨찾기를 추가합니다.\n\n"
        "- 이미 즐겨찾기한 단지여도 오류 없이 201을 반환합니다.\n"
        "- 쿠키가 없거나 세션이 만료·삭제되었으면 401을 반환합니다.\n"
        "- `complex_id`에 해당하는 단지가 없으면 404를 반환합니다."
    ),
)
async def add_favorite_complex_route(
    body: FavoriteComplexAddRequest,
    user: Annotated[User, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> FavoriteComplexAddResponse:
    """단지 즐겨찾기를 추가한다."""
    async with session.begin():
        return await add_favorite_complex(session, user_id=user.id, complex_id=body.complex_id)


@router.delete(
    "/complexes/{complex_id}",
    response_class=Response,
    status_code=status.HTTP_204_NO_CONTENT,
    summary="단지 즐겨찾기 제거",
    description=(
        "로그인한 사용자의 단지 즐겨찾기를 제거합니다.\n\n"
        "- 즐겨찾기하지 않은 단지나 없는 단지여도 오류 없이 204를 반환합니다.\n"
        "- 쿠키가 없거나 세션이 만료·삭제되었으면 401을 반환합니다."
    ),
)
async def remove_favorite_complex_route(
    complex_id: Annotated[int, Path(ge=1)],
    user: Annotated[User, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> None:
    """단지 즐겨찾기를 제거한다."""
    async with session.begin():
        await remove_favorite_complex(session, user_id=user.id, complex_id=complex_id)
