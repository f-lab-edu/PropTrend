from typing import Annotated

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from ..dependencies import get_current_user, get_session
from ..model.user import User
from ..schemas.favorite import FavoriteComplexToggleRequest, FavoriteComplexToggleResponse
from ..services.favorite import toggle_favorite_complex

router = APIRouter()


@router.post(
    "/complexes",
    status_code=status.HTTP_200_OK,
    summary="단지 즐겨찾기 토글",
    description=(
        "로그인한 사용자의 단지 즐겨찾기를 토글합니다.\n\n"
        "- 즐겨찾기하지 않은 단지면 추가하고, 이미 즐겨찾기한 단지면 제거합니다.\n"
        "- `is_favorite`는 토글한 뒤의 상태입니다. `true`면 추가, `false`면 제거된 것입니다.\n"
        "- 쿠키가 없거나 세션이 만료·삭제되었으면 401을 반환합니다.\n"
        "- `complex_id`에 해당하는 단지가 없으면 404를 반환합니다."
    ),
)
async def toggle_favorite_complex_route(
    body: FavoriteComplexToggleRequest,
    user: Annotated[User, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> FavoriteComplexToggleResponse:
    """단지 즐겨찾기를 토글한다."""
    async with session.begin():
        return await toggle_favorite_complex(session, user.id, body.complex_id)
