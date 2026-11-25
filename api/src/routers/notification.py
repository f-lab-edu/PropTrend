from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from ..dependencies import get_current_user, get_session
from ..model.user import User
from ..schemas.notification import NotificationListQuery, NotificationListResponse
from ..services.notification import get_notifications, read_notification

router = APIRouter()


@router.get(
    "",
    summary="알림 목록 조회",
    description=(
        "로그인한 사용자가 받은 즐겨찾기 단지의 신규 거래 알림을 최근에 생긴 순으로 반환합니다.\n\n"
        "- 정기 갱신에서 즐겨찾기 단지에 새로 신고된 거래가 보이면 회차가 끝난 뒤 단지마다 알림 하나가 생깁니다.\n"
        "- `sale_count`·`rent_count`는 그 회차에 새로 보인 매매·전월세 건수입니다. 해제된 거래는 세지 않습니다.\n"
        "- `read_at`은 처음 읽음으로 표시한 시각이고, 읽지 않았으면 null입니다.\n"
        "- `total`은 전체 알림 수, `unread_count`는 페이지와 상관없이 읽지 않은 알림 수입니다.\n"
        "- `limit`은 1~100(기본 20), `offset`은 0 이상입니다. 범위를 벗어나면 422를 반환합니다.\n"
        "- 쿠키가 없거나 세션이 만료·삭제되었으면 401을 반환합니다."
    ),
)
async def get_notification_list(
    query: Annotated[NotificationListQuery, Query()],
    user: Annotated[User, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> NotificationListResponse:
    """알림 목록을 조회한다."""
    async with session.begin():
        return await get_notifications(session, user_id=user.id, limit=query.limit, offset=query.offset)


@router.post(
    "/{notification_id}/read",
    response_class=Response,
    status_code=status.HTTP_204_NO_CONTENT,
    summary="알림 읽음 표시",
    description=(
        "로그인한 사용자의 알림 하나를 읽음으로 표시합니다.\n\n"
        "- 이미 읽은 알림이어도 오류 없이 204를 반환하고, 처음 읽은 시각을 유지합니다.\n"
        "- 쿠키가 없거나 세션이 만료·삭제되었으면 401을 반환합니다.\n"
        "- `notification_id`에 해당하는 알림이 없거나 다른 사용자의 알림이면 404를 반환합니다."
    ),
)
async def mark_notification_read(
    notification_id: Annotated[int, Path(ge=1)],
    user: Annotated[User, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> None:
    """알림을 읽음으로 표시한다."""
    async with session.begin():
        await read_notification(session, user_id=user.id, notification_id=notification_id)
