from datetime import UTC, datetime

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..exceptions import NotificationNotFoundError
from ..model.complex import Complex
from ..model.notification import Notification, TransactionEvent
from ..schemas.notification import NotificationListResponse, NotificationResponse
from .prop_transaction import get_region_names


async def get_notifications(
    session: AsyncSession, *, user_id: int, limit: int, offset: int
) -> NotificationListResponse:
    """사용자의 알림을 최근에 생긴 순으로 조회한다."""
    total, unread_count = (
        await session.execute(
            select(func.count(), func.count().filter(Notification.read_at.is_(None))).where(
                Notification.user_id == user_id
            )
        )
    ).one()
    result = await session.execute(
        select(
            Notification.id,
            Notification.created_at,
            Notification.read_at,
            TransactionEvent.sale_count,
            TransactionEvent.rent_count,
            Complex,
        )
        .join(TransactionEvent, TransactionEvent.id == Notification.event_id)
        .join(Complex, Complex.id == TransactionEvent.complex_id)
        .where(Notification.user_id == user_id)
        .order_by(Notification.id.desc())
        .limit(limit)
        .offset(offset)
    )
    names = await get_region_names(session)
    return NotificationListResponse(
        items=[
            NotificationResponse(
                id=notification_id,
                complex_id=complex_.id,
                property_type=complex_.property_type,
                building_name=complex_.building_name,
                sale_count=sale_count,
                rent_count=rent_count,
                created_at=created_at,
                read_at=read_at,
                region_name=names.get((complex_.sido_code, complex_.sigungu_code)),
                umd_name=complex_.umd_name,
                jibun=complex_.jibun,
            )
            for notification_id, created_at, read_at, sale_count, rent_count, complex_ in result.tuples()
        ],
        total=total,
        limit=limit,
        offset=offset,
        unread_count=unread_count,
    )


async def read_notification(session: AsyncSession, *, user_id: int, notification_id: int) -> None:
    """사용자의 알림을 읽음으로 표시한다. 이미 읽었으면 처음 읽은 시각을 그대로 둔다."""
    # 다른 사용자의 알림은 없는 알림과 같게 답해 id로 남의 알림이 있는지 알 수 없게 한다.
    updated = await session.scalar(
        update(Notification)
        .where(Notification.id == notification_id, Notification.user_id == user_id)
        .values(read_at=func.coalesce(Notification.read_at, datetime.now(UTC)))
        .returning(Notification.id)
    )
    if updated is None:
        raise NotificationNotFoundError("알림을 찾을 수 없습니다")
