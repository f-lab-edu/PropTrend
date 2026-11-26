from datetime import datetime

from pydantic import Field

from ..model.prop_transaction import PropertyType
from . import PageResponse, PropTrendCoreModel
from .prop_transaction import AddressResponse


class NotificationListQuery(PropTrendCoreModel):
    """알림 목록 조회 조건."""

    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0)


class NotificationResponse(AddressResponse):
    """즐겨찾기 단지의 신규 거래 알림."""

    id: int
    complex_id: int
    property_type: PropertyType
    building_name: str | None
    sale_count: int
    rent_count: int
    created_at: datetime
    read_at: datetime | None


class NotificationListResponse(PageResponse):
    """알림 목록."""

    items: list[NotificationResponse]
    # 페이지와 상관없는 읽지 않은 알림 전체 건수. 화면의 배지에 쓴다.
    unread_count: int
