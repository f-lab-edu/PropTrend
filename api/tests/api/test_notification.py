"""인앱 알림 API 테스트."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.model import Complex, LegalDongCodeRawItem, Notification, TransactionEvent, User, UserSession
from src.services.user import SESSION_TTL

from .conftest import seed_rows
from .test_favorite import APARTMENT_ROW, OFFICETEL_ROW, UNAUTHENTICATED_HEADERS, assert_unauthenticated
from .test_user import USER_ROW, session_cookie_header, session_row

# 세션 범위 엔진의 커넥션을 같은 이벤트 루프에서 재사용한다.
pytestmark = pytest.mark.asyncio(loop_scope="session")

NOTIFICATIONS_URL = "/api/notifications"

TOKEN = "notification-valid"  # noqa: S105 - 테스트 시드 세션의 토큰

READ_AT = datetime(2026, 3, 1, tzinfo=UTC)


def read_url(notification_id: int) -> str:
    return f"{NOTIFICATIONS_URL}/{notification_id}/read"


@asynccontextmanager
async def seed_users_and_events(session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Any]]:
    """대상·다른 사용자, 아파트·오피스텔 단지와 단지별 처리된 이벤트, 지역명을 넣고 이름으로 돌려준다."""
    # 알림은 사용자나 이벤트가 지워질 때 FK의 ON DELETE CASCADE로 함께 지워진다.
    user_rows = [
        USER_ROW | {"email": "notification@example.com"},
        USER_ROW | {"email": "notification-other@example.com"},
    ]
    legal_dong_code_rows = [{"payload": {"sido_cd": "11", "sgg_cd": "110", "locatadd_nm": "서울특별시 종로구"}}]
    async with (
        seed_rows(session_factory, User, user_rows) as (user, other),
        seed_rows(session_factory, UserSession, [session_row(user, TOKEN, SESSION_TTL)]),
        seed_rows(session_factory, Complex, [APARTMENT_ROW, OFFICETEL_ROW]) as (apartment, officetel),
        seed_rows(session_factory, LegalDongCodeRawItem, legal_dong_code_rows),
        seed_rows(
            session_factory,
            TransactionEvent,
            [
                {"complex_id": apartment.id, "sale_count": 2, "rent_count": 1, "available_at": READ_AT},
                {"complex_id": officetel.id, "sale_count": 0, "rent_count": 3, "available_at": READ_AT},
            ],
        ) as (apartment_event, officetel_event),
    ):
        yield {
            "user": user,
            "other": other,
            "apartment": apartment,
            "officetel": officetel,
            "apartment_event": apartment_event,
            "officetel_event": officetel_event,
        }


async def read_ats(session_factory: async_sessionmaker[AsyncSession], *notification_ids: int) -> list[datetime | None]:
    """알림의 읽은 시각을 넘긴 순서대로 읽는다."""
    async with session_factory() as session, session.begin():
        read_at = dict(
            (
                await session.execute(
                    select(Notification.id, Notification.read_at).where(Notification.id.in_(notification_ids))
                )
            )
            .tuples()
            .all()
        )
    return [read_at[notification_id] for notification_id in notification_ids]


class TestGetNotificationList:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Any]]:
        async with seed_users_and_events(session_factory) as seeded:
            yield seeded

    async def test_lists_own_notifications_latest_first(
        self, client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession], seed: dict[str, Any]
    ) -> None:
        """내 알림만 최근에 생긴 순으로 주고, 읽지 않은 수는 페이지와 상관없이 센다."""
        user, other = seed["user"], seed["other"]
        apartment_event, officetel_event = seed["apartment_event"], seed["officetel_event"]
        rows = [
            {"user_id": user.id, "event_id": apartment_event.id, "read_at": READ_AT},
            {"user_id": user.id, "event_id": officetel_event.id},
            {"user_id": other.id, "event_id": apartment_event.id},
        ]
        async with seed_rows(session_factory, Notification, rows) as (read, unread, _):
            response = await client.get(NOTIFICATIONS_URL, params={"limit": 1}, headers=session_cookie_header(TOKEN))
            next_page = await client.get(
                NOTIFICATIONS_URL, params={"limit": 1, "offset": 1}, headers=session_cookie_header(TOKEN)
            )

        body = response.json()
        assert response.status_code == 200
        assert datetime.fromisoformat(body["items"][0].pop("created_at")) == unread.created_at
        assert body == {
            "items": [
                {
                    "id": unread.id,
                    "complex_id": seed["officetel"].id,
                    "property_type": "OFFICETEL",
                    "building_name": "청운오피스텔",
                    "sale_count": 0,
                    "rent_count": 3,
                    "read_at": None,
                    "address": "서울특별시 종로구 청운동 1",
                }
            ],
            "total": 2,
            "limit": 1,
            "offset": 0,
            "unread_count": 1,
        }
        [item] = next_page.json()["items"]
        assert (item["id"], item["complex_id"], item["sale_count"], item["rent_count"]) == (
            read.id,
            seed["apartment"].id,
            2,
            1,
        )
        assert datetime.fromisoformat(item["read_at"]) == READ_AT

    async def test_returns_empty_page(self, client: httpx.AsyncClient, seed: dict[str, Any]) -> None:
        """알림이 없으면 빈 목록과 0건을 준다."""
        response = await client.get(NOTIFICATIONS_URL, headers=session_cookie_header(TOKEN))

        assert response.status_code == 200
        assert response.json() == {"items": [], "total": 0, "limit": 20, "offset": 0, "unread_count": 0}

    @pytest.mark.parametrize("headers", UNAUTHENTICATED_HEADERS)
    async def test_rejects_unauthenticated(
        self, client: httpx.AsyncClient, seed: dict[str, Any], headers: dict[str, str]
    ) -> None:
        """쿠키가 없거나 세션이 무효하면 401 공통 오류 응답을 준다."""
        response = await client.get(NOTIFICATIONS_URL, headers=headers)

        assert_unauthenticated(response)

    @pytest.mark.parametrize(
        ("params", "loc", "error_type"),
        [
            pytest.param({"limit": 0}, "limit", "greater_than_equal", id="limit_zero"),
            pytest.param({"limit": 101}, "limit", "less_than_equal", id="limit_over"),
            pytest.param({"offset": -1}, "offset", "greater_than_equal", id="offset_negative"),
        ],
    )
    async def test_rejects_invalid_query(
        self, client: httpx.AsyncClient, seed: dict[str, Any], params: dict[str, int], loc: str, error_type: str
    ) -> None:
        """limit·offset이 범위를 벗어나면 422 공통 오류 응답을 준다."""
        response = await client.get(NOTIFICATIONS_URL, params=params, headers=session_cookie_header(TOKEN))

        assert response.status_code == 422
        [error] = response.json()["errors"]
        assert (error["loc"], error["type"]) == (["query", loc], error_type)


class TestMarkNotificationRead:
    @pytest_asyncio.fixture(scope="class", loop_scope="session")
    @classmethod
    async def seed(cls, session_factory: async_sessionmaker[AsyncSession]) -> AsyncIterator[dict[str, Any]]:
        async with seed_users_and_events(session_factory) as seeded:
            yield seeded

    async def test_marks_unread_notification(
        self, client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession], seed: dict[str, Any]
    ) -> None:
        """읽지 않은 알림이면 읽은 시각을 찍고 204를 준다. 다른 알림은 그대로 둔다."""
        rows = [
            {"user_id": seed["user"].id, "event_id": seed["apartment_event"].id},
            {"user_id": seed["user"].id, "event_id": seed["officetel_event"].id},
        ]
        async with seed_rows(session_factory, Notification, rows) as (target, untouched):
            before = datetime.now(UTC)
            response = await client.post(read_url(target.id), headers=session_cookie_header(TOKEN))
            target_read_at, untouched_read_at = await read_ats(session_factory, target.id, untouched.id)

        assert response.status_code == 204
        assert response.content == b""
        assert target_read_at is not None
        assert target_read_at >= before
        assert untouched_read_at is None

    async def test_keeps_first_read_time(
        self, client: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession], seed: dict[str, Any]
    ) -> None:
        """이미 읽은 알림이어도 204를 주고 처음 읽은 시각을 유지한다."""
        row = {"user_id": seed["user"].id, "event_id": seed["apartment_event"].id, "read_at": READ_AT}
        async with seed_rows(session_factory, Notification, [row]) as (notification,):
            response = await client.post(read_url(notification.id), headers=session_cookie_header(TOKEN))
            read_at = await read_ats(session_factory, notification.id)

        assert response.status_code == 204
        assert read_at == [READ_AT]

    @pytest.mark.parametrize("headers", UNAUTHENTICATED_HEADERS)
    async def test_rejects_unauthenticated(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        seed: dict[str, Any],
        headers: dict[str, str],
    ) -> None:
        """쿠키가 없거나 세션이 무효하면 401 공통 오류 응답을 주고 알림을 읽음으로 바꾸지 않는다."""
        row = {"user_id": seed["user"].id, "event_id": seed["apartment_event"].id}
        async with seed_rows(session_factory, Notification, [row]) as (notification,):
            response = await client.post(read_url(notification.id), headers=headers)
            read_at = await read_ats(session_factory, notification.id)

        assert_unauthenticated(response)
        assert read_at == [None]

    @pytest.mark.parametrize("owner", ["other", None], ids=["other_user", "missing"])
    async def test_rejects_unknown_notification(
        self,
        client: httpx.AsyncClient,
        session_factory: async_sessionmaker[AsyncSession],
        seed: dict[str, Any],
        owner: str | None,
    ) -> None:
        """없는 알림이나 다른 사용자의 알림이면 같은 404 공통 오류 응답을 주고 읽음으로 바꾸지 않는다."""
        row = {"user_id": seed["other"].id, "event_id": seed["apartment_event"].id}
        async with seed_rows(session_factory, Notification, [row]) as (notification,):
            notification_id = notification.id if owner else notification.id + 1
            response = await client.post(read_url(notification_id), headers=session_cookie_header(TOKEN))
            read_at = await read_ats(session_factory, notification.id)

        assert response.status_code == 404
        assert response.json() == {
            "message": "알림을 찾을 수 없습니다",
            "errors": [],
            "trace_id": response.headers["X-Trace-ID"],
        }
        assert read_at == [None]

    async def test_rejects_invalid_notification_id(self, client: httpx.AsyncClient, seed: dict[str, Any]) -> None:
        """알림 id가 1보다 작으면 422 공통 오류 응답을 준다."""
        response = await client.post(read_url(0), headers=session_cookie_header(TOKEN))

        assert response.status_code == 422
        [error] = response.json()["errors"]
        assert (error["loc"], error["type"]) == (["path", "notification_id"], "greater_than_equal")
