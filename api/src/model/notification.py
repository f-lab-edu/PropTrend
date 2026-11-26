"""즐겨찾기 단지의 신규 거래를 알리는 대기열과 인앱 알림 모델.

정기 갱신(`airflow/dags/rtms`)이 정제 단위를 갈아끼우는 트랜잭션 안에서 신규 거래 수를 `transaction_events`에
더하고(아웃박스), 회차가 끝나면 한꺼번에 처리 가능으로 연다. 알림 워커(`src/notification_worker.py`)는 이 테이블을
대기열로 읽어 즐겨찾기한 사용자마다 `notifications`를 만든다. 별도 메시지 브로커 없이 같은 DB에 두므로 정제 결과와
이벤트, 알림 생성과 처리 완료 표시가 각각 한 트랜잭션으로 묶인다.
"""

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

# 회차가 끝나기 전의 행. 정제 단위가 신규 건수를 더하는 대상이다.
TRANSACTION_EVENT_OPEN_WHERE = "available_at IS NULL"
# 워커가 가져갈 수 있는 행. 처리를 마쳤거나 재시도를 다 쓴 행은 빠진다.
TRANSACTION_EVENT_PENDING_WHERE = "available_at IS NOT NULL AND processed_at IS NULL AND failed_at IS NULL"


class TransactionEvent(Base):
    """정기 갱신이 단지 하나에서 새로 찾은 거래 수. 알림 워커의 대기열 항목이다."""

    __tablename__ = "transaction_events"
    __table_args__ = (
        # 회차가 끝나기 전에는 단지마다 열린 행을 하나만 두고, 매매·전월세 정제가 같은 행에 건수를 더한다.
        # 회차 id 대신 열림 여부로 묶어, 발행이 끝난 뒤 다시 돌린 정제가 이미 처리된 행에 건수를 더해 잃지 않게 한다.
        Index(
            "uq_transaction_events_open_complex",
            "complex_id",
            unique=True,
            postgresql_where=text(TRANSACTION_EVENT_OPEN_WHERE),
        ),
        # 처리가 끝난 행은 계속 쌓이므로 대기 중인 행만 담아 워커가 매번 테이블 전체를 훑지 않게 한다.
        Index(
            "ix_transaction_events_pending",
            "available_at",
            "id",
            postgresql_where=text(TRANSACTION_EVENT_PENDING_WHERE),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    complex_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("complexes.id", ondelete="CASCADE"))
    sale_count: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    rent_count: Mapped[int] = mapped_column(Integer, server_default=text("0"))

    # NULL이면 회차가 아직 끝나지 않아 건수가 더 늘 수 있다. 재시도할 때는 다음 시도 시각으로 미룬다.
    available_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(SmallInteger, server_default=text("0"))
    last_error: Mapped[str | None] = mapped_column(Text)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # 재시도를 다 쓴 행. 대기열에서 빠지고 원인은 last_error에 남는다.
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class Notification(Base):
    """사용자 한 명에게 보여 주는 인앱 알림. 즐겨찾기 단지의 신규 거래 이벤트 하나를 가리킨다."""

    __tablename__ = "notifications"
    __table_args__ = (
        # 워커가 같은 이벤트를 다시 처리해도 사용자마다 알림이 하나만 생긴다.
        UniqueConstraint("user_id", "event_id"),
        # 사용자의 알림을 최근 순으로 읽는다.
        Index("ix_notifications_user_id_id", "user_id", "id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"))
    event_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("transaction_events.id", ondelete="CASCADE"))
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
