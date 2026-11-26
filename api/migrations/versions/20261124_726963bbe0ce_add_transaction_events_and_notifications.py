"""즐겨찾기 단지의 신규 거래 알림 대기열과 인앱 알림 테이블을 더한다.

Revision ID: 726963bbe0ce
Revises: 8c4267c96ef9
Create Date: 2026-11-24 21:20:02.696783

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "726963bbe0ce"
down_revision: str | Sequence[str] | None = "8c4267c96ef9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """스키마를 이 리비전으로 올린다."""
    op.create_table(
        "transaction_events",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("complex_id", sa.BigInteger(), nullable=False),
        sa.Column("sale_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("rent_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.SmallInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["complex_id"], ["complexes.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_transaction_events_pending",
        "transaction_events",
        ["available_at", "id"],
        unique=False,
        postgresql_where=sa.text("available_at IS NOT NULL AND processed_at IS NULL AND failed_at IS NULL"),
    )
    op.create_index(
        "uq_transaction_events_open_complex",
        "transaction_events",
        ["complex_id"],
        unique=True,
        postgresql_where=sa.text("available_at IS NULL"),
    )
    op.create_table(
        "notifications",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("event_id", sa.BigInteger(), nullable=False),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["event_id"], ["transaction_events.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "event_id"),
    )
    op.create_index("ix_notifications_user_id_id", "notifications", ["user_id", "id"], unique=False)


def downgrade() -> None:
    """스키마를 이전 리비전으로 되돌린다."""
    op.drop_index("ix_notifications_user_id_id", table_name="notifications")
    op.drop_table("notifications")
    op.drop_index(
        "uq_transaction_events_open_complex",
        table_name="transaction_events",
        postgresql_where=sa.text("available_at IS NULL"),
    )
    op.drop_index(
        "ix_transaction_events_pending",
        table_name="transaction_events",
        postgresql_where=sa.text("available_at IS NOT NULL AND processed_at IS NULL AND failed_at IS NULL"),
    )
    op.drop_table("transaction_events")
