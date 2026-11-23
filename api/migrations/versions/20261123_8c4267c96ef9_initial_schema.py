"""모델 정의로 만들던 기존 스키마를 첫 리비전으로 옮긴다.

기존 DB는 create_all로 이미 이 상태이므로 upgrade 대신 `alembic stamp 8c4267c96ef9`로 기록만 남긴다.

Revision ID: 8c4267c96ef9
Revises:
Create Date: 2026-11-23 20:09:04.040431

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "8c4267c96ef9"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """스키마를 이 리비전으로 올린다."""
    op.create_table(
        "complexes",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column(
            "property_type",
            sa.Enum(
                "APT", "OFFICETEL", "ROW_HOUSE", "SINGLE_MULTI", name="property_type", native_enum=False, length=20
            ),
            nullable=False,
        ),
        sa.Column("sido_code", sa.CHAR(length=2), nullable=False),
        sa.Column("sigungu_code", sa.CHAR(length=3), nullable=False),
        sa.Column("umd_name", sa.String(length=60), nullable=True),
        sa.Column("jibun", sa.String(length=20), nullable=True),
        sa.Column("building_name", sa.String(length=100), nullable=True),
        sa.Column("build_year", sa.SmallInteger(), nullable=True),
        sa.Column("apartment_serial_number", sa.String(length=20), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_complexes_apartment",
        "complexes",
        ["apartment_serial_number"],
        unique=True,
        postgresql_where=sa.text("property_type = 'APT'"),
    )
    op.create_index(
        "uq_complexes_officetel",
        "complexes",
        ["sido_code", "sigungu_code", "umd_name", "jibun", "building_name", "build_year"],
        unique=True,
        postgresql_where=sa.text("property_type = 'OFFICETEL'"),
        postgresql_nulls_not_distinct=True,
    )
    op.create_table(
        "legal_dong_code_raw_items",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("collected_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "market_price_movers",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("change_rate", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("latest_id", sa.BigInteger(), nullable=False),
        sa.Column("sido_code", sa.CHAR(length=2), nullable=False),
        sa.Column("sigungu_code", sa.CHAR(length=3), nullable=False),
        sa.Column("umd_name", sa.String(length=60), nullable=True),
        sa.Column("jibun", sa.String(length=20), nullable=True),
        sa.Column("building_name", sa.String(length=100), nullable=True),
        sa.Column("apartment_dong", sa.String(length=50), nullable=True),
        sa.Column("floor", sa.SmallInteger(), nullable=True),
        sa.Column("build_year", sa.SmallInteger(), nullable=True),
        sa.Column("exclusive_use_area", sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column("deal_date", sa.Date(), nullable=False),
        sa.Column("deal_amount", sa.BigInteger(), nullable=False),
        sa.Column("dealing_type", sa.String(length=20), nullable=True),
        sa.Column("cancel_deal_date", sa.Date(), nullable=True),
        sa.Column("complex_id", sa.BigInteger(), nullable=True),
        sa.Column("previous_id", sa.BigInteger(), nullable=False),
        sa.Column("previous_deal_date", sa.Date(), nullable=False),
        sa.Column("previous_deal_amount", sa.BigInteger(), nullable=False),
        sa.Column("previous_floor", sa.SmallInteger(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "market_snapshots",
        sa.Column("name", sa.String(length=50), nullable=False),
        sa.Column("base_date", sa.Date(), nullable=False),
        sa.Column("refreshed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("name"),
    )
    op.create_table(
        "raw_load_progress",
        sa.Column("api_id", sa.String(length=40), nullable=False),
        sa.Column("yyyymm", sa.String(length=6), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("loaded_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("api_id", "yyyymm"),
    )
    op.create_table(
        "regions",
        sa.Column("sido_code", sa.CHAR(length=2), nullable=False),
        sa.Column("sigungu_code", sa.CHAR(length=3), nullable=False),
        sa.Column("sido_name", sa.String(length=20), nullable=False),
        sa.Column("sigungu_name", sa.String(length=50), nullable=False),
        sa.PrimaryKeyConstraint("sido_code", "sigungu_code"),
    )
    op.create_table(
        "rent_transactions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column(
            "property_type",
            sa.Enum(
                "APT", "OFFICETEL", "ROW_HOUSE", "SINGLE_MULTI", name="property_type", native_enum=False, length=20
            ),
            nullable=False,
        ),
        sa.Column("house_type", sa.String(length=10), nullable=True),
        sa.Column("sido_code", sa.CHAR(length=2), nullable=False),
        sa.Column("sigungu_code", sa.CHAR(length=3), nullable=False),
        sa.Column("umd_name", sa.String(length=60), nullable=True),
        sa.Column("jibun", sa.String(length=20), nullable=True),
        sa.Column("building_name", sa.String(length=100), nullable=True),
        sa.Column("deal_date", sa.Date(), nullable=False),
        sa.Column("exclusive_use_area", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("floor", sa.SmallInteger(), nullable=True),
        sa.Column("build_year", sa.SmallInteger(), nullable=True),
        sa.Column("total_floor_area", sa.Numeric(precision=14, scale=4), nullable=True),
        sa.Column("apartment_serial_number", sa.String(length=20), nullable=True),
        sa.Column("deposit", sa.BigInteger(), nullable=False),
        sa.Column("monthly_rent", sa.BigInteger(), nullable=False),
        sa.Column("contract_term", sa.String(length=20), nullable=True),
        sa.Column("contract_type", sa.String(length=10), nullable=True),
        sa.Column("renewal_right_used", sa.String(length=10), nullable=True),
        sa.Column("previous_deposit", sa.BigInteger(), nullable=True),
        sa.Column("previous_monthly_rent", sa.BigInteger(), nullable=True),
        sa.Column("sigungu_name", sa.String(length=30), nullable=True),
        sa.Column("road_address_detail", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_rent_transactions_refresh_unit",
        "rent_transactions",
        ["property_type", "sido_code", "sigungu_code", "deal_date"],
        unique=False,
    )
    op.create_table(
        "rtms_raw_items",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("api_id", sa.String(length=40), nullable=False),
        sa.Column("lawd_cd", sa.String(length=5), nullable=False),
        sa.Column("deal_ymd", sa.String(length=6), nullable=False),
        sa.Column("collected_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_rtms_raw_items_refresh_unit", "rtms_raw_items", ["api_id", "deal_ymd", "lawd_cd"], unique=False)
    op.create_table(
        "sale_transactions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column(
            "property_type",
            sa.Enum(
                "APT", "OFFICETEL", "ROW_HOUSE", "SINGLE_MULTI", name="property_type", native_enum=False, length=20
            ),
            nullable=False,
        ),
        sa.Column("house_type", sa.String(length=10), nullable=True),
        sa.Column("sido_code", sa.CHAR(length=2), nullable=False),
        sa.Column("sigungu_code", sa.CHAR(length=3), nullable=False),
        sa.Column("umd_name", sa.String(length=60), nullable=True),
        sa.Column("jibun", sa.String(length=20), nullable=True),
        sa.Column("building_name", sa.String(length=100), nullable=True),
        sa.Column("deal_date", sa.Date(), nullable=False),
        sa.Column("exclusive_use_area", sa.Numeric(precision=10, scale=4), nullable=True),
        sa.Column("floor", sa.SmallInteger(), nullable=True),
        sa.Column("build_year", sa.SmallInteger(), nullable=True),
        sa.Column("total_floor_area", sa.Numeric(precision=14, scale=4), nullable=True),
        sa.Column("apartment_serial_number", sa.String(length=20), nullable=True),
        sa.Column("plottage_area", sa.Numeric(precision=14, scale=4), nullable=True),
        sa.Column("land_area", sa.Numeric(precision=14, scale=4), nullable=True),
        sa.Column("deal_amount", sa.BigInteger(), nullable=False),
        sa.Column("dealing_type", sa.String(length=20), nullable=True),
        sa.Column("estate_agent_sigungu_name", sa.String(length=100), nullable=True),
        sa.Column("seller_type", sa.String(length=20), nullable=True),
        sa.Column("buyer_type", sa.String(length=20), nullable=True),
        sa.Column("cancel_deal_type", sa.String(length=10), nullable=True),
        sa.Column("cancel_deal_date", sa.Date(), nullable=True),
        sa.Column("registration_date", sa.Date(), nullable=True),
        sa.Column("apartment_dong", sa.String(length=50), nullable=True),
        sa.Column("land_leasehold_type", sa.CHAR(length=1), nullable=True),
        sa.Column("sigungu_name", sa.String(length=30), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_sale_transactions_refresh_unit",
        "sale_transactions",
        ["property_type", "sido_code", "sigungu_code", "deal_date"],
        unique=False,
    )
    op.create_table(
        "users",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("nickname", sa.String(length=30), nullable=False),
        sa.Column("password", sa.String(length=255), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("email"),
    )
    op.create_table(
        "favorite_complexes",
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("complex_id", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["complex_id"], ["complexes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "complex_id"),
    )
    op.create_index(op.f("ix_favorite_complexes_complex_id"), "favorite_complexes", ["complex_id"], unique=False)
    op.create_table(
        "user_sessions",
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("token_hash"),
    )
    op.create_index(op.f("ix_user_sessions_user_id"), "user_sessions", ["user_id"], unique=False)


def downgrade() -> None:
    """스키마를 이전 리비전으로 되돌린다."""
    op.drop_index(op.f("ix_user_sessions_user_id"), table_name="user_sessions")
    op.drop_table("user_sessions")
    op.drop_index(op.f("ix_favorite_complexes_complex_id"), table_name="favorite_complexes")
    op.drop_table("favorite_complexes")
    op.drop_table("users")
    op.drop_index("ix_sale_transactions_refresh_unit", table_name="sale_transactions")
    op.drop_table("sale_transactions")
    op.drop_index("ix_rtms_raw_items_refresh_unit", table_name="rtms_raw_items")
    op.drop_table("rtms_raw_items")
    op.drop_index("ix_rent_transactions_refresh_unit", table_name="rent_transactions")
    op.drop_table("rent_transactions")
    op.drop_table("regions")
    op.drop_table("raw_load_progress")
    op.drop_table("market_snapshots")
    op.drop_table("market_price_movers")
    op.drop_table("legal_dong_code_raw_items")
    op.drop_index(
        "uq_complexes_officetel",
        table_name="complexes",
        postgresql_where=sa.text("property_type = 'OFFICETEL'"),
        postgresql_nulls_not_distinct=True,
    )
    op.drop_index("uq_complexes_apartment", table_name="complexes", postgresql_where=sa.text("property_type = 'APT'"))
    op.drop_table("complexes")
