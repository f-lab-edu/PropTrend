import enum
from datetime import date
from decimal import Decimal

from sqlalchemy import (
    CHAR,
    BigInteger,
    Date,
    Enum,
    Index,
    Numeric,
    SmallInteger,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.model.base import Base


class PropertyType(str, enum.Enum):
    APT = "APT"
    OFFICETEL = "OFFICETEL"
    ROW_HOUSE = "ROW_HOUSE"
    SINGLE_MULTI = "SINGLE_MULTI"


def _refresh_unit_index(table_name: str) -> Index:
    """갱신 단위((부동산 유형, 시군구, 계약년월)) 조회/삭제용 인덱스.

    적재가 이 단위로 기존 행을 지우고 다시 넣으므로, DELETE가 매번 전체 스캔을 타지
    않도록 같은 컬럼 순서로 인덱스를 둔다. 시군구·기간별 조회에도 그대로 쓰인다."""
    return Index(
        f"ix_{table_name}_refresh_unit",
        "property_type",
        "sido_code",
        "sigungu_code",
        "deal_date",
    )


class PropTransactionMixin:
    """매매/전월세 실거래가 테이블의 공통 컬럼."""

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    property_type: Mapped[PropertyType] = mapped_column(
        Enum(PropertyType, native_enum=False), nullable=False
    )
    house_type: Mapped[str | None] = mapped_column(String(10))
    sido_code: Mapped[str] = mapped_column(CHAR(2), nullable=False)
    sigungu_code: Mapped[str] = mapped_column(CHAR(3), nullable=False)
    umd_name: Mapped[str] = mapped_column(String(60), nullable=False)
    jibun: Mapped[str | None] = mapped_column(String(20))
    building_name: Mapped[str | None] = mapped_column(String(100))
    deal_date: Mapped[date] = mapped_column(Date, nullable=False)
    exclusive_use_area: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    floor: Mapped[int | None] = mapped_column(SmallInteger)
    build_year: Mapped[int | None] = mapped_column(SmallInteger)


class SaleTransaction(PropTransactionMixin, Base):
    """아파트/오피스텔/연립다세대/단독·다가구 매매 실거래가."""

    __tablename__ = "sale_transactions"
    __table_args__ = (_refresh_unit_index("sale_transactions"),)

    total_floor_area: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    plottage_area: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    land_area: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    deal_amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    dealing_type: Mapped[str | None] = mapped_column(String(20))
    estate_agent_sigungu_name: Mapped[str | None] = mapped_column(String(100))
    seller_type: Mapped[str | None] = mapped_column(String(20))
    buyer_type: Mapped[str | None] = mapped_column(String(20))
    cancel_deal_type: Mapped[str | None] = mapped_column(String(10))
    cancel_deal_date: Mapped[date | None] = mapped_column(Date)
    registration_date: Mapped[date | None] = mapped_column(Date)
    apartment_dong: Mapped[str | None] = mapped_column(String(50))
    land_leasehold_type: Mapped[str | None] = mapped_column(CHAR(1))
    sigungu_name: Mapped[str | None] = mapped_column(String(30))


class RentTransaction(PropTransactionMixin, Base):
    """아파트/오피스텔/연립다세대/단독·다가구 전월세 실거래가."""

    __tablename__ = "rent_transactions"
    __table_args__ = (_refresh_unit_index("rent_transactions"),)

    total_floor_area: Mapped[Decimal | None] = mapped_column(Numeric(10, 4))
    deposit: Mapped[int] = mapped_column(BigInteger, nullable=False)
    monthly_rent: Mapped[int] = mapped_column(BigInteger, nullable=False)
    contract_term: Mapped[str | None] = mapped_column(String(20))
    contract_type: Mapped[str | None] = mapped_column(String(10))
    renewal_right_used: Mapped[str | None] = mapped_column(String(10))
    previous_deposit: Mapped[int | None] = mapped_column(BigInteger)
    previous_monthly_rent: Mapped[int | None] = mapped_column(BigInteger)
    sigungu_name: Mapped[str | None] = mapped_column(String(30))
    apartment_serial_number: Mapped[str | None] = mapped_column(String(20))
    road_address_detail: Mapped[dict | None] = mapped_column(JSONB)


class LegalStandardCode(Base):
    """법정동코드(시군구 단위) 마스터."""

    __tablename__ = "legal_standard_codes"

    code: Mapped[str] = mapped_column(CHAR(5), primary_key=True)
    name: Mapped[str] = mapped_column(String(60), nullable=False)
