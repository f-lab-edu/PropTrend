"""실거래 갱신과 무관하게 유지되는 아파트·오피스텔 단지 모델.

실거래 테이블은 갱신 단위마다 지우고 다시 넣어 거래 id가 매번 바뀐다. 즐겨찾기처럼 오래 붙들어야 하는
참조는 이 테이블을 가리킨다. 단지 행은 upsert만 하고 지우지 않는다.

실거래 테이블과는 FK 대신 자연키로 잇는다. 묶는 기준은 실거래가 추이(`PRICE_TREND_GROUP_COLUMNS`)에서
전용면적만 뺀 것이다.
"""

from datetime import datetime

from sqlalchemy import CHAR, BigInteger, DateTime, Index, SmallInteger, String, func, text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base
from .prop_transaction import PropertyType, PropertyTypeColumn

# 단지 개념이 분명한 유형. 연립다세대·단독다가구는 같은 매물을 묶는 기준이 약해 단지를 만들지 않는다.
COMPLEX_PROPERTY_TYPES = frozenset({PropertyType.APT, PropertyType.OFFICETEL})

# 유형별 단지 식별 컬럼. upsert의 충돌 대상과 부분 유니크 인덱스가 같은 값을 쓴다.
# 아파트는 단지명이 과거 거래까지 소급해 바뀌므로 이름이 아니라 단지 일련번호로 식별한다.
# 오피스텔은 단지 일련번호가 없어 지번·건물명으로 건물을 잡고, 같은 자리에 다시 지은 건물은 건축년도로 가른다.
COMPLEX_KEY_COLUMNS: dict[PropertyType, tuple[str, ...]] = {
    PropertyType.APT: ("apartment_serial_number",),
    PropertyType.OFFICETEL: ("sido_code", "sigungu_code", "umd_name", "jibun", "building_name", "build_year"),
}

# 부분 유니크 인덱스의 조건. ON CONFLICT가 인덱스를 추론하려면 같은 조건을 넘겨야 한다.
COMPLEX_INDEX_WHERE: dict[PropertyType, str] = {
    PropertyType.APT: "property_type = 'APT'",
    PropertyType.OFFICETEL: "property_type = 'OFFICETEL'",
}


class Complex(Base):
    """아파트·오피스텔 단지."""

    __tablename__ = "complexes"
    __table_args__ = (
        Index(
            "uq_complexes_apartment",
            *COMPLEX_KEY_COLUMNS[PropertyType.APT],
            unique=True,
            postgresql_where=text(COMPLEX_INDEX_WHERE[PropertyType.APT]),
        ),
        # 실거래가 추이와 같게 원본에 빈 값이 있는 오피스텔도 NULL끼리 같은 단지로 본다.
        Index(
            "uq_complexes_officetel",
            *COMPLEX_KEY_COLUMNS[PropertyType.OFFICETEL],
            unique=True,
            postgresql_where=text(COMPLEX_INDEX_WHERE[PropertyType.OFFICETEL]),
            postgresql_nulls_not_distinct=True,
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    property_type: Mapped[PropertyType] = mapped_column(PropertyTypeColumn)

    sido_code: Mapped[str] = mapped_column(CHAR(2))
    sigungu_code: Mapped[str] = mapped_column(CHAR(3))
    umd_name: Mapped[str | None] = mapped_column(String(60))
    jibun: Mapped[str | None] = mapped_column(String(20))
    # 아파트는 가장 최근 거래의 단지명으로 갱신된다.
    building_name: Mapped[str | None] = mapped_column(String(100))
    build_year: Mapped[int | None] = mapped_column(SmallInteger)
    # 단지 일련번호(aptSeq). 아파트만.
    apartment_serial_number: Mapped[str | None] = mapped_column(String(20))

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
