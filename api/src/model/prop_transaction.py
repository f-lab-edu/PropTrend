"""raw 8종 테이블을 가공해 채우는 실거래 정제 테이블 모델.

설계 근거는 `scripts/docs/schema/draft.md`. 요약하면,

- 매매/전월세는 필드 집합이 근본적으로 다르므로 테이블을 분리한다
  (`sale_transactions` / `rent_transactions`).
- 부동산 유형 4종(아파트/오피스텔/연립다세대/단독·다가구)은 `property_type`
  컬럼으로 한 테이블 안에서 구분하고, 유형별 고유 필드는 nullable 컬럼으로 둔다.
- raw 계층이 문자열 그대로 적재한 값을 이 계층에서 타입·단위까지 정규화한다.
  (지역코드 5자리 → 시도 2 + 시군구 3 분리, 계약일 3필드 → `DATE` 1개,
  금액 문자열 `"36,900"` → 원 단위 `BIGINT`, 빈 문자열 → `NULL`)
"""

from datetime import date, datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    CHAR,
    BigInteger,
    Date,
    DateTime,
    Enum,
    Index,
    Numeric,
    SmallInteger,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, declared_attr, mapped_column

from .base import Base


class PropertyType(StrEnum):
    """부동산 유형. 어느 raw 테이블에서 넘어온 행인지를 구분한다."""

    APT = "APT"
    OFFICETEL = "OFFICETEL"
    ROW_HOUSE = "ROW_HOUSE"
    SINGLE_MULTI = "SINGLE_MULTI"


# native_enum=False로 두어 DB에는 VARCHAR(20)로 떨어진다. 값 집합이 아직 확정
# 전이라(draft.md 5절) PostgreSQL ENUM 타입을 만들면 값 추가 때마다 ALTER TYPE이
# 필요해지기 때문. 값 검증은 파이썬 쪽 PropertyType이 담당한다(validate_strings).
PropertyTypeColumn = Enum(
    PropertyType,
    native_enum=False,
    length=20,
    validate_strings=True,
    name="property_type",
)


class TransactionMixin:
    """매매/전월세 테이블이 공유하는 컬럼(draft.md 2절).

    raw 테이블과 달리 이 계층은 가공을 마친 값만 받으므로, 모든 유형에 반드시
    존재하는 컬럼은 NOT NULL로 잠근다. 특정 유형에만 있는 필드는 nullable이며
    주석에 어느 유형에서 채워지는지를 적어둔다.

    갱신을 "적재 대상 날짜 범위 삭제 → 재적재" 순으로 수행하므로 중복 방지용
    자연키(unique)는 두지 않는다. 인덱스도 실제 쿼리에서 필요해진 것만 둔다.
    """

    @declared_attr.directive
    def __table_args__(cls) -> tuple[Any, ...]:
        # 갱신 단위(유형, 시군구, 계약년월) 삭제 전용. 이 인덱스가 없으면 삭제 대상을
        # 찾는 데만 매번 테이블 전체를 훑는다(1,600만행 기준 Seq Scan 2.2GB/654ms →
        # Index Scan 59버퍼/2.4ms). 갱신은 하루 1만 회 이상 돈다.
        # deal_date를 맨 뒤에 둬야 범위 조건이 선두 등치 조건들 뒤에서 좁혀지고,
        # 시군구를 생략하는 전국 삭제에서도 property_type이 선두에 남는다.
        return (
            Index(
                f"ix_{cls.__tablename__}_refresh_unit",
                "property_type",
                "sido_code",
                "sigungu_code",
                "deal_date",
            ),
        )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True, sort_order=-1)

    property_type: Mapped[PropertyType] = mapped_column(PropertyTypeColumn, sort_order=-1)
    # 원본 houseType(연립/다세대/단독/다가구). 아파트·오피스텔은 NULL.
    house_type: Mapped[str | None] = mapped_column(String(10), sort_order=-1)

    # 원본 sggCd 5자리를 시도 2자리 + 시군구 3자리로 분리한 값.
    # raw_legal_dong_code의 sido_cd/sgg_cd와 자릿수가 그대로 맞아 변환 없이 조인된다.
    sido_code: Mapped[str] = mapped_column(CHAR(2), sort_order=-1)
    sigungu_code: Mapped[str] = mapped_column(CHAR(3), sort_order=-1)

    umd_name: Mapped[str] = mapped_column(String(60), sort_order=-1)
    # 단독·다가구 전월세는 원본에 지번 필드 자체가 없어 항상 NULL.
    jibun: Mapped[str | None] = mapped_column(String(20), sort_order=-1)
    # aptNm/offiNm/mhouseNm 통합. 단독·다가구는 건물명 개념이 없어 NULL.
    building_name: Mapped[str | None] = mapped_column(String(100), sort_order=-1)

    # dealYear + dealMonth + dealDay 통합.
    deal_date: Mapped[date] = mapped_column(Date, sort_order=-1)

    # 전용면적(㎡)/층. 단독·다가구는 개념이 없어 NULL.
    exclusive_use_area: Mapped[float | None] = mapped_column(Numeric(10, 4), sort_order=-1)
    floor: Mapped[int | None] = mapped_column(SmallInteger, sort_order=-1)
    build_year: Mapped[int | None] = mapped_column(SmallInteger, sort_order=-1)

    # 연면적. 단독·다가구에만 존재하며 매매/전월세 양쪽 원본에 모두 있다.
    # 전용면적과 달리 필지 단위 면적이라 자릿수가 크고 원본에 오류값이 섞여 있어
    # (10,4)로는 넘친다. 실측 최대 123,101.43(smf_rent).
    total_floor_area: Mapped[float | None] = mapped_column(Numeric(14, 4), sort_order=-1)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), sort_order=100)


class SaleTransaction(Base, TransactionMixin):
    """매매 실거래(draft.md 3절).

    출처: raw_apart_sale / raw_officetel_sale / raw_multiflex_sale /
    raw_single_multi_family_sale.
    """

    __tablename__ = "sale_transactions"

    # 대지면적. 단독·다가구 매매만. 실측 최대 959,676으로 (10,4) 한계에 붙어 있다.
    plottage_area: Mapped[float | None] = mapped_column(Numeric(14, 4))
    # 대지권면적. 연립다세대 매매만. 원본에 448,128,851.9(약 448km²)라는 명백한
    # 오류값이 15건 있어 (10,4)로는 적재가 실패한다. 값을 버리지 않고 넓혀서 받는다.
    land_area: Mapped[float | None] = mapped_column(Numeric(14, 4))

    # 거래금액(원). 원본은 만원 단위 콤마 문자열이라 콤마 제거 후 ×10,000.
    deal_amount: Mapped[int] = mapped_column(BigInteger)

    # 중개거래/직거래.
    dealing_type: Mapped[str | None] = mapped_column(String(20))
    estate_agent_sigungu_name: Mapped[str | None] = mapped_column(String(100))
    # 개인/법인/공공기관/기타.
    seller_type: Mapped[str | None] = mapped_column(String(20))
    buyer_type: Mapped[str | None] = mapped_column(String(20))

    # 해제여부와 해제사유발생일.
    cancel_deal_type: Mapped[str | None] = mapped_column(String(10))
    cancel_deal_date: Mapped[date | None] = mapped_column(Date)

    # 등기일자. 아파트·연립다세대 매매만(오피스텔·단독다가구는 원본에 필드 없음).
    registration_date: Mapped[date | None] = mapped_column(Date)

    # 아파트 매매만.
    apartment_dong: Mapped[str | None] = mapped_column(String(50))
    # 토지임대부 아파트 여부(Y/N). 아파트 매매만.
    land_leasehold_type: Mapped[str | None] = mapped_column(CHAR(1))

    # 시군구명. 오피스텔 매매만 원본에 포함.
    sigungu_name: Mapped[str | None] = mapped_column(String(30))


class RentTransaction(Base, TransactionMixin):
    """전월세 실거래(draft.md 4절).

    출처: raw_apart_rent / raw_officetel_rent / raw_multiflex_rent /
    raw_single_multi_family_rent.
    """

    __tablename__ = "rent_transactions"

    # 보증금/월세(원). 매매와 동일하게 ×10,000. 전세는 monthly_rent가 0.
    deposit: Mapped[int] = mapped_column(BigInteger)
    monthly_rent: Mapped[int] = mapped_column(BigInteger)

    # 계약기간(예: "24.09~26.09"). 원본이 자유 형식이라 문자열 그대로 둔다.
    contract_term: Mapped[str | None] = mapped_column(String(20))
    # 신규/갱신.
    contract_type: Mapped[str | None] = mapped_column(String(10))
    # 갱신요구권 사용여부.
    renewal_right_used: Mapped[str | None] = mapped_column(String(10))

    # 종전계약 보증금/월세(원).
    previous_deposit: Mapped[int | None] = mapped_column(BigInteger)
    previous_monthly_rent: Mapped[int | None] = mapped_column(BigInteger)

    # 시군구명. 오피스텔 전월세만.
    sigungu_name: Mapped[str | None] = mapped_column(String(30))
    # 단지 일련번호. 아파트 전월세만.
    apartment_serial_number: Mapped[str | None] = mapped_column(String(20))

    # 도로명주소 상세. 아파트 전월세에만 있는 7개 필드(roadnm/roadnmsggcd/
    # roadnmcd/roadnmseq/roadnmbcd/roadnmbonbun/roadnmbubun)를 컬럼으로 늘리는
    # 대신 하나로 묶는다. 나머지 유형에서는 전부 NULL이 되기 때문(draft.md 1절).
    road_address_detail: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
