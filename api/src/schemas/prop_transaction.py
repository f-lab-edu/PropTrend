from datetime import date

from pydantic import Field, computed_field

from ..model.prop_transaction import PropertyType
from . import PropTrendCoreModel


class PropTransactionQuery(PropTrendCoreModel):
    """실거래 목록 조회 조건."""

    property_type: PropertyType
    sido_code: str | None = Field(default=None, pattern=r"^\d{2}$")
    sigungu_code: str | None = Field(default=None, pattern=r"^\d{3}$")
    deal_date: date | None = None
    # 한 요청이 테이블 전체를 읽지 않도록 한 페이지의 최대 행 수를 서버에서 제한한다.
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)


class AddressResponse(PropTrendCoreModel):
    """주소 구성요소를 받아 address 하나로 응답한다."""

    # 법정동코드에서 찾은 "서울특별시 강남구". 거래 행에 없어 서비스가 검증 후 채운다.
    region_name: str | None = Field(default=None, exclude=True)
    umd_name: str | None = Field(exclude=True)
    jibun: str | None = Field(exclude=True)

    @computed_field
    @property
    def address(self) -> str:
        """지역명·읍면동·지번 중 값이 있는 것만 이은 주소."""
        return " ".join(part for part in (self.region_name, self.umd_name, self.jibun) if part)


class SalePropTransactionResponse(AddressResponse):
    """매매 실거래 응답."""

    # 모든 유형 공통
    id: int
    property_type: PropertyType
    deal_date: date
    deal_amount: int
    dealing_type: str | None

    # 유형별
    house_type: str | None = None  # 단독다가구, 연립다세대
    building_name: str | None = None  # 아파트, 연립다세대, 오피스텔
    apartment_dong: str | None = None  # 아파트
    floor: int | None = None  # 아파트, 연립다세대, 오피스텔
    build_year: int | None = None  # 아파트, 연립다세대
    exclusive_use_area: float | None = None  # 아파트, 연립다세대, 오피스텔
    total_floor_area: float | None = None  # 단독다가구
    plottage_area: float | None = None  # 단독다가구
    land_area: float | None = None  # 연립다세대


class RentPropTransactionResponse(AddressResponse):
    """전월세 실거래 응답."""

    # 모든 유형 공통
    id: int
    property_type: PropertyType
    deal_date: date
    deposit: int
    monthly_rent: int
    contract_term: str | None
    contract_type: str | None

    # 유형별
    house_type: str | None = None  # 단독다가구, 연립다세대
    building_name: str | None = None  # 아파트, 연립다세대, 오피스텔
    floor: int | None = None  # 아파트, 오피스텔
    build_year: int | None = None  # 아파트, 연립다세대
    exclusive_use_area: float | None = None  # 아파트, 연립다세대, 오피스텔
    total_floor_area: float | None = None  # 단독다가구
