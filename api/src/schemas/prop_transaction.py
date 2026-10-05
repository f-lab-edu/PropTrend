from datetime import date

from pydantic import Field, computed_field

from ..model.prop_transaction import PropertyType
from . import PropTrendCoreModel


class PropTransactionQuery(PropTrendCoreModel):
    """실거래 목록 조회 조건."""

    # 넓은 범위를 id 순으로 자르면 쿼리가 수 초~수십 초 걸려서, 갱신용 인덱스와 맞는 네 조건을 모두 받는다.
    property_type: PropertyType
    sido_code: str = Field(pattern=r"^\d{2}$")
    sigungu_code: str = Field(pattern=r"^\d{3}$")
    deal_date: date
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
    # 거래가 속한 단지. 서비스가 단지 키로 찾아 채운다. 단지가 없는 거래는 null.
    complex_id: int | None = None
    deal_date: date
    deal_amount: int
    dealing_type: str | None

    # 유형별
    house_type: str | None = None  # 단독다가구, 연립다세대
    building_name: str | None = None  # 아파트, 연립다세대, 오피스텔
    apartment_dong: str | None = None  # 아파트
    floor: int | None = None  # 아파트, 연립다세대, 오피스텔
    build_year: int | None = None  # 모든 유형 (일부 행은 비어 있음)
    exclusive_use_area: float | None = None  # 아파트, 연립다세대, 오피스텔
    total_floor_area: float | None = None  # 단독다가구
    plottage_area: float | None = None  # 단독다가구
    land_area: float | None = None  # 연립다세대


class RentPropTransactionResponse(AddressResponse):
    """전월세 실거래 응답."""

    # 모든 유형 공통
    id: int
    property_type: PropertyType
    # 거래가 속한 단지. 서비스가 단지 키로 찾아 채운다. 단지가 없는 거래는 null.
    complex_id: int | None = None
    deal_date: date
    deposit: int
    monthly_rent: int
    contract_term: str | None
    contract_type: str | None

    # 유형별
    house_type: str | None = None  # 단독다가구, 연립다세대
    building_name: str | None = None  # 아파트, 연립다세대, 오피스텔
    floor: int | None = None  # 아파트, 연립다세대, 오피스텔
    build_year: int | None = None  # 모든 유형 (일부 행은 비어 있음)
    exclusive_use_area: float | None = None  # 아파트, 연립다세대, 오피스텔
    total_floor_area: float | None = None  # 단독다가구


class SalePriceTrendPoint(PropTrendCoreModel):
    """매매 실거래가 추이의 거래 한 건."""

    id: int
    deal_date: date
    deal_amount: int
    floor: int | None


class RentPriceTrendPoint(PropTrendCoreModel):
    """전월세 실거래가 추이의 거래 한 건."""

    id: int
    deal_date: date
    deposit: int
    monthly_rent: int
    floor: int | None


class SalePropTransactionDetailResponse(PropTrendCoreModel):
    """매매 실거래 상세 응답."""

    base_transaction: SalePropTransactionResponse
    # 같은 그룹으로 묶인 거래들의 실거래가 추이. 그룹을 정할 수 없는 단독다가구는 null.
    trend: list[SalePriceTrendPoint] | None


class RentPropTransactionDetailResponse(PropTrendCoreModel):
    """전월세 실거래 상세 응답."""

    base_transaction: RentPropTransactionResponse
    # 같은 그룹으로 묶인 거래들의 실거래가 추이. 보증금끼리 비교되도록 전세와 월세를 나눈다.
    # 그룹을 정할 수 없는 단독다가구는 둘 다 null.
    jeonse_trend: list[RentPriceTrendPoint] | None
    monthly_rent_trend: list[RentPriceTrendPoint] | None
