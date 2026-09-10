"""raw 테이블 행을 정제 테이블 컬럼으로 바꾸는 전처리기 인터페이스와 구현체."""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

from sqlalchemy import RowMapping

from ..model import PropertyType
from .utils import split_sgg_cd

ROAD_ADDRESS_FIELDS = (
    "roadnm",
    "roadnmsggcd",
    "roadnmcd",
    "roadnmseq",
    "roadnmbcd",
    "roadnmbonbun",
    "roadnmbubun",
)


class DataPreprocessor(ABC):
    """원본 행 목록을 적재 가능한 row 목록으로 바꾸는 인터페이스."""

    @abstractmethod
    def preprocess(self, rows: Sequence[RowMapping]) -> list[dict[str, Any]]:
        """원본 행을 대상 컬럼명 dict 목록으로 바꿔 반환한다."""


class RawTablePreprocessor(DataPreprocessor):
    """raw 테이블 8종의 공통 변환기. 하위 클래스는 유형과 건물명 필드만 지정한다."""

    property_type: ClassVar[PropertyType]
    building_name_field: ClassVar[str | None] = None
    table_name: ClassVar[str]

    def preprocess(self, rows: Sequence[RowMapping]) -> list[dict[str, Any]]:
        """raw 행을 정제 테이블 컬럼명 dict 목록으로 바꾼다."""
        return [self._convert(row) for row in rows]

    def _convert(self, row: RowMapping) -> dict[str, Any]:
        """행 하나를 변환한다. 실패하면 어느 테이블 어느 행인지 붙여 다시 던진다."""
        try:
            return self._common(row) | self._specific(row)
        except (ValueError, InvalidOperation) as error:
            raise ValueError(
                f"{self.table_name} 행을 가공할 수 없다(id={row.get('id')}): {error}"
            ) from error

    def _common(self, row: RowMapping) -> dict[str, Any]:
        """매매·전월세가 함께 쓰는 컬럼을 채운다(draft.md 2절)."""
        sido_code, sigungu_code = split_sgg_cd(_required(row, "sggCd"))
        building_name = (
            _text(row.get(self.building_name_field))
            if self.building_name_field
            else None
        )
        return {
            "property_type": self.property_type,
            "house_type": _text(row.get("houseType")),
            "sido_code": sido_code,
            "sigungu_code": sigungu_code,
            "umd_name": _required(row, "umdNm"),
            "jibun": _text(row.get("jibun")),
            "building_name": building_name,
            "deal_date": date(
                int(_required(row, "dealYear")),
                int(_required(row, "dealMonth")),
                int(_required(row, "dealDay")),
            ),
            "exclusive_use_area": _decimal(row.get("excluUseAr")),
            "floor": _int(row.get("floor")),
            "build_year": _int(row.get("buildYear")),
            "total_floor_area": _decimal(row.get("totalFloorAr")),
        }

    def _specific(self, row: RowMapping) -> dict[str, Any]:
        """매매/전월세 각각의 고유 컬럼을 채운다."""
        raise NotImplementedError


class SalePreprocessor(RawTablePreprocessor):
    """매매 raw 4종을 `sale_transaction` 행으로 바꾼다."""

    def _specific(self, row: RowMapping) -> dict[str, Any]:
        return {
            "plottage_area": _decimal(row.get("plottageAr")),
            "land_area": _decimal(row.get("landAr")),
            "deal_amount": _amount(_required(row, "dealAmount")),
            "dealing_type": _text(row.get("dealingGbn")),
            "estate_agent_sigungu_name": _text(row.get("estateAgentSggNm")),
            "seller_type": _text(row.get("slerGbn")),
            "buyer_type": _text(row.get("buyerGbn")),
            "cancel_deal_type": _text(row.get("cdealType")),
            "cancel_deal_date": _short_date(row.get("cdealDay")),
            "registration_date": _short_date(row.get("rgstDate")),
            "apartment_dong": _text(row.get("aptDong")),
            "land_leasehold_type": _text(row.get("landLeaseholdGbn")),
            "sigungu_name": _text(row.get("sggNm")),
        }


class RentPreprocessor(RawTablePreprocessor):
    """전월세 raw 4종을 `rent_transaction` 행으로 바꾼다."""

    def _specific(self, row: RowMapping) -> dict[str, Any]:
        return {
            "deposit": _amount(_required(row, "deposit")),
            "monthly_rent": _amount(_required(row, "monthlyRent")),
            "contract_term": _text(row.get("contractTerm")),
            "contract_type": _text(row.get("contractType")),
            "renewal_right_used": _text(row.get("useRRRight")),
            # 종전 보증금은 음수로 들어오는 행이 110건 있다. 원본 값을 그대로 둔다.
            "previous_deposit": _optional_amount(row.get("preDeposit")),
            "previous_monthly_rent": _optional_amount(row.get("preMonthlyRent")),
            "sigungu_name": _text(row.get("sggNm")),
            "apartment_serial_number": _text(row.get("aptSeq")),
            "road_address_detail": _road_address(row),
        }


class ApartSalePreprocessor(SalePreprocessor):
    """`raw_apart_sale`(아파트 매매)을 가공한다."""

    table_name = "raw_apart_sale"
    property_type = PropertyType.APT
    building_name_field = "aptNm"


class OfficetelSalePreprocessor(SalePreprocessor):
    """`raw_officetel_sale`(오피스텔 매매)을 가공한다."""

    table_name = "raw_officetel_sale"
    property_type = PropertyType.OFFICETEL
    building_name_field = "offiNm"


class MultiflexSalePreprocessor(SalePreprocessor):
    """`raw_multiflex_sale`(연립다세대 매매)을 가공한다."""

    table_name = "raw_multiflex_sale"
    property_type = PropertyType.ROW_HOUSE
    building_name_field = "mhouseNm"


class SingleMultiFamilySalePreprocessor(SalePreprocessor):
    """`raw_single_multi_family_sale`(단독·다가구 매매)을 가공한다. 건물명이 없다."""

    table_name = "raw_single_multi_family_sale"
    property_type = PropertyType.SINGLE_MULTI


class ApartRentPreprocessor(RentPreprocessor):
    """`raw_apart_rent`(아파트 전월세)를 가공한다."""

    table_name = "raw_apart_rent"
    property_type = PropertyType.APT
    building_name_field = "aptNm"


class OfficetelRentPreprocessor(RentPreprocessor):
    """`raw_officetel_rent`(오피스텔 전월세)를 가공한다."""

    table_name = "raw_officetel_rent"
    property_type = PropertyType.OFFICETEL
    building_name_field = "offiNm"


class MultiflexRentPreprocessor(RentPreprocessor):
    """`raw_multiflex_rent`(연립다세대 전월세)를 가공한다."""

    table_name = "raw_multiflex_rent"
    property_type = PropertyType.ROW_HOUSE
    building_name_field = "mhouseNm"


class SingleMultiFamilyRentPreprocessor(RentPreprocessor):
    """`raw_single_multi_family_rent`(단독·다가구 전월세)를 가공한다. 건물명이 없다."""

    table_name = "raw_single_multi_family_rent"
    property_type = PropertyType.SINGLE_MULTI


def _text(value: str | None) -> str | None:
    """원본이 쓰는 빈 값 세 가지(None, "", 공백 한 칸)를 모두 None으로 맞춘다."""
    if value is None:
        return None
    return value.strip() or None


def _required(row: RowMapping, field: str) -> str:
    """비어 있으면 안 되는 필드를 읽는다. 비어 있으면 예외를 던진다."""
    value = _text(row[field])
    if value is None:
        raise ValueError(f"{field}가 비어 있다")
    return value


def _int(value: str | None) -> int | None:
    """빈 값은 None으로 두고 int로 바꾼다."""
    text = _text(value)
    return None if text is None else int(text)


def _decimal(value: str | None) -> Decimal | None:
    """빈 값은 None으로 두고 Decimal로 바꾼다."""
    text = _text(value)
    return None if text is None else Decimal(text)


def _amount(value: str) -> int:
    """만원 단위 콤마 문자열("36,900")을 원 단위 정수로 바꾼다."""
    return int(value.replace(",", "")) * 10_000


def _optional_amount(value: str | None) -> int | None:
    """빈 값을 허용하는 금액 필드용 `_amount`."""
    text = _text(value)
    return None if text is None else _amount(text)


def _short_date(value: str | None) -> date | None:
    """cdealDay/rgstDate의 "26.02.27"(YY.MM.DD) 형식을 date로 바꾼다."""
    text = _text(value)
    if text is None:
        return None
    year, month, day = text.split(".")
    return date(2000 + int(year), int(month), int(day))


def _road_address(row: RowMapping) -> dict[str, str] | None:
    """아파트 전월세 전용 도로명 7개 필드를 컬럼 대신 dict 하나로 묶는다(draft.md 1절)."""
    detail = {
        field: value
        for field in ROAD_ADDRESS_FIELDS
        if (value := _text(row.get(field))) is not None
    }
    return detail or None
