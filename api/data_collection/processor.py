import logging
from abc import ABC, abstractmethod
from datetime import date
from decimal import Decimal
from typing import Any, ClassVar

from app.model.prop_transaction import PropertyType

logger = logging.getLogger(__name__)

# 면적 컬럼(전용면적/연면적/대지면적/대지권면적)은 모두 NUMERIC(10, 4)이라 정수부가
# 6자리를 넘을 수 없다. prop_transaction.py의 컬럼 정의와 함께 유지해야 한다.
MAX_AREA = Decimal(10) ** 6


def _normalize_empty(value: str) -> str | None:
    # 원본 응답은 값이 없는 항목을 빈 문자열이 아니라 공백 한 칸으로 채워 보내기도 한다.
    return value.strip() or None


def _parse_won_amount(value: str) -> int:
    return int(value.strip().replace(",", "")) * 10000


def _parse_int(value: str) -> int | None:
    stripped = value.strip()
    return int(stripped) if stripped else None


def _parse_decimal(value: str) -> Decimal | None:
    stripped = value.strip()
    if not stripped:
        return None
    parsed = Decimal(stripped)
    if abs(parsed) >= MAX_AREA:
        # 원본에 대지권면적 448,128,851.9㎡(=448㎢) 같은 명백한 오류값이 섞여 있다.
        # 그대로 넣으면 컬럼 범위를 넘어 해당 갱신 단위 전체가 롤백되므로, 거래 기록을
        # 통째로 잃는 대신 면적 값만 버린다.
        logger.warning("면적이 저장 범위를 벗어나 NULL로 대체합니다: %s", stripped)
        return None
    return parsed


def _parse_deal_date(item: dict[str, str]) -> date:
    return date(int(item["dealYear"]), int(item["dealMonth"]), int(item["dealDay"]))


def _parse_optional_date(value: str) -> date | None:
    stripped = value.strip()
    if not stripped:
        return None
    # 등기일자/해제사유발생일은 명세상 8자리(YYYYMMDD)지만, 실제 응답은 "25.11.17"처럼
    # 2자리 연도를 점으로 구분해 내려주므로 두 형식을 모두 받는다.
    if "." in stripped:
        year, month, day = stripped.split(".")
        return date(2000 + int(year), int(month), int(day))
    return date(int(stripped[:4]), int(stripped[4:6]), int(stripped[6:8]))


def _split_region_code(sgg_cd: str) -> tuple[str, str]:
    stripped = sgg_cd.strip()
    return stripped[:2], stripped[2:5]


class DataProcessor(ABC):
    @abstractmethod
    def process(self, raw_items: list[dict[str, str]]) -> list[Any]:
        """수집된 원본 데이터 중 필요한 항목만 추출해 정해진 형식으로 변환한다."""


class LegalDongCodeProcessor(DataProcessor):
    """법정동코드 원본 응답을 시군구 단위 {code, name} 목록으로 가공한다."""

    def process(self, raw_items: list[dict[str, str]]) -> list[dict[str, str]]:
        result: list[dict[str, str]] = []
        for row in raw_items:
            if row.get("umd_cd") != "000" or row.get("ri_cd") != "00":
                continue
            result.append(
                {
                    "code": row["sido_cd"] + row["sgg_cd"],
                    "name": row["locatadd_nm"],
                }
            )
        return result


def _common_transaction_fields(
    item: dict[str, str], property_type: PropertyType, building_name_field: str | None
) -> dict[str, Any]:
    sido_code, sigungu_code = _split_region_code(item["sggCd"])
    return {
        "property_type": property_type,
        "house_type": _normalize_empty(item.get("houseType", "")),
        "sido_code": sido_code,
        "sigungu_code": sigungu_code,
        "umd_name": item["umdNm"].strip(),
        "jibun": _normalize_empty(item.get("jibun", "")),
        "building_name": (
            _normalize_empty(item.get(building_name_field, ""))
            if building_name_field
            else None
        ),
        "deal_date": _parse_deal_date(item),
        "exclusive_use_area": _parse_decimal(item.get("excluUseAr", "")),
        "floor": _parse_int(item.get("floor", "")),
        "build_year": _parse_int(item.get("buildYear", "")),
    }


def _sale_dealing_fields(item: dict[str, str]) -> dict[str, Any]:
    return {
        "dealing_type": _normalize_empty(item.get("dealingGbn", "")),
        "estate_agent_sigungu_name": _normalize_empty(item.get("estateAgentSggNm", "")),
        "seller_type": _normalize_empty(item.get("slerGbn", "")),
        "buyer_type": _normalize_empty(item.get("buyerGbn", "")),
        "cancel_deal_type": _normalize_empty(item.get("cdealType", "")),
        "cancel_deal_date": _parse_optional_date(item.get("cdealDay", "")),
    }


def _rent_contract_fields(item: dict[str, str]) -> dict[str, Any]:
    pre_deposit = item.get("preDeposit", "").strip()
    pre_monthly_rent = item.get("preMonthlyRent", "").strip()
    return {
        "contract_term": _normalize_empty(item.get("contractTerm", "")),
        "contract_type": _normalize_empty(item.get("contractType", "")),
        "renewal_right_used": _normalize_empty(item.get("useRRRight", "")),
        "previous_deposit": _parse_won_amount(pre_deposit) if pre_deposit else None,
        "previous_monthly_rent": (
            _parse_won_amount(pre_monthly_rent) if pre_monthly_rent else None
        ),
    }


class _RTMSSaleProcessor(DataProcessor):
    """국토교통부 실거래가 매매 4종 API의 공통 가공 로직."""

    PROPERTY_TYPE: ClassVar[PropertyType]
    BUILDING_NAME_FIELD: ClassVar[str | None] = None

    def process(self, raw_items: list[dict[str, str]]) -> list[dict[str, Any]]:
        return [self._to_row(item) for item in raw_items]

    def _to_row(self, item: dict[str, str]) -> dict[str, Any]:
        common = _common_transaction_fields(
            item, self.PROPERTY_TYPE, self.BUILDING_NAME_FIELD
        )
        return {
            **common,
            "deal_amount": _parse_won_amount(item["dealAmount"]),
            **self._extra_fields(item),
        }

    def _extra_fields(self, item: dict[str, str]) -> dict[str, Any]:
        return {}


class _RTMSRentProcessor(DataProcessor):
    """국토교통부 실거래가 전월세 4종 API의 공통 가공 로직."""

    PROPERTY_TYPE: ClassVar[PropertyType]
    BUILDING_NAME_FIELD: ClassVar[str | None] = None

    def process(self, raw_items: list[dict[str, str]]) -> list[dict[str, Any]]:
        return [self._to_row(item) for item in raw_items]

    def _to_row(self, item: dict[str, str]) -> dict[str, Any]:
        common = _common_transaction_fields(
            item, self.PROPERTY_TYPE, self.BUILDING_NAME_FIELD
        )
        return {
            **common,
            "deposit": _parse_won_amount(item["deposit"]),
            "monthly_rent": _parse_won_amount(item["monthlyRent"]),
            **self._extra_fields(item),
        }

    def _extra_fields(self, item: dict[str, str]) -> dict[str, Any]:
        return {}


class RTMSAptTradeProcessor(_RTMSSaleProcessor):
    """국토교통부 아파트 매매 실거래 데이터 가공 모듈."""

    PROPERTY_TYPE = PropertyType.APT
    BUILDING_NAME_FIELD = "aptNm"

    def _extra_fields(self, item: dict[str, str]) -> dict[str, Any]:
        return {
            **_sale_dealing_fields(item),
            "registration_date": _parse_optional_date(item.get("rgstDate", "")),
            "apartment_dong": _normalize_empty(item.get("aptDong", "")),
            "land_leasehold_type": _normalize_empty(item.get("landLeaseholdGbn", "")),
        }


class RTMSOffiTradeProcessor(_RTMSSaleProcessor):
    """국토교통부 오피스텔 매매 실거래 데이터 가공 모듈."""

    PROPERTY_TYPE = PropertyType.OFFICETEL
    BUILDING_NAME_FIELD = "offiNm"

    def _extra_fields(self, item: dict[str, str]) -> dict[str, Any]:
        return {
            **_sale_dealing_fields(item),
            "sigungu_name": _normalize_empty(item.get("sggNm", "")),
        }


class RTMSRHTradeProcessor(_RTMSSaleProcessor):
    """국토교통부 연립다세대 매매 실거래 데이터 가공 모듈."""

    PROPERTY_TYPE = PropertyType.ROW_HOUSE
    BUILDING_NAME_FIELD = "mhouseNm"

    def _extra_fields(self, item: dict[str, str]) -> dict[str, Any]:
        return {
            **_sale_dealing_fields(item),
            "registration_date": _parse_optional_date(item.get("rgstDate", "")),
            "land_area": _parse_decimal(item.get("landAr", "")),
        }


class RTMSSHTradeProcessor(_RTMSSaleProcessor):
    """국토교통부 단독다가구 매매 실거래 데이터 가공 모듈."""

    PROPERTY_TYPE = PropertyType.SINGLE_MULTI
    BUILDING_NAME_FIELD = None

    def _extra_fields(self, item: dict[str, str]) -> dict[str, Any]:
        return {
            **_sale_dealing_fields(item),
            "total_floor_area": _parse_decimal(item.get("totalFloorAr", "")),
            "plottage_area": _parse_decimal(item.get("plottageAr", "")),
        }


class RTMSAptRentProcessor(_RTMSRentProcessor):
    """국토교통부 아파트 전월세 실거래 데이터 가공 모듈."""

    PROPERTY_TYPE = PropertyType.APT
    BUILDING_NAME_FIELD = "aptNm"

    _ROAD_ADDRESS_FIELDS = (
        "roadnm",
        "roadnmsggcd",
        "roadnmcd",
        "roadnmseq",
        "roadnmbcd",
        "roadnmbonbun",
        "roadnmbubun",
    )

    def _extra_fields(self, item: dict[str, str]) -> dict[str, Any]:
        road_address_detail = {
            field: value
            for field in self._ROAD_ADDRESS_FIELDS
            if (value := _normalize_empty(item.get(field, ""))) is not None
        }
        return {
            **_rent_contract_fields(item),
            "apartment_serial_number": _normalize_empty(item.get("aptSeq", "")),
            "road_address_detail": road_address_detail or None,
        }


class RTMSOffiRentProcessor(_RTMSRentProcessor):
    """국토교통부 오피스텔 전월세 실거래 데이터 가공 모듈."""

    PROPERTY_TYPE = PropertyType.OFFICETEL
    BUILDING_NAME_FIELD = "offiNm"

    def _extra_fields(self, item: dict[str, str]) -> dict[str, Any]:
        return {
            **_rent_contract_fields(item),
            "sigungu_name": _normalize_empty(item.get("sggNm", "")),
        }


class RTMSRHRentProcessor(_RTMSRentProcessor):
    """국토교통부 연립다세대 전월세 실거래 데이터 가공 모듈."""

    PROPERTY_TYPE = PropertyType.ROW_HOUSE
    BUILDING_NAME_FIELD = "mhouseNm"

    def _extra_fields(self, item: dict[str, str]) -> dict[str, Any]:
        return _rent_contract_fields(item)


class RTMSSHRentProcessor(_RTMSRentProcessor):
    """국토교통부 단독다가구 전월세 실거래 데이터 가공 모듈."""

    PROPERTY_TYPE = PropertyType.SINGLE_MULTI
    BUILDING_NAME_FIELD = None

    def _extra_fields(self, item: dict[str, str]) -> dict[str, Any]:
        return {
            **_rent_contract_fields(item),
            "total_floor_area": _parse_decimal(item.get("totalFloorAr", "")),
        }
