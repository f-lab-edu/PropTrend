import hashlib
import json
from datetime import date
from decimal import Decimal
from typing import Any

from app.model import PropertyType

ROAD_ADDRESS_FIELDS = (
    "roadnm",
    "roadnmsggcd",
    "roadnmcd",
    "roadnmseq",
    "roadnmbcd",
    "roadnmbonbun",
    "roadnmbubun",
)


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _to_amount(value: str | None) -> int | None:
    cleaned = _clean(value)
    if cleaned is None:
        return None
    return int(cleaned.replace(",", "")) * 10000


def _to_decimal(value: str | None) -> Decimal | None:
    cleaned = _clean(value)
    return Decimal(cleaned) if cleaned is not None else None


def _to_int(value: str | None) -> int | None:
    cleaned = _clean(value)
    return int(cleaned) if cleaned is not None else None


def _to_deal_date(item: dict[str, Any]) -> date:
    return date(int(item["dealYear"]), int(item["dealMonth"]), int(item["dealDay"]))


def _to_partial_date(value: str | None) -> date | None:
    cleaned = _clean(value)
    if cleaned is None:
        return None
    yy, mm, dd = cleaned.split(".")
    return date(2000 + int(yy), int(mm), int(dd))


def _load_items(content: str) -> list[dict[str, Any]]:
    data = json.loads(content)
    item = data.get("body", {}).get("items", {}).get("item")
    if item is None:
        return []
    if isinstance(item, dict):
        return [item]
    return item


def _road_address_detail(item: dict[str, Any]) -> dict[str, str | None] | None:
    if not any(field in item for field in ROAD_ADDRESS_FIELDS):
        return None
    return {field: _clean(item.get(field)) for field in ROAD_ADDRESS_FIELDS}


def _dedup_hash(row: dict[str, Any]) -> str:
    """행 전체 내용 기반 해시. NULL이 섞인 컬럼 조합으로는 구분할 수 없는
    row(예: 단독·다가구)도 내용이 다르면 반드시 다른 값이 되도록 보장한다."""
    canonical = json.dumps(row, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _common_fields(item: dict[str, Any], property_type: PropertyType) -> dict[str, Any]:
    sgg_cd = item["sggCd"]
    return {
        "property_type": property_type,
        "house_type": _clean(item.get("houseType")),
        "sido_code": sgg_cd[:2],
        "sigungu_code": sgg_cd[2:5],
        "umd_name": item["umdNm"],
        "jibun": _clean(item.get("jibun")),
        "building_name": _clean(
            item.get("aptNm") or item.get("offiNm") or item.get("mhouseNm")
        ),
        "deal_date": _to_deal_date(item),
        "exclusive_use_area": _to_decimal(item.get("excluUseAr")),
        "floor": _to_int(item.get("floor")),
        "build_year": _to_int(item.get("buildYear")),
        "total_floor_area": _to_decimal(item.get("totalFloorAr")),
        "sigungu_name": _clean(item.get("sggNm")),
    }


def transform_sale_item(
    item: dict[str, Any], property_type: PropertyType
) -> dict[str, Any]:
    row = _common_fields(item, property_type)
    row.update(
        {
            "plottage_area": _to_decimal(item.get("plottageAr")),
            "land_area": _to_decimal(item.get("landAr")),
            "deal_amount": _to_amount(item["dealAmount"]),
            "dealing_type": _clean(item.get("dealingGbn")),
            "estate_agent_sigungu_name": _clean(item.get("estateAgentSggNm")),
            "seller_type": _clean(item.get("slerGbn")),
            "buyer_type": _clean(item.get("buyerGbn")),
            "cancel_deal_type": _clean(item.get("cdealType")),
            "cancel_deal_date": _to_partial_date(item.get("cdealDay")),
            "registration_date": _to_partial_date(item.get("rgstDate")),
            "apartment_dong": _clean(item.get("aptDong")),
            "land_leasehold_type": _clean(item.get("landLeaseholdGbn")),
        }
    )
    row["dedup_hash"] = _dedup_hash(row)
    return row


def transform_rent_item(
    item: dict[str, Any], property_type: PropertyType
) -> dict[str, Any]:
    row = _common_fields(item, property_type)
    row.update(
        {
            "deposit": _to_amount(item["deposit"]),
            "monthly_rent": _to_amount(item["monthlyRent"]),
            "contract_term": _clean(item.get("contractTerm")),
            "contract_type": _clean(item.get("contractType")),
            "renewal_right_used": _clean(item.get("useRRRight")),
            "previous_deposit": _to_amount(item.get("preDeposit")),
            "previous_monthly_rent": _to_amount(item.get("preMonthlyRent")),
            "apartment_serial_number": _clean(item.get("aptSeq")),
            "road_address_detail": _road_address_detail(item),
        }
    )
    row["dedup_hash"] = _dedup_hash(row)
    return row
