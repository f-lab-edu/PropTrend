"""bronze payload를 정제 테이블·지역 목록의 행으로 바꾼다."""

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from rtms.open_api import RTMS_APIS

# 아파트 전월세 전용 도로명 7개 필드. 나머지 유형에서는 전부 NULL이라 컬럼 대신 JSONB 하나로 묶는다.
ROAD_ADDRESS_FIELDS = ("roadnm", "roadnmsggcd", "roadnmcd", "roadnmseq", "roadnmbcd", "roadnmbonbun", "roadnmbubun")


def _text(value: str | None) -> str | None:
    """원본이 쓰는 빈 값 세 가지(None, "", 공백 한 칸)를 모두 None으로 맞춘다."""
    if value is None:
        return None
    return value.strip() or None


def _required(row: dict[str, Any], field: str) -> str:
    """비어 있으면 안 되는 필드를 읽는다. payload에 키 자체가 없어도 빈 값과 같게 다룬다."""
    value = _text(row.get(field))
    if value is None:
        raise ValueError(f"{field}가 비어 있다")
    return value


def _sgg_cd(value: str) -> tuple[str, str]:
    """5자리 시군구코드를 시도 2자리 + 시군구 3자리로 나눈다."""
    # isdigit()만으로는 부족하다. 아라비아-인도 숫자 같은 비ASCII 숫자도 True다.
    if len(value) != 5 or not (value.isascii() and value.isdigit()):  # noqa: PLR2004
        raise ValueError(f"sgg_cd는 숫자 5자리여야 한다: {value!r}")
    return value[:2], value[2:]


def _int(value: str | None) -> int | None:
    text = _text(value)
    return None if text is None else int(text)


def _decimal(value: str | None) -> Decimal | None:
    text = _text(value)
    return None if text is None else Decimal(text)


def _amount(value: str | None) -> int | None:
    """만원 단위 콤마 문자열("36,900")을 원 단위 정수로 바꾼다."""
    text = _text(value)
    return None if text is None else int(text.replace(",", "")) * 10_000


def _short_date(value: str | None) -> date | None:
    """cdealDay/rgstDate의 "26.02.27"(YY.MM.DD) 형식을 date로 바꾼다."""
    text = _text(value)
    if text is None:
        return None
    year, month, day = text.split(".")
    return date(2000 + int(year), int(month), int(day))


def preprocess_rtms(api_id: str, raw_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    """bronze payload 한 건을 정제 테이블 행으로 바꾼다. 실패하면 어느 bronze 행인지 붙여 던진다."""
    api = RTMS_APIS[api_id]
    try:
        sido_code, sigungu_code = _sgg_cd(_required(payload, "sggCd"))
        row: dict[str, Any] = {
            "property_type": api.property_type,
            "house_type": _text(payload.get("houseType")),
            "sido_code": sido_code,
            "sigungu_code": sigungu_code,
            "umd_name": _text(payload.get("umdNm")),
            "jibun": _text(payload.get("jibun")),
            "building_name": _text(payload.get(api.building_name_field)) if api.building_name_field else None,
            "deal_date": date(
                int(_required(payload, "dealYear")),
                int(_required(payload, "dealMonth")),
                int(_required(payload, "dealDay")),
            ),
            "exclusive_use_area": _decimal(payload.get("excluUseAr")),
            "floor": _int(payload.get("floor")),
            "build_year": _int(payload.get("buildYear")),
            "total_floor_area": _decimal(payload.get("totalFloorAr")),
            "apartment_serial_number": _text(payload.get("aptSeq")),
            "sigungu_name": _text(payload.get("sggNm")),
        }
        if api.is_sale:
            row |= {
                "plottage_area": _decimal(payload.get("plottageAr")),
                "land_area": _decimal(payload.get("landAr")),
                "deal_amount": _amount(_required(payload, "dealAmount")),
                "dealing_type": _text(payload.get("dealingGbn")),
                "estate_agent_sigungu_name": _text(payload.get("estateAgentSggNm")),
                "seller_type": _text(payload.get("slerGbn")),
                "buyer_type": _text(payload.get("buyerGbn")),
                "cancel_deal_type": _text(payload.get("cdealType")),
                "cancel_deal_date": _short_date(payload.get("cdealDay")),
                "registration_date": _short_date(payload.get("rgstDate")),
                "apartment_dong": _text(payload.get("aptDong")),
                "land_leasehold_type": _text(payload.get("landLeaseholdGbn")),
            }
        else:
            road_address = {
                field: value for field in ROAD_ADDRESS_FIELDS if (value := _text(payload.get(field))) is not None
            }
            row |= {
                "deposit": _amount(_required(payload, "deposit")),
                "monthly_rent": _amount(_required(payload, "monthlyRent")),
                "contract_term": _text(payload.get("contractTerm")),
                "contract_type": _text(payload.get("contractType")),
                "renewal_right_used": _text(payload.get("useRRRight")),
                # 종전 보증금은 음수로 들어오는 행이 있다. 원본 값을 그대로 둔다.
                "previous_deposit": _amount(payload.get("preDeposit")),
                "previous_monthly_rent": _amount(payload.get("preMonthlyRent")),
                "road_address_detail": road_address or None,
            }
    except (ValueError, InvalidOperation) as error:
        raise ValueError(f"{api_id} 가공 실패(rtms_raw_items.id={raw_id}): {error}") from error
    return row


def preprocess_regions(items: list[dict[str, Any]]) -> list[dict[str, str]]:
    """법정동코드 시군구 행을 regions 행으로 바꾼다. 한 행이라도 비정상이면 예외를 던진다."""
    names = [_required(item, "locatadd_nm") for item in items]
    # 일반구를 둔 시("경기도 수원시")는 실거래가 구 코드로만 들어와 고르면 항상 빈 결과다.
    # 코드 앞자리로는 영동군(43740)·증평군(43745)처럼 별개 지역이 겹쳐 이름으로 판별한다.
    ancestors = {" ".join(tokens[:i]) for tokens in (name.split() for name in names) for i in range(1, len(tokens))}

    rows: list[dict[str, str]] = []
    sido_names: dict[str, str] = {}
    for item, name in zip(items, names, strict=True):
        if name in ancestors:
            continue
        # 실거래 수집의 LAWD_CD와 같은 출처(region_cd 앞 5자리)를 쓴다.
        sido_code, sigungu_code = _sgg_cd(_required(item, "region_cd")[:5])
        sido_name, _, sigungu_name = name.partition(" ")
        # 시도명이 행마다 반복되므로 같은 시도가 다른 이름으로 갈라지면 드롭다운에 시도가 둘로 보인다.
        if sido_names.setdefault(sido_code, sido_name) != sido_name:
            raise ValueError(f"시도 {sido_code}의 이름이 하나가 아니다: {sido_names[sido_code]!r}, {sido_name!r}")
        # 세종은 시군구 단계 없이 "세종특별자치시"만 온다.
        rows.append(
            {
                "sido_code": sido_code,
                "sigungu_code": sigungu_code,
                "sido_name": sido_name,
                "sigungu_name": sigungu_name or sido_name,
            }
        )
    return rows
