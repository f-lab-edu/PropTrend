"""bronze payload를 정제 테이블 컬럼으로 바꾸는 변환 규칙."""

from datetime import date
from decimal import Decimal
from typing import Any

import pytest
from conftest import FakeSession, bronze_rows, rent_payload, sale_payload

from src.jobs.loader import RentTransactionLoader, SaleTransactionLoader
from src.jobs.preprocessor import RentPreprocessor, SalePreprocessor
from src.model import PropertyType


def sale(**overrides: Any) -> dict[str, Any]:
    """아파트 매매 전처리기로 payload 하나를 변환한다."""
    processor = SalePreprocessor(PropertyType.APT, "apart_sale", "aptNm")
    return processor.preprocess(bronze_rows(sale_payload(**overrides)))[0]


def rent(**overrides: Any) -> dict[str, Any]:
    """아파트 전월세 전처리기로 payload 하나를 변환한다."""
    processor = RentPreprocessor(PropertyType.APT, "apart_rent", "aptNm")
    return processor.preprocess(bronze_rows(rent_payload(**overrides)))[0]


def test_common_columns_are_normalized() -> None:
    row = sale(aptNm=" 청운현대 ", excluUseAr="84.9700", floor="3", buildYear="1998")

    assert row["property_type"] is PropertyType.APT
    assert (row["sido_code"], row["sigungu_code"]) == ("11", "110")  # sggCd 5자리 분리
    assert row["deal_date"] == date(2026, 2, 27)  # dealYear/Month/Day 통합
    assert row["building_name"] == "청운현대"
    assert row["exclusive_use_area"] == Decimal("84.9700")
    assert (row["floor"], row["build_year"]) == (3, 1998)


@pytest.mark.parametrize("empty", [None, "", " "])
def test_blank_values_become_none(empty: str | None) -> None:
    # 원본이 쓰는 빈 값 세 가지가 모두 NULL로 맞춰져야 한다.
    assert sale(jibun=empty)["jibun"] is None


def test_building_name_is_none_without_field() -> None:
    # 단독·다가구는 건물명 필드 자체가 없어 주입되는 필드 이름도 None이다.
    processor = SalePreprocessor(PropertyType.SINGLE_MULTI, "single_multi_family_sale", None)
    row = processor.preprocess(bronze_rows(sale_payload(aptNm="무시된다")))[0]

    assert row["building_name"] is None


def test_sale_amount_becomes_won() -> None:
    # 만원 단위 콤마 문자열을 원 단위 정수로 바꾼다.
    assert sale(dealAmount="36,900")["deal_amount"] == 369_000_000


@pytest.mark.parametrize(("value", "expected"), [("26.02.27", date(2026, 2, 27)), (" ", None)])
def test_sale_short_date_is_parsed(value: str, expected: date | None) -> None:
    assert sale(cdealDay=value)["cancel_deal_date"] == expected


def test_rent_amounts_become_won() -> None:
    row = rent(deposit="30,000", monthlyRent="150")

    assert (row["deposit"], row["monthly_rent"]) == (300_000_000, 1_500_000)


def test_rent_keeps_negative_previous_deposit() -> None:
    # 원본에 음수로 들어오는 행이 있다. 걸러내지 않고 값을 그대로 둔다.
    assert rent(preDeposit="-1,000")["previous_deposit"] == -10_000_000


def test_rent_road_address_is_collapsed_into_one_dict() -> None:
    row = rent(roadnm="자하문로", roadnmbonbun="00042", roadnmbubun=" ")

    # 빈 값인 필드는 아예 키가 생기지 않는다.
    assert row["road_address_detail"] == {"roadnm": "자하문로", "roadnmbonbun": "00042"}


def test_rent_road_address_is_none_when_all_blank() -> None:
    assert rent()["road_address_detail"] is None


def test_missing_required_field_points_at_bronze_row() -> None:
    processor = SalePreprocessor(PropertyType.APT, "apart_sale", "aptNm")
    # 키 자체가 없는 경우다. KeyError로 새면 _convert가 못 잡아 단위 전체가 터진다.
    rows = bronze_rows(sale_payload(dealYear=None), start_id=42)

    with pytest.raises(ValueError, match=r"apart_sale 가공 실패\(rtms_raw_items.id=42\)") as error:
        processor.preprocess(rows)

    assert "dealYear" in str(error.value)


@pytest.mark.parametrize("empty", [None, " "])
def test_blank_umd_name_becomes_none(empty: str | None) -> None:
    # 단독·다가구 전월세 원본에 읍면동명이 빈 행이 있다. 단위를 실패시키지 않고 NULL로 둔다.
    assert rent(umdNm=empty)["umd_name"] is None


def test_unparsable_amount_points_at_bronze_row() -> None:
    processor = SalePreprocessor(PropertyType.APT, "apart_sale", "aptNm")
    rows = bronze_rows(sale_payload(excluUseAr="여든네평"), start_id=7)

    # Decimal은 ValueError가 아닌 InvalidOperation을 던진다. 그쪽도 잡혀야 한다.
    with pytest.raises(ValueError, match=r"rtms_raw_items.id=7"):
        processor.preprocess(rows)


@pytest.mark.parametrize(
    ("row", "loader"),
    [(sale(), SaleTransactionLoader), (rent(), RentTransactionLoader)],
)
def test_output_keys_match_table_columns(row: dict[str, Any], loader: type) -> None:
    # 전처리기와 모델이 어긋나면 적재기가 런타임에야 막는다. 여기서 먼저 잡는다.
    assert row.keys() == loader(FakeSession()).columns
