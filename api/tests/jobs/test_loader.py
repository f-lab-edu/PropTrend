"""적재기의 입력 검증과 스키마 드리프트 경고."""

import logging
from datetime import date
from typing import Any

import pytest

from src.jobs import loader as loader_module
from src.jobs.loader import ComplexLoader, RTMSRawItemLoader, SaleTransactionLoader
from src.model import PropertyType
from tests.jobs.conftest import FakeSession


def sale_row(**overrides: Any) -> dict[str, Any]:
    """정제 테이블 컬럼을 모두 채운 적재 row. 값 자체는 검증 대상이 아니다."""
    columns = SaleTransactionLoader(FakeSession()).columns
    return dict.fromkeys(columns) | overrides


def test_rejects_unknown_api_id() -> None:
    # 오타 난 슬러그는 갱신 단위 삭제가 영영 못 찾는 행을 만든다.
    with pytest.raises(ValueError, match="모르는 api_id"):
        RTMSRawItemLoader(FakeSession(), "apart_sail")


async def test_empty_items_skip_execute() -> None:
    session = FakeSession()

    assert await RTMSRawItemLoader(session, "apart_sale").load("11110", "202602", []) == 0
    assert session.statements == []


async def test_raw_rows_carry_request_params_only() -> None:
    session = FakeSession()

    loaded = await RTMSRawItemLoader(session, "apart_sale").load("11110", "202602", [{"aptNm": "청운현대"}])

    assert loaded == 1
    ((_, rows),) = session.statements
    # 갱신 단위 키는 응답이 아니라 요청 파라미터에서 온다.
    assert rows[0]["api_id"] == "apart_sale"
    assert (rows[0]["lawd_cd"], rows[0]["deal_ymd"]) == ("11110", "202602")
    # collected_at은 server_default에 맡기려고 키 자체를 넣지 않는다.
    assert "collected_at" not in rows[0]


async def test_rows_are_inserted_in_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(loader_module, "CHUNK_SIZE", 2)
    session = FakeSession()

    loaded = await RTMSRawItemLoader(session, "apart_sale").load("11110", "202602", [{"aptNm": "x"}] * 5)

    assert loaded == 5
    assert len(session.statements) == 3  # 2 + 2 + 1


async def test_rejects_rows_whose_keys_differ_from_columns() -> None:
    row = sale_row()
    row["없는컬럼"] = None
    del row["deal_amount"]

    with pytest.raises(ValueError, match="sale_transactions 컬럼과 다르다") as error:
        await SaleTransactionLoader(FakeSession()).load([row])

    assert "없는컬럼" in str(error.value)
    assert "deal_amount" in str(error.value)


async def test_warns_on_schema_drift(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="src.jobs.loader"):
        # 기준선에 없는 newField가 왔고, 기준선에 있던 나머지 필드는 오지 않았다.
        await RTMSRawItemLoader(FakeSession(), "apart_sale").load("11110", "202602", [{"newField": "1"}])

    messages = [record.getMessage() for record in caplog.records]
    assert any("기준선에 없는 필드가 왔다" in message and "newField" in message for message in messages)
    assert any("기준선에 있던 필드가 응답에 없다" in message and "aptNm" in message for message in messages)


def complex_source_row(**overrides: Any) -> dict[str, Any]:
    """단지 적재기가 읽는 컬럼만 채운 정제 row."""
    return {
        "sido_code": "11",
        "sigungu_code": "110",
        "umd_name": "청운동",
        "jibun": "1",
        "building_name": "청운현대",
        "build_year": 2000,
        "apartment_serial_number": "11110-1",
        "deal_date": date(2026, 2, 1),
    } | overrides


async def test_complex_loader_skips_empty_rows() -> None:
    session = FakeSession()

    assert await ComplexLoader(session).load(PropertyType.APT, []) == 0
    assert session.statements == []


async def test_complex_loader_keeps_latest_attributes_per_apartment() -> None:
    session = FakeSession()
    rows = [
        complex_source_row(building_name="옛이름", deal_date=date(2026, 2, 1)),
        complex_source_row(building_name="새이름", deal_date=date(2026, 2, 20)),
        complex_source_row(apartment_serial_number="11110-2", building_name="다른단지"),
        # 상세 자료로 재수집되기 전의 매매는 단지를 특정할 수 없다.
        complex_source_row(apartment_serial_number=None, building_name="일련번호없음"),
    ]

    loaded = await ComplexLoader(session).load(PropertyType.APT, rows)

    assert loaded == 2
    ((_, params),) = session.statements
    assert [(row["apartment_serial_number"], row["building_name"]) for row in params] == [
        ("11110-1", "새이름"),
        ("11110-2", "다른단지"),
    ]
    assert all(row["property_type"] == PropertyType.APT for row in params)
    assert "deal_date" not in params[0]


async def test_complex_loader_groups_officetel_with_null_keys() -> None:
    session = FakeSession()
    rows = [
        complex_source_row(apartment_serial_number=None, jibun=None),
        complex_source_row(apartment_serial_number=None, jibun=None, deal_date=date(2026, 2, 20)),
        complex_source_row(apartment_serial_number=None, jibun="2"),
    ]

    loaded = await ComplexLoader(session).load(PropertyType.OFFICETEL, rows)

    # 지번이 빈 두 거래는 같은 단지로 묶이고, NULL이 값보다 앞에 정렬된다.
    assert loaded == 2
    ((_, params),) = session.statements
    assert [row["jibun"] for row in params] == [None, "2"]
