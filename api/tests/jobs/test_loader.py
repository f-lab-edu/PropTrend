"""적재기의 입력 검증과 스키마 드리프트 경고."""

import logging
from typing import Any

import pytest

from src.jobs import loader as loader_module
from src.jobs.loader import RTMSRawItemLoader, SaleTransactionLoader
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
