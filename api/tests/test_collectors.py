import xml.etree.ElementTree as ET
from datetime import date
from typing import Any

import pytest
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.model.prop_transaction import LegalStandardCode
from data_collection.collectors import (
    COLLECTION_MONTHS,
    RegionCodesNotFoundError,
    _load_region_codes,
    _parse_item_element,
    _parse_response,
    _recent_months,
    _stan_regin_cd_result_code,
)


async def _insert_codes(
    session_factory: async_sessionmaker[AsyncSession], rows: list[dict[str, Any]]
) -> None:
    async with session_factory() as session:
        await session.execute(insert(LegalStandardCode.__table__), rows)
        await session.commit()


async def test_load_region_codes_returns_codes_from_db(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _insert_codes(
        session_factory,
        [
            {"code": "41111", "name": "경기도 수원시 장안구"},
            {"code": "11110", "name": "서울특별시 종로구"},
        ],
    )

    assert await _load_region_codes(session_factory) == ["11110", "41111"]


async def test_load_region_codes_rejects_malformed_code(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _insert_codes(session_factory, [{"code": "1111", "name": "잘못된 코드"}])

    with pytest.raises(ValueError, match="올바르지 않은 법정동 코드"):
        await _load_region_codes(session_factory)


async def test_load_region_codes_raises_when_master_is_empty(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    with pytest.raises(RegionCodesNotFoundError):
        await _load_region_codes(session_factory)


def test_recent_months_returns_ascending_range_ending_at_current_month() -> None:
    months = _recent_months(COLLECTION_MONTHS, today=date(2026, 9, 4))

    assert len(months) == COLLECTION_MONTHS
    assert months == sorted(months)
    assert months[0] == "202410"
    assert months[-1] == "202609"


def test_recent_months_handles_year_rollover() -> None:
    months = _recent_months(COLLECTION_MONTHS, today=date(2025, 1, 15))

    assert months[0] == "202302"
    assert months[-1] == "202501"


def test_recent_months_honors_shorter_collection_range() -> None:
    months = _recent_months(3, today=date(2026, 1, 20))

    assert months == ["202511", "202512", "202601"]


def test_parse_item_element_maps_child_tags_to_text() -> None:
    item_el = ET.fromstring(
        "<item>"
        "<aptNm>종로중흥S클래스</aptNm>"
        "<dealAmount>12,000</dealAmount>"
        "<cdealDay></cdealDay>"
        "</item>"
    )

    assert _parse_item_element(item_el) == {
        "aptNm": "종로중흥S클래스",
        "dealAmount": "12,000",
        "cdealDay": "",
    }


def test_parse_response_extracts_header_items_and_total_count() -> None:
    root = ET.fromstring(
        "<response>"
        "<header><resultCode>000</resultCode><resultMsg>OK</resultMsg></header>"
        "<body>"
        "<items>"
        "<item><umdNm>숭인동</umdNm></item>"
        "<item><umdNm>창신동</umdNm></item>"
        "</items>"
        "<totalCount>2</totalCount>"
        "</body>"
        "</response>"
    )

    result_code, result_msg, items, total_count = _parse_response(root)

    assert result_code == "000"
    assert result_msg == "OK"
    assert items == [{"umdNm": "숭인동"}, {"umdNm": "창신동"}]
    assert total_count == 2


def test_parse_response_handles_no_data_result_with_missing_items() -> None:
    root = ET.fromstring(
        "<response>"
        "<header><resultCode>03</resultCode><resultMsg>NO_DATA</resultMsg></header>"
        "<body><totalCount>0</totalCount></body>"
        "</response>"
    )

    result_code, result_msg, items, total_count = _parse_response(root)

    assert result_code == "03"
    assert result_msg == "NO_DATA"
    assert items == []
    assert total_count == 0


def test_stan_regin_cd_result_code_strips_prefix() -> None:
    root = ET.fromstring(
        "<StanReginCd>"
        "<head><RESULT>"
        "<resultCode>INFO-0</resultCode>"
        "<resultMsg>NORMAL SERVICE.</resultMsg>"
        "</RESULT></head>"
        "</StanReginCd>"
    )

    code, result_msg = _stan_regin_cd_result_code(root)

    assert code == "0"
    assert result_msg == "NORMAL SERVICE."


def test_stan_regin_cd_result_code_strips_prefix_from_error_code() -> None:
    root = ET.fromstring(
        "<StanReginCd>"
        "<head><RESULT>"
        "<resultCode>ERROR-337</resultCode>"
        "<resultMsg>LIMITED NUMBER OF SERVICE REQUESTS EXCEEDS ERROR.</resultMsg>"
        "</RESULT></head>"
        "</StanReginCd>"
    )

    code, result_msg = _stan_regin_cd_result_code(root)

    assert code == "337"
    assert result_msg == "LIMITED NUMBER OF SERVICE REQUESTS EXCEEDS ERROR."
