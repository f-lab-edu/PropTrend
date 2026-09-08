from datetime import date
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.model.prop_transaction import (
    LegalStandardCode,
    PropertyType,
    RentTransaction,
    SaleTransaction,
)
from data_collection import loader as loader_module
from data_collection.loader import (
    LegalStandardCodeLoader,
    RentTransactionLoader,
    SaleTransactionLoader,
)
from data_collection.processor import (
    LegalDongCodeProcessor,
    RTMSAptRentProcessor,
    RTMSAptTradeProcessor,
    RTMSSHTradeProcessor,
)

APT_TRADE_ITEM = {
    "aptDong": "",
    "aptNm": "종로중흥S클래스",
    "buildYear": "2013",
    "buyerGbn": "개인",
    "cdealDay": "",
    "cdealType": "",
    "dealAmount": "12,000",
    "dealDay": "23",
    "dealMonth": "7",
    "dealYear": "2024",
    "dealingGbn": "중개거래",
    "estateAgentSggNm": "서울 종로구",
    "excluUseAr": "17.811",
    "floor": "10",
    "jibun": "202-3",
    "landLeaseholdGbn": "N",
    "rgstDate": "",
    "sggCd": "11110",
    "slerGbn": "개인",
    "umdNm": "숭인동",
}

SH_TRADE_ITEM = {
    "buildYear": "1973",
    "buyerGbn": "개인",
    "cdealDay": "",
    "cdealType": "",
    "dealAmount": "98,700",
    "dealDay": "13",
    "dealMonth": "7",
    "dealYear": "2024",
    "dealingGbn": "중개거래",
    "estateAgentSggNm": "서울 종로구",
    "houseType": "단독",
    "jibun": "",
    "plottageAr": "83",
    "sggCd": "11110",
    "slerGbn": "개인",
    "totalFloorAr": "120.2",
    "umdNm": "옥인동",
}

APT_RENT_ITEM = {
    "aptNm": "두산",
    "aptSeq": "11110-34",
    "buildYear": "1999",
    "contractTerm": "",
    "contractType": "",
    "dealDay": "20",
    "dealMonth": "7",
    "dealYear": "2024",
    "deposit": "50,000",
    "excluUseAr": "59.95",
    "floor": "3",
    "jibun": "232",
    "monthlyRent": "0",
    "preDeposit": "",
    "preMonthlyRent": "",
    "roadnm": "지봉로5길 7",
    "roadnmbcd": "0",
    "roadnmbonbun": "00007",
    "roadnmbubun": "00000",
    "roadnmcd": "4100390",
    "roadnmseq": "1",
    "roadnmsggcd": "11110",
    "sggCd": "11110",
    "umdNm": "창신동",
    "useRRRight": "",
}


def _apt_trade_rows(count: int = 1) -> list[dict[str, Any]]:
    # 같은 갱신 단위(종로구 2024-07)에 속하는, 계약일만 다른 행들.
    items = [{**APT_TRADE_ITEM, "dealDay": str(day)} for day in range(1, count + 1)]
    return RTMSAptTradeProcessor().process(items)


async def _count(session_factory: async_sessionmaker, model: type) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(model))


async def test_load_inserts_sale_rows(session_factory: async_sessionmaker) -> None:
    rows = _apt_trade_rows()

    inserted = await SaleTransactionLoader(session_factory).load(rows)

    assert inserted == 1
    async with session_factory() as session:
        saved = await session.scalar(select(SaleTransaction))
    assert saved.property_type == PropertyType.APT
    assert saved.building_name == "종로중흥S클래스"
    assert saved.deal_date == date(2024, 7, 1)
    assert saved.deal_amount == 120_000_000
    assert saved.exclusive_use_area == Decimal("17.811")


async def test_load_returns_zero_for_empty_rows(
    session_factory: async_sessionmaker,
) -> None:
    assert await SaleTransactionLoader(session_factory).load([]) == 0


async def test_load_accepts_rows_from_processors_with_different_keys(
    session_factory: async_sessionmaker,
) -> None:
    # 처리기마다 유형별 필드가 달라 키 집합이 다르지만, 한 번에 넘겨도 적재되어야 한다.
    rows = _apt_trade_rows() + RTMSSHTradeProcessor().process([SH_TRADE_ITEM])

    inserted = await SaleTransactionLoader(session_factory).load(rows)

    assert inserted == 2
    async with session_factory() as session:
        saved = (
            await session.scalars(
                select(SaleTransaction).order_by(SaleTransaction.deal_amount)
            )
        ).all()
    apt, single_multi = saved
    assert apt.plottage_area is None
    assert apt.total_floor_area is None
    assert apt.land_leasehold_type == "N"
    assert single_multi.plottage_area == Decimal(83)
    assert single_multi.total_floor_area == Decimal("120.2")
    assert single_multi.apartment_dong is None


async def test_load_replaces_rows_of_the_same_refresh_unit(
    session_factory: async_sessionmaker,
) -> None:
    # 같은 갱신 단위를 다시 적재하면 이전 행이 남지 않고 통째로 교체되어야 한다.
    rows = _apt_trade_rows(3)
    loader = SaleTransactionLoader(session_factory)
    await loader.load(rows)

    inserted = await loader.load(rows)

    assert inserted == 3
    assert await _count(session_factory, SaleTransaction) == 3


async def test_load_drops_rows_no_longer_reported_for_the_refresh_unit(
    session_factory: async_sessionmaker,
) -> None:
    # 계약 해제 등으로 원본에서 빠진 거래는 재적재 후 DB에도 남아 있으면 안 된다.
    loader = SaleTransactionLoader(session_factory)
    await loader.load(_apt_trade_rows(3))

    inserted = await loader.load(_apt_trade_rows(1))

    assert inserted == 1
    assert await _count(session_factory, SaleTransaction) == 1


async def test_load_keeps_indistinguishable_rows_inside_one_batch(
    session_factory: async_sessionmaker,
) -> None:
    # 원본 API에 거래 ID가 없어 모든 값이 같은 서로 다른 거래가 올 수 있는데,
    # 중복으로 오인해 버리지 않고 그대로 적재해야 한다(단독·다가구는 지번/건물명/
    # 층/전용면적이 모두 비어 와서 특히 흔하다).
    rows = RTMSSHTradeProcessor().process([SH_TRADE_ITEM, SH_TRADE_ITEM])

    inserted = await SaleTransactionLoader(session_factory).load(rows)

    assert inserted == 2
    assert await _count(session_factory, SaleTransaction) == 2


async def test_load_keeps_other_refresh_units_untouched(
    session_factory: async_sessionmaker,
) -> None:
    # 다른 시군구/계약년월 데이터까지 삭제 범위에 들어가면 안 된다.
    other_unit_items = [
        {**APT_TRADE_ITEM, "sggCd": "41135"},
        {**APT_TRADE_ITEM, "dealMonth": "8"},
    ]
    loader = SaleTransactionLoader(session_factory)
    await loader.load(
        _apt_trade_rows() + RTMSAptTradeProcessor().process(other_unit_items)
    )

    await loader.load(_apt_trade_rows())

    assert await _count(session_factory, SaleTransaction) == 3


async def test_load_inserts_every_chunk(
    session_factory: async_sessionmaker, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(loader_module, "CHUNK_SIZE", 2)
    rows = _apt_trade_rows(5)

    inserted = await SaleTransactionLoader(session_factory).load(rows)

    assert inserted == 5
    assert await _count(session_factory, SaleTransaction) == 5


async def test_load_inserts_rent_rows_with_road_address_detail(
    session_factory: async_sessionmaker,
) -> None:
    rows = RTMSAptRentProcessor().process([APT_RENT_ITEM])

    inserted = await RentTransactionLoader(session_factory).load(rows)

    assert inserted == 1
    async with session_factory() as session:
        saved = await session.scalar(select(RentTransaction))
    assert saved.deposit == 500_000_000
    assert saved.monthly_rent == 0
    assert saved.apartment_serial_number == "11110-34"
    assert saved.total_floor_area is None
    assert saved.road_address_detail == {
        "roadnm": "지봉로5길 7",
        "roadnmsggcd": "11110",
        "roadnmcd": "4100390",
        "roadnmseq": "1",
        "roadnmbcd": "0",
        "roadnmbonbun": "00007",
        "roadnmbubun": "00000",
    }


def _legal_dong_rows(name: str = "서울특별시 종로구") -> list[dict[str, str]]:
    item = {
        "sido_cd": "11",
        "sgg_cd": "110",
        "umd_cd": "000",
        "ri_cd": "00",
        "locatadd_nm": name,
    }
    return LegalDongCodeProcessor().process([item])


async def test_load_inserts_legal_standard_codes(
    session_factory: async_sessionmaker,
) -> None:
    rows = _legal_dong_rows()

    loaded = await LegalStandardCodeLoader(session_factory).load(rows)

    assert loaded == 1
    async with session_factory() as session:
        saved = await session.scalar(select(LegalStandardCode))
    assert saved.code == "11110"
    assert saved.name == "서울특별시 종로구"


async def test_load_updates_renamed_legal_standard_code(
    session_factory: async_sessionmaker,
) -> None:
    loader = LegalStandardCodeLoader(session_factory)
    await loader.load(_legal_dong_rows())

    # 같은 코드로 다시 수집되면 최신 행정구역명으로 갱신되어야 한다.
    loaded = await loader.load(_legal_dong_rows("서울특별시 종로구(개칭)"))

    assert loaded == 1
    assert await _count(session_factory, LegalStandardCode) == 1
    async with session_factory() as session:
        saved = await session.scalar(select(LegalStandardCode))
    assert saved.name == "서울특별시 종로구(개칭)"


async def test_load_keeps_last_row_for_duplicate_code_inside_one_batch(
    session_factory: async_sessionmaker,
) -> None:
    rows = _legal_dong_rows() + _legal_dong_rows("서울특별시 종로구(최신)")

    loaded = await LegalStandardCodeLoader(session_factory).load(rows)

    assert loaded == 1
    async with session_factory() as session:
        saved = await session.scalar(select(LegalStandardCode))
    assert saved.name == "서울특별시 종로구(최신)"
