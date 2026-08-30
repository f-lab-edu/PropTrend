import contextlib
import hashlib
import json
import logging
import os
from collections.abc import AsyncGenerator, Iterable
from contextlib import asynccontextmanager
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import async_session_factory, engine
from app.model import PropertyType, RentTransaction, SaleTransaction

logger = logging.getLogger(__name__)

RESULTS_DIR = Path(
    os.environ.get(
        "RESULTS_DIR", str(Path(__file__).resolve().parents[2] / "scripts" / "results")
    )
)

# api_id -> (property_type, "sale" | "rent")
SOURCE_DIRS: dict[str, tuple[PropertyType, str]] = {
    "apart_sale": (PropertyType.APT, "sale"),
    "apart_rent": (PropertyType.APT, "rent"),
    "officetel_sale": (PropertyType.OFFICETEL, "sale"),
    "officetel_rent": (PropertyType.OFFICETEL, "rent"),
    "multiflex_sale": (PropertyType.ROW_HOUSE, "sale"),
    "multiflex_rent": (PropertyType.ROW_HOUSE, "rent"),
    "single_multi_family_sale": (PropertyType.SINGLE_MULTI, "sale"),
    "single_multi_family_rent": (PropertyType.SINGLE_MULTI, "rent"),
}

BATCH_SIZE = 500

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


def _load_items(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
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


def _transform_sale_item(
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


def _transform_rent_item(
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


def _chunked(rows: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


async def _upsert_rows(
    session: AsyncSession,
    model: type[SaleTransaction | RentTransaction],
    rows: list[dict[str, Any]],
) -> None:
    for batch in _chunked(rows, BATCH_SIZE):
        stmt = (
            pg_insert(model)
            .values(batch)
            .on_conflict_do_nothing(index_elements=["dedup_hash"])
        )
        await session.execute(stmt)


async def _ingest_file(
    session: AsyncSession, path: Path, property_type: PropertyType, kind: str
) -> int:
    items = _load_items(path)
    if not items:
        return 0
    if kind == "sale":
        rows = [_transform_sale_item(item, property_type) for item in items]
        await _upsert_rows(session, SaleTransaction, rows)
    else:
        rows = [_transform_rent_item(item, property_type) for item in items]
        await _upsert_rows(session, RentTransaction, rows)
    return len(rows)


async def run_data_extraction_job() -> None:
    """scripts/results의 월별 실거래가 JSON을 읽어 DB에 배치 적재한다."""
    total = 0
    async with async_session_factory() as session:
        for api_id, (property_type, kind) in SOURCE_DIRS.items():
            source_dir = RESULTS_DIR / api_id
            if not source_dir.is_dir():
                logger.warning("source directory not found: %s", source_dir)
                continue
            for path in sorted(source_dir.glob("*.json")):
                try:
                    total += await _ingest_file(session, path, property_type, kind)
                    await session.commit()
                except Exception:
                    await session.rollback()
                    logger.exception("failed to ingest file: %s", path)
    logger.info("data extraction job finished: %d rows processed", total)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_data_extraction_job, "cron", hour=4, minute=0)
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        with contextlib.suppress(Exception):
            await engine.dispose()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
