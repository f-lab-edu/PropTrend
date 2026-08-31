import logging
from collections.abc import Iterable
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.model import PropertyType, RentTransaction, SaleTransaction
from app.scheduler.transform import (
    _load_items,
    transform_rent_item,
    transform_sale_item,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 500


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


async def ingest_file(
    session: AsyncSession, content: str, property_type: PropertyType, kind: str
) -> int:
    items = _load_items(content)
    if not items:
        return 0
    if kind == "sale":
        rows = [transform_sale_item(item, property_type) for item in items]
        await _upsert_rows(session, SaleTransaction, rows)
    else:
        rows = [transform_rent_item(item, property_type) for item in items]
        await _upsert_rows(session, RentTransaction, rows)
    return len(rows)
