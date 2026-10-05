"""이미 적재된 정제 테이블 전체에서 아파트·오피스텔 단지를 뽑아 complexes를 채우는 1회성 배치.

이후의 단지는 갱신 파이프라인이 단위마다 upsert한다. 한 번 돌려 기존 데이터를 채우면 이 파일을 지운다.
같은 충돌 처리를 쓰므로 여러 번 돌려도 단지가 중복되지 않는다.

    python -m src.jobs.backfill_complexes
"""

import asyncio
import logging
import time

from dotenv import load_dotenv
from sqlalchemy import literal, select, union_all
from sqlalchemy.dialects.postgresql import insert

from ..db import dispose_engine, session_scope
from ..logging_config import configure_logging
from ..model import COMPLEX_PROPERTY_TYPES, Complex, PropertyType, RentTransaction, SaleTransaction
from .__main__ import EXIT_FAILED, EXIT_OK
from .loader import COMPLEX_COLUMNS, with_complex_conflict
from .lock import advisory_lock

logger = logging.getLogger(__name__)


async def backfill(property_type: PropertyType) -> int:
    """한 유형의 단지를 매매·전월세 전체에서 뽑아 upsert하고, 영향받은 행 수를 반환한다."""
    sources = []
    for table in (SaleTransaction.__table__, RentTransaction.__table__):
        conditions = [table.c.property_type == property_type]
        # 상세 자료로 재수집되기 전의 아파트 매매는 단지 일련번호가 없어 단지를 특정할 수 없다.
        if property_type == PropertyType.APT:
            conditions.append(table.c.apartment_serial_number.is_not(None))
        sources.append(select(*(table.c[column] for column in COMPLEX_COLUMNS), table.c.deal_date).where(*conditions))
    source = union_all(*sources).subquery()

    complexes = select(literal(property_type.value), *(source.c[column] for column in COMPLEX_COLUMNS))
    if property_type == PropertyType.APT:
        # 단지명은 과거 거래까지 소급해 바뀌므로 가장 최근 거래의 속성을 쓴다.
        # 한 문장 안에서 같은 단지를 두 번 고치면 ON CONFLICT DO UPDATE가 실패하므로 단지당 한 행만 남긴다.
        serial_number = source.c.apartment_serial_number
        complexes = complexes.distinct(serial_number).order_by(serial_number, source.c.deal_date.desc())
    else:
        # DISTINCT도 NULL끼리 같은 값으로 보므로 단지 유니크 인덱스(NULLS NOT DISTINCT)와 기준이 같다.
        complexes = complexes.distinct()

    statement = insert(Complex.__table__).from_select(["property_type", *COMPLEX_COLUMNS], complexes)
    async with session_scope() as session:
        result = await session.execute(with_complex_conflict(statement, property_type))
    return result.rowcount


async def main() -> int:
    configure_logging("pipeline")
    try:
        # 정기 갱신과 같은 락을 쓴다. 정제 테이블이 갈아끼워지는 도중의 구간을 읽지 않게 한다.
        async with advisory_lock() as acquired:
            if not acquired:
                logger.error("다른 프로세스가 갱신 중이라 단지를 채우지 못했다", extra={"stage": "lock_busy"})
                return EXIT_FAILED

            for property_type in sorted(COMPLEX_PROPERTY_TYPES):
                started = time.perf_counter()
                affected = await backfill(property_type)
                logger.info(
                    "%s 단지 %d건 upsert",
                    property_type,
                    affected,
                    extra={
                        "stage": "backfill_complex",
                        "affected": affected,
                        "elapsed_ms": round((time.perf_counter() - started) * 1000),
                    },
                )
    except Exception:
        logger.exception("단지 백필이 예외로 중단됐다", extra={"stage": "backfill_aborted"})
        return EXIT_FAILED
    finally:
        await dispose_engine()
    return EXIT_OK


if __name__ == "__main__":
    load_dotenv()
    raise SystemExit(asyncio.run(main()))
