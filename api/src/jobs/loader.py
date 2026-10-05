"""가공을 마친 row를 테이블에 적재하는 적재기."""

import logging
from typing import Any, ClassVar

from sqlalchemy import func, insert, or_, text
from sqlalchemy.dialects.postgresql import Insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..model import (
    COMPLEX_INDEX_WHERE,
    COMPLEX_KEY_COLUMNS,
    LEGAL_DONG_CODE_KNOWN_FIELDS,
    RTMS_KNOWN_FIELDS,
    Base,
    Complex,
    LegalDongCodeRawItem,
    PropertyType,
    Region,
    RentTransaction,
    RTMSRawItem,
    SaleTransaction,
)

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1000

# 정제 테이블 row에서 단지로 옮기는 컬럼.
COMPLEX_COLUMNS = (
    "sido_code",
    "sigungu_code",
    "umd_name",
    "jibun",
    "building_name",
    "build_year",
    "apartment_serial_number",
)


def _warn_schema_drift(api_id: str, items: list[dict[str, Any]], known: frozenset[str]) -> None:
    """payload 키를 기준선과 대조해 경고만 남긴다. 적재 자체는 어떤 키가 와도 막지 않는다."""
    seen = {key for item in items for key in item}
    if unknown := seen - known:
        logger.warning("%s: 기준선에 없는 필드가 왔다 %s", api_id, sorted(unknown))
    if missing := known - seen:
        # 응답은 값이 비어도 태그는 보내므로, 단위 전체에서 한 번도 안 보이면 개명·삭제다.
        logger.warning("%s: 기준선에 있던 필드가 응답에 없다 %s", api_id, sorted(missing))


async def _insert_chunked(session: AsyncSession, table: Any, rows: list[dict[str, Any]]) -> int:
    """모델이 아니라 Table을 넘긴다. 모델을 넘기면 ORM 경로를 타서 느리다."""
    for start in range(0, len(rows), CHUNK_SIZE):
        await session.execute(insert(table), rows[start : start + CHUNK_SIZE])
    return len(rows)


class RTMSRawItemLoader:
    """실거래가 오픈API 응답 item을 rtms_raw_items에 그대로 적재한다."""

    def __init__(self, session: AsyncSession, api_id: str) -> None:
        # 오타 난 api_id는 갱신 단위 삭제가 영영 못 찾는 행을 만들어 매일 중복을 쌓는다.
        # RTMS_KNOWN_FIELDS의 키가 곧 허용된 슬러그 8종이라 그대로 검사에 쓴다.
        if api_id not in RTMS_KNOWN_FIELDS:
            raise ValueError(f"모르는 api_id다: {api_id!r}")
        self.session = session
        self.api_id = api_id

    async def load(self, lawd_cd: str, deal_ymd: str, items: list[dict[str, Any]]) -> int:
        """응답 item을 한 건도 거르지 않고 넣고, 실제로 넣은 수를 반환한다."""
        # 빈 목록에 드리프트 검사를 돌리면 기준선 전 필드가 사라진 것으로 보여 오경보가 난다.
        if items:
            _warn_schema_drift(self.api_id, items, RTMS_KNOWN_FIELDS[self.api_id])

            # 갱신 단위 키는 응답이 아니라 요청 파라미터에서 온다. collected_at은 server_default에
            # 맡기려고 키 자체를 넣지 않는다(None을 넣으면 기본값을 덮어 NOT NULL 위반이 난다).
            rows = [
                {"api_id": self.api_id, "lawd_cd": lawd_cd, "deal_ymd": deal_ymd, "payload": item} for item in items
            ]
            loaded = await _insert_chunked(self.session, RTMSRawItem.__table__, rows)
        else:
            loaded = 0

        logger.debug("부동산 실거래 bronze 데이터 적재 완료", extra={"stage": "load_bronze", "loaded": loaded})
        return loaded


class LegalDongCodeRawItemLoader:
    """법정동코드 오픈API 응답 row를 legal_dong_code_raw_items에 그대로 적재한다."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def load(self, items: list[dict[str, Any]]) -> int:
        if items:
            _warn_schema_drift("legal_dong_code", items, LEGAL_DONG_CODE_KNOWN_FIELDS)

            rows = [{"payload": item} for item in items]
            loaded = await _insert_chunked(self.session, LegalDongCodeRawItem.__table__, rows)
        else:
            loaded = 0

        logger.debug("법정동코드 bronze 적재 완료", extra={"stage": "load_legal_dong", "loaded": loaded})
        return loaded


class RegionLoader:
    """전처리기가 넘긴 시도·시군구 행을 regions에 적재한다."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def load(self, rows: list[dict[str, Any]]) -> int:
        # 전처리에서 전부 걸러진 채 확정되면 지역 필터가 통째로 사라진다. 예외로 bronze 교체까지 되돌린다.
        if not rows:
            raise ValueError("적재할 지역 행이 없다")

        loaded = await _insert_chunked(self.session, Region.__table__, rows)
        logger.debug("지역 silver 적재 완료", extra={"stage": "load_region", "loaded": loaded})
        return loaded


class TransactionLoader:
    """전처리기가 넘긴 row를 정제 테이블에 적재한다."""

    model: ClassVar[type[Base]]

    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.columns = frozenset(
            column.name for column in self.model.__table__.columns if column.name not in {"id", "created_at"}
        )

    async def load(self, rows: list[dict[str, Any]]) -> int:
        if rows:
            # 전처리기가 리터럴 dict 하나로 모든 행을 만들므로 첫 행만 검사하면 충분하다.
            keys = rows[0].keys()
            if keys != self.columns:
                raise ValueError(
                    f"적재 row의 키가 {self.model.__tablename__} 컬럼과 다르다: "
                    f"모르는 키={sorted(keys - self.columns)}, "
                    f"빠진 키={sorted(self.columns - keys)}"
                )

            loaded = await _insert_chunked(self.session, self.model.__table__, rows)
        else:
            loaded = 0

        logger.debug(
            "부동산 실거래 silver 데이터 적재 완료",
            extra={"stage": "load_silver", "table": self.model.__tablename__, "loaded": loaded},
        )
        return loaded


class SaleTransactionLoader(TransactionLoader):
    model = SaleTransaction


class RentTransactionLoader(TransactionLoader):
    model = RentTransaction


def with_complex_conflict(statement: Insert, property_type: PropertyType) -> Insert:
    """단지 insert 문에 유형별 충돌 처리를 붙인다. 이미 있는 단지는 새로 만들지 않는다."""
    table = Complex.__table__
    key_columns = COMPLEX_KEY_COLUMNS[property_type]
    conflict_target = {"index_elements": key_columns, "index_where": text(COMPLEX_INDEX_WHERE[property_type])}
    if property_type == PropertyType.APT:
        # 단지명은 과거 거래까지 소급해 바뀐다. 키가 아닌 속성은 최근 거래 값으로 맞추되,
        # 매일 같은 값으로 덮어 updated_at과 행 버전만 쌓이지 않도록 달라졌을 때만 고친다.
        updatable = [column for column in COMPLEX_COLUMNS if column not in key_columns]
        return statement.on_conflict_do_update(
            **conflict_target,
            set_={column: statement.excluded[column] for column in updatable} | {"updated_at": func.now()},
            where=or_(*(table.c[column].is_distinct_from(statement.excluded[column]) for column in updatable)),
        )
    # 오피스텔은 키 컬럼이 곧 속성 전부라 고칠 값이 없다.
    return statement.on_conflict_do_nothing(**conflict_target)


class ComplexLoader:
    """정제 테이블에 적재한 row에서 단지를 뽑아 complexes에 upsert한다."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def load(self, property_type: PropertyType, rows: list[dict[str, Any]]) -> int:
        """단지별로 가장 최근 거래의 속성을 upsert하고, upsert를 시도한 단지 수를 반환한다."""
        key_columns = COMPLEX_KEY_COLUMNS[property_type]
        latest: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in rows:
            # 상세 자료로 재수집되기 전의 아파트 매매는 단지 일련번호가 없어 단지를 특정할 수 없다.
            if property_type == PropertyType.APT and row["apartment_serial_number"] is None:
                continue
            key = tuple(row[column] for column in key_columns)
            if key not in latest or row["deal_date"] > latest[key]["deal_date"]:
                latest[key] = row

        # 같은 단지를 upsert하는 단위가 동시에 돌 때 행 락을 서로 다른 순서로 잡아 데드락이 나지 않게 키 순으로 넣는다.
        # NULL과 값을 직접 비교하면 TypeError가 나므로 NULL 여부를 먼저 비교한다.
        ordered = sorted(latest.items(), key=lambda item: tuple((value is not None, value) for value in item[0]))
        complexes = [
            {"property_type": property_type} | {column: row[column] for column in COMPLEX_COLUMNS} for _, row in ordered
        ]

        if complexes:
            statement = with_complex_conflict(pg_insert(Complex.__table__), property_type)
            for start in range(0, len(complexes), CHUNK_SIZE):
                await self.session.execute(statement, complexes[start : start + CHUNK_SIZE])

        logger.debug("단지 upsert 완료", extra={"stage": "load_complex", "loaded": len(complexes)})
        return len(complexes)
