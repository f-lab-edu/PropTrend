from abc import ABC, abstractmethod
from calendar import monthrange
from collections import defaultdict
from collections.abc import Iterator
from datetime import date
from typing import Any, ClassVar

from sqlalchemy import Delete, delete, insert
from sqlalchemy.dialects.postgresql import Insert as PostgresqlInsert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import Insert as SQLiteInsert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.model.base import Base
from app.model.prop_transaction import (
    LegalStandardCode,
    PropertyType,
    RentTransaction,
    SaleTransaction,
)

# 실거래가 테이블 중 컬럼이 가장 많은 sale_transactions가 25컬럼이므로, 한 번의 INSERT에
# 묶이는 바인드 파라미터는 최대 25,000개다(PostgreSQL 한도 65,535 이내).
CHUNK_SIZE = 1000

type DialectInsert = PostgresqlInsert | SQLiteInsert

# 갱신 단위: (부동산 유형, 시도 코드, 시군구 코드, 계약월 첫날, 계약월 마지막날)
type RefreshUnit = tuple[PropertyType, str, str, date, date]

# ON CONFLICT 구문은 방언별 INSERT에만 있고, 두 방언의 API가 동일하다.
_INSERT_BUILDERS = {
    "postgresql": postgresql_insert,
    "sqlite": sqlite_insert,
}


class UnsupportedDialectError(Exception):
    pass


class DataLoader(ABC):
    @abstractmethod
    async def load(self, rows: list[dict[str, Any]]) -> int:
        """가공까지 마친 데이터를 정해진 저장소에 적재하고, 적재된 행 수를 반환한다."""


class _TableLoader(DataLoader):
    """모델 테이블 하나에 dict 목록을 청크 단위로 적재하는 공통 로직."""

    MODEL: ClassVar[type[Base]]

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    def _column_names(self) -> list[str]:
        return [column.name for column in self.MODEL.__table__.columns]

    def _normalize(self, row: dict[str, Any], columns: list[str]) -> dict[str, Any]:
        # 처리기마다 부동산 유형별 필드가 있고 없고가 달라 키 집합이 제각각인데, 대량
        # INSERT는 모든 행의 키가 같아야 하므로 테이블 컬럼 기준으로 빈 값을 채워준다.
        return {column: row.get(column) for column in columns}

    def _chunks(self, rows: list[dict[str, Any]]) -> Iterator[list[dict[str, Any]]]:
        for start in range(0, len(rows), CHUNK_SIZE):
            yield rows[start : start + CHUNK_SIZE]


class _PropTransactionLoader(_TableLoader):
    """매매/전월세 실거래가 테이블 적재의 공통 로직.

    원본 API에는 거래 ID가 없고 자연키가 될 만한 컬럼 조합도 없다(단독·다가구는 지번,
    건물명, 층, 전용면적이 모두 비어 온다). 그래서 개별 행 단위로 중복을 걸러내는 대신,
    수집 단위와 같은 갱신 단위((부동산 유형, 시군구, 계약년월)) 전체를 지우고 새로
    수집한 행으로 통째로 교체한다. 원본에 우연히 모든 값이 같은 서로 다른 거래가 있어도
    유실되지 않고, 신고 지연·계약 해제로 지난 달 데이터가 나중에 바뀌어도 그대로
    반영된다."""

    MODEL: ClassVar[type[SaleTransaction | RentTransaction]]

    async def load(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        columns = self._column_names()
        loaded = 0
        async with self._session_factory() as session:
            for unit, unit_rows in self._group_by_refresh_unit(rows).items():
                # 삭제와 삽입을 한 트랜잭션으로 묶어야, 중간에 실패해도 해당 갱신 단위가
                # 비어 있거나 반만 채워진 상태로 남지 않는다.
                async with session.begin():
                    await session.execute(self._delete_statement(unit))
                    for chunk in self._chunks(unit_rows):
                        values = [self._normalize(row, columns) for row in chunk]
                        # values(list) 대신 파라미터 목록을 넘긴다. 전자는 행 수만큼
                        # VALUES 절을 펼친 SQL을 컴파일 단계에서 만들어내는데, 컴파일
                        # 캐시 키에 행 수가 들어가 갱신 단위마다 행 수가 다른 이 적재에서는
                        # 매번 캐시 미스가 난다. 후자는 1행짜리 문장만 컴파일해 캐시에 태우고
                        # 확장은 insertmanyvalues에 맡기므로, 같은 SQL·같은 왕복 횟수로
                        # 훨씬 싸게 끝난다(실측 3.5천행/s -> 3.7만행/s).
                        await session.execute(insert(self.MODEL.__table__), values)
                loaded += len(unit_rows)
        return loaded

    def _column_names(self) -> list[str]:
        # id는 시퀀스가 채우는 대리키라 적재 대상에서 제외한다.
        return [name for name in super()._column_names() if name != "id"]

    def _group_by_refresh_unit(
        self, rows: list[dict[str, Any]]
    ) -> dict[RefreshUnit, list[dict[str, Any]]]:
        # 삭제 범위를 적재할 행에서 직접 뽑아내므로, 삭제 범위와 삽입 범위가 어긋날 수
        # 없다. 수집되지 않은 갱신 단위는 키가 만들어지지 않아 건드리지도 않는다.
        groups: dict[RefreshUnit, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[self._refresh_unit(row)].append(row)
        return groups

    def _refresh_unit(self, row: dict[str, Any]) -> RefreshUnit:
        deal_date: date = row["deal_date"]
        last_day = monthrange(deal_date.year, deal_date.month)[1]
        return (
            row["property_type"],
            row["sido_code"],
            row["sigungu_code"],
            deal_date.replace(day=1),
            deal_date.replace(day=last_day),
        )

    def _delete_statement(self, unit: RefreshUnit) -> Delete:
        property_type, sido_code, sigungu_code, first_day, last_day = unit
        model = self.MODEL
        return delete(model).where(
            model.property_type == property_type,
            model.sido_code == sido_code,
            model.sigungu_code == sigungu_code,
            model.deal_date >= first_day,
            model.deal_date <= last_day,
        )


class SaleTransactionLoader(_PropTransactionLoader):
    """아파트/오피스텔/연립다세대/단독다가구 매매 실거래 데이터 적재 모듈."""

    MODEL = SaleTransaction


class RentTransactionLoader(_PropTransactionLoader):
    """아파트/오피스텔/연립다세대/단독다가구 전월세 실거래 데이터 적재 모듈."""

    MODEL = RentTransaction


class LegalStandardCodeLoader(_TableLoader):
    """법정동코드(시군구 단위) 마스터 적재 모듈."""

    MODEL = LegalStandardCode

    async def load(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        columns = self._column_names()
        targets = self._deduplicate([self._normalize(row, columns) for row in rows])
        loaded = 0
        async with self._session_factory() as session:
            for chunk in self._chunks(targets):
                result = await session.execute(self._upsert_statement(session, chunk))
                await session.commit()
                loaded += result.rowcount
        return loaded

    def _deduplicate(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        # 한 INSERT 안에서 같은 코드가 두 번 나오면 ON CONFLICT DO UPDATE가 같은 행을
        # 두 번 건드려 실패하므로 미리 걸러낸다. 뒤에 온 행을 더 최신으로 본다.
        unique_rows = {row["code"]: row for row in rows}
        return list(unique_rows.values())

    def _upsert_statement(
        self, session: AsyncSession, chunk: list[dict[str, Any]]
    ) -> DialectInsert:
        dialect_name = session.get_bind().dialect.name
        builder = _INSERT_BUILDERS.get(dialect_name)
        if builder is None:
            raise UnsupportedDialectError(
                f"지원하지 않는 DB 방언입니다: {dialect_name}"
            )
        statement = builder(self.MODEL).values(chunk)
        # 코드는 그대로 두고 행정구역명만 바뀌는 경우가 있어, 이미 있는 코드는 건너뛰지
        # 않고 최신 이름으로 갱신한다.
        return statement.on_conflict_do_update(
            index_elements=["code"],
            set_={"name": statement.excluded.name},
        )
