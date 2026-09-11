"""가공을 마친 row를 테이블에 적재하는 적재기."""

from typing import Any, ClassVar

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..model import Base, RentTransaction, SaleTransaction


class RawDataLoader:
    """API 응답 필드명을 그대로 컬럼으로 갖는 raw 테이블 공용 적재기."""

    CHUNK_SIZE = 1000

    def __init__(self, session: AsyncSession, model: type[Base]) -> None:
        self.session = session
        self.model = model
        self.columns = tuple(
            column.name
            for column in model.__table__.columns
            if column.name not in {"id", "created_at"}
        )

    async def load(self, rows: list[dict[str, Any]]) -> int:
        # executemany는 모든 파라미터 dict의 키가 같아야 해서 컬럼 전체로 고정한다.
        payload = [
            {column: row.get(column) for column in self.columns}
            for row in rows
            if any(key in self.columns for key in row)
        ]
        if not payload:
            return 0

        for start in range(0, len(payload), self.CHUNK_SIZE):
            # 모델을 넘기면 ORM 경로를 타서 느리다(docs/temp/bulk-insert-compile-cache.md).
            await self.session.execute(
                insert(self.model.__table__), payload[start : start + self.CHUNK_SIZE]
            )

        return len(payload)


class TransactionLoader:
    """전처리기가 넘긴 row를 정제 테이블에 적재한다."""

    CHUNK_SIZE = 1000

    model: ClassVar[type[Base]]

    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.columns = frozenset(
            column.name
            for column in self.model.__table__.columns
            if column.name not in {"id", "created_at"}
        )

    async def load(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        # 전처리기가 리터럴 dict 하나로 모든 행을 만들므로 첫 행만 검사하면 충분하다.
        keys = rows[0].keys()
        if keys != self.columns:
            raise ValueError(
                f"적재 row의 키가 {self.model.__tablename__} 컬럼과 다르다: "
                f"모르는 키={sorted(keys - self.columns)}, "
                f"빠진 키={sorted(self.columns - keys)}"
            )

        for start in range(0, len(rows), self.CHUNK_SIZE):
            await self.session.execute(
                insert(self.model.__table__), rows[start : start + self.CHUNK_SIZE]
            )

        return len(rows)


class SaleTransactionLoader(TransactionLoader):
    model = SaleTransaction


class RentTransactionLoader(TransactionLoader):
    model = RentTransaction
