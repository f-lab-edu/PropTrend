"""수집한 원본 row를 raw 테이블에 적재하는 적재기 인터페이스."""

from abc import ABC, abstractmethod
from typing import Any

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..model import Base


class DataLoader(ABC):
    """수집기가 넘긴 row 목록을 저장소에 적재하는 인터페이스."""

    @abstractmethod
    async def load(self, rows: list[dict[str, Any]]) -> int:
        """row 목록을 적재하고 적재한 건수를 반환한다."""


class RawDataLoader(DataLoader):
    """API 응답 필드명을 그대로 컬럼으로 갖는 raw 테이블 공용 적재기.

    raw 테이블은 응답 필드명을 컬럼명으로 쓰고 값도 모두 문자열이므로, 모델에
    없는 키를 버리는 것 외에 별도의 변환이 필요 없다. 그래서 9개 API가 모델만
    바꿔가며 이 적재기 하나를 공유한다.

    트랜잭션은 세션을 넘긴 호출자가 관리한다(이 클래스는 commit하지 않는다).
    """

    CHUNK_SIZE = 1000

    def __init__(self, session: AsyncSession, model: type[Base]) -> None:
        self.session = session
        self.model = model
        # id/created_at은 DB가 채우므로 적재 대상에서 제외한다.
        self.columns = tuple(
            column.name
            for column in model.__table__.columns
            if column.name not in {"id", "created_at"}
        )

    async def load(self, rows: list[dict[str, Any]]) -> int:
        # executemany는 모든 파라미터 dict의 키가 같아야 하므로, 모델에 없는 키를 버리는
        # 것에 더해 응답에 빠진 컬럼도 None으로 채워 키 집합을 컬럼 전체로 고정한다.
        payload = [
            {column: row.get(column) for column in self.columns}
            for row in rows
            if any(key in self.columns for key in row)
        ]
        if not payload:
            return 0

        for start in range(0, len(payload), self.CHUNK_SIZE):
            # 모델이 아니라 __table__을 넘긴다. 모델을 넘기면 ORM(bulk_persistence) 경로를
            # 타서 처리량이 한 자릿수로 떨어진다(docs/temp/bulk-insert-compile-cache.md).
            await self.session.execute(
                insert(self.model.__table__), payload[start : start + self.CHUNK_SIZE]
            )

        return len(payload)
