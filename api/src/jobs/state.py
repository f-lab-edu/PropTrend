"""갱신 단위 상태 표를 읽고 쓴다. 트랜잭션 경계는 부르는 쪽(pipeline)이 정한다."""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..model import LAST_ERROR_MAX, RefreshUnitState, UnitStatus

logger = logging.getLogger(__name__)

# 한 회차가 한 단위에 최대 2회를 쓴다(본 패스 + 정제 2차 패스). 10이면 대략 5일치다.
# 상수만 올리면 상한에 걸려 빠졌던 행도 다음 회차부터 다시 대상이 된다.
MAX_ATTEMPTS = 10

_KEY_COLUMNS = ("api_id", "lawd_cd", "deal_ymd")


@dataclass(frozen=True)
class UnitRecord:
    """상태 표에 남아 있는 단위 한 줄."""

    api_id: str
    lawd_cd: str
    deal_ymd: str
    status: str
    attempts: int

    def coordinate(self) -> str:
        """`--only`에 그대로 넣을 수 있는 좌표 문자열."""
        return f"{self.api_id}:{self.lawd_cd}:{self.deal_ymd}"


def summarize_error(error: BaseException) -> str:
    """예외를 한 줄로 줄인다. 길이를 잘라 접속 정보가 통째로 실리는 일을 줄인다."""
    return f"{type(error).__name__}: {error}"[:LAST_ERROR_MAX]


class RefreshUnitStateStore:
    """갱신 단위 상태 표 접근. 주어진 세션의 트랜잭션에 그대로 올라탄다."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def mark_collected(self, api_id: str, lawd_cd: str, deal_ymd: str) -> None:
        """bronze 커밋과 같은 트랜잭션에서 '정제 미완'을 남긴다."""
        # attempts를 건드리지 않는다. 여기서 리셋하면 --with-collect 재실행이 매번 카운터를
        # 0으로 돌려 시도 상한이 영영 걸리지 않는다.
        await self._upsert(
            {"api_id": api_id, "lawd_cd": lawd_cd, "deal_ymd": deal_ymd, "status": UnitStatus.COLLECTED},
            {"status": UnitStatus.COLLECTED},
        )

    async def mark_failed(
        self,
        api_id: str,
        lawd_cd: str,
        deal_ymd: str,
        status: UnitStatus,
        error: BaseException,
    ) -> None:
        """실패를 남기고 시도 횟수를 1 올린다. 롤백된 트랜잭션 밖에서 불러야 한다."""
        last_error = summarize_error(error)
        await self._upsert(
            {
                "api_id": api_id,
                "lawd_cd": lawd_cd,
                "deal_ymd": deal_ymd,
                "status": status,
                "attempts": 1,
                "last_error": last_error,
            },
            {
                "status": status,
                "attempts": RefreshUnitState.__table__.c.attempts + 1,
                "last_error": last_error,
            },
        )

    async def clear(self, api_id: str, lawd_cd: str, deal_ymd: str) -> None:
        """정제 커밋과 같은 트랜잭션에서 행을 지운다. 롤백되면 삭제도 함께 되돌아간다."""
        table = RefreshUnitState.__table__
        await self.session.execute(
            delete(table).where(
                table.c.api_id == api_id,
                table.c.lawd_cd == lawd_cd,
                table.c.deal_ymd == deal_ymd,
            )
        )

    async def pending(
        self,
        statuses: Sequence[UnitStatus],
        max_attempts: int = MAX_ATTEMPTS,
    ) -> list[UnitRecord]:
        """다시 돌릴 단위를 최신 월부터 돌려준다. 시도 상한을 넘긴 행은 뺀다."""
        table = RefreshUnitState.__table__
        # 상한을 넘긴 행은 코드를 고치기 전엔 결과가 같다. --only로는 여전히 지목할 수 있다.
        return await self._select(table.c.status.in_(list(statuses)), table.c.attempts < max_attempts)

    async def leftovers(self) -> list[UnitRecord]:
        """회차가 끝난 뒤에도 남아 있는 단위 전부. 상한을 넘긴 것도 센다."""
        return await self._select()

    async def _select(self, *conditions: Any) -> list[UnitRecord]:
        table = RefreshUnitState.__table__
        # iter_units와 같은 "최신 월 먼저" 순서를 따른다. 중간에 잘려도 최근 것이 먼저 복구된다.
        statement = (
            select(table.c.api_id, table.c.lawd_cd, table.c.deal_ymd, table.c.status, table.c.attempts)
            .where(*conditions)
            .order_by(table.c.deal_ymd.desc(), table.c.lawd_cd, table.c.api_id)
        )
        rows = (await self.session.execute(statement)).all()
        return [UnitRecord(row[0], row[1], row[2], row[3], row[4]) for row in rows]

    async def _upsert(self, values: dict[str, Any], set_: dict[str, Any]) -> None:
        statement = pg_insert(RefreshUnitState).values(**values)
        await self.session.execute(
            statement.on_conflict_do_update(index_elements=list(_KEY_COLUMNS), set_=set_ | {"updated_at": func.now()})
        )
