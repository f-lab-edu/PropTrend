from typing import TYPE_CHECKING, ClassVar

from sqlalchemy import Table
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    if TYPE_CHECKING:
        # SQLAlchemy는 서브쿼리 매핑까지 고려해 FromClause로 표기하지만 이 프로젝트의 모델은
        # 모두 Table에 매핑된다. 그대로 두면 insert/delete에 __table__을 넘길 때마다 타입 오류가 난다.
        __table__: ClassVar[Table]  # pyright: ignore[reportIncompatibleVariableOverride]
