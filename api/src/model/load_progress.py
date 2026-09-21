from datetime import datetime

from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class RawLoadProgress(Base):
    """raw 테이블에 적재를 마친 원본 파일 기록. 적재와 같은 트랜잭션에 들어간다."""

    __tablename__ = "raw_load_progress"

    # (api_id, yyyymm)이 곧 원본 파일 하나다. 복합 PK가 유일성과 조회 인덱스를 함께 준다.
    api_id: Mapped[str] = mapped_column(String(40), primary_key=True)
    yyyymm: Mapped[str] = mapped_column(String(6), primary_key=True)
    row_count: Mapped[int] = mapped_column(Integer)
    loaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
