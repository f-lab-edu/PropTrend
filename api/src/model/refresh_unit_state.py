"""갱신 단위 중 아직 끝나지 않은 것만 남기는 작업 대기표."""

from datetime import datetime
from enum import StrEnum

from sqlalchemy import DateTime, Integer, String, func, text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

# 마지막 실패 사유 보관 길이. 트레이스백이 아니라 한 줄 요약만 남긴다.
LAST_ERROR_MAX = 500


class UnitStatus(StrEnum):
    """단위가 어디까지 커밋됐는지. 끝난 단위는 행 자체가 없으므로 DONE은 두지 않는다."""

    # bronze는 커밋됐고 정제가 안 끝났다. 오픈API 없이 정제만 다시 돌리면 된다.
    COLLECTED = "COLLECTED"
    # 아무것도 커밋되지 않았다. 다시 돌리려면 수집부터 해야 한다.
    FAILED = "FAILED"


class RefreshUnitState(Base):
    """갱신 단위 1건의 미완 상태. 정제까지 끝나면 같은 트랜잭션에서 행을 지운다."""

    __tablename__ = "refresh_unit_states"

    # 좌표 3개가 그대로 키다. rtms_raw_items의 같은 이름 컬럼과 타입·어휘를 맞춘다.
    api_id: Mapped[str] = mapped_column(String(40), primary_key=True)
    lawd_cd: Mapped[str] = mapped_column(String(5), primary_key=True)
    deal_ymd: Mapped[str] = mapped_column(String(6), primary_key=True)

    # PG ENUM 타입은 create_all이 나중에 값을 더해주지 못한다. 문자열로 두고 어휘는 UnitStatus가 쥔다.
    status: Mapped[str] = mapped_column(String(16))

    # 고칠 수 없는 단위가 매 회차를 갉아먹지 않도록 재시도 질의에서 상한으로 거른다.
    attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))

    # 인증키가 섞이지 않도록 걸러서 넣는다. 수집기의 _raise_for_status가 요청 URL을 이미 걷어낸다.
    last_error: Mapped[str | None] = mapped_column(String(LAST_ERROR_MAX), nullable=True)

    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
