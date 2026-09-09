from datetime import datetime

from sqlalchemy import BigInteger, DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class RawRecordMixin:
    """공공 오픈API 원본 응답(item)을 그대로 저장하는 테이블의 공통 컬럼.

    원본 API 문서 자체에 오탈자·형식 오류가 다수 확인되는 만큼, 이 계층에서는
    수신한 값을 그대로 적재하는 것을 목표로 하며 모든 데이터 컬럼을 nullable로
    둔다. 값 검증/가공은 이 raw 테이블을 읽어가는 후속 단계의 책임이다.
    """

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
