from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class FavoriteComplex(Base):
    """사용자가 즐겨찾기한 단지."""

    __tablename__ = "favorite_complexes"

    # 복합 기본키가 같은 단지를 두 번 즐겨찾기하지 못하게 막는다.
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    # 단지에 새 실거래가 생겼을 때 즐겨찾기한 사용자를 찾는 역조회용 인덱스.
    complex_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("complexes.id", ondelete="CASCADE"), primary_key=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
