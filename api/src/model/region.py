"""시도·시군구 필터 드롭다운에 쓰는 지역 모델.

legal_dong_code_raw_items(bronze)를 가공해 법정동코드 갱신마다 통째로 갈아끼운다.
"""

from sqlalchemy import CHAR, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Region(Base):
    """시도·시군구 한 쌍. 시군구 코드는 시도 안에서만 고유해 복합 기본키로 둔다."""

    __tablename__ = "regions"

    sido_code: Mapped[str] = mapped_column(CHAR(2), primary_key=True)
    sigungu_code: Mapped[str] = mapped_column(CHAR(3), primary_key=True)
    sido_name: Mapped[str] = mapped_column(String(20))
    # 일반구는 "수원시 장안구"처럼 시 이름까지 담는다. 시군구가 없는 세종은 시도명을 그대로 쓴다.
    sigungu_name: Mapped[str] = mapped_column(String(50))
