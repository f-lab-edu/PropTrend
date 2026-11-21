"""정기 갱신이 미리 계산해 두는 시장 지표 결과 테이블 모델.

요청마다 전국 거래를 집계하면 수 초가 걸리는데, 결과는 하루 한 번 정기 갱신 뒤로는 바뀌지 않는다.
정기 갱신 DAG(`airflow/dags/rtms`)가 계산해 통째로 갈아끼우고 API는 읽기만 한다.
"""

from datetime import date, datetime

from sqlalchemy import CHAR, BigInteger, Date, DateTime, Numeric, SmallInteger, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class MarketSnapshot(Base):
    """시장 지표 결과 테이블 하나를 마지막으로 계산한 기준일."""

    __tablename__ = "market_snapshots"

    # 결과 테이블 이름. 결과가 0건인 날에도 기준일을 응답하려고 결과 행과 따로 둔다.
    name: Mapped[str] = mapped_column(String(50), primary_key=True)
    base_date: Mapped[date] = mapped_column(Date)
    refreshed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MarketPriceMover(Base):
    """직전 거래 대비 최근 거래 금액 변동률이 큰 아파트 단지(전용면적별) 하나.

    실거래 테이블은 갱신 단위마다 지우고 다시 넣어 거래 id가 매일 바뀐다. 다음 계산 전까지 거래 행을 다시
    찾지 않아도 응답할 수 있도록 응답에 필요한 값을 계산 시점에 옮겨 둔다.
    """

    __tablename__ = "market_price_movers"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    # 부호가 급등(양수)·급락(음수)을 가른다.
    change_rate: Mapped[float] = mapped_column(Numeric(12, 2))

    # 최근 거래. 컬럼 이름은 sale_transactions와 같다.
    latest_id: Mapped[int] = mapped_column(BigInteger)
    sido_code: Mapped[str] = mapped_column(CHAR(2))
    sigungu_code: Mapped[str] = mapped_column(CHAR(3))
    umd_name: Mapped[str | None] = mapped_column(String(60))
    jibun: Mapped[str | None] = mapped_column(String(20))
    building_name: Mapped[str | None] = mapped_column(String(100))
    apartment_dong: Mapped[str | None] = mapped_column(String(50))
    floor: Mapped[int | None] = mapped_column(SmallInteger)
    build_year: Mapped[int | None] = mapped_column(SmallInteger)
    exclusive_use_area: Mapped[float] = mapped_column(Numeric(10, 4))
    deal_date: Mapped[date] = mapped_column(Date)
    deal_amount: Mapped[int] = mapped_column(BigInteger)
    dealing_type: Mapped[str | None] = mapped_column(String(20))
    cancel_deal_date: Mapped[date | None] = mapped_column(Date)
    # 단지 id는 갱신으로 바뀌지 않으므로 계산할 때 찾아 둔다. 단지가 없으면 NULL.
    complex_id: Mapped[int | None] = mapped_column(BigInteger)

    # 바로 앞 거래.
    previous_id: Mapped[int] = mapped_column(BigInteger)
    previous_deal_date: Mapped[date] = mapped_column(Date)
    previous_deal_amount: Mapped[int] = mapped_column(BigInteger)
    previous_floor: Mapped[int | None] = mapped_column(SmallInteger)
