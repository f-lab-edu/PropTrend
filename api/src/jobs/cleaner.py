"""재적재에 앞서 갱신 단위의 기존 데이터를 지우는 정리기."""

from typing import ClassVar

from sqlalchemy import delete, func
from sqlalchemy.ext.asyncio import AsyncSession

from ..model import Base, PropertyType, RentTransaction, SaleTransaction
from .utils import month_range, parse_deal_ymd, split_sgg_cd


class TransactionCleaner:
    """정제 테이블에서 갱신 단위 1건을 지운다."""

    model: ClassVar[type[Base]]

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def clean(
        self, property_type: PropertyType, deal_ymd: str, sgg_cd: str | None = None
    ) -> int:
        """`sgg_cd`를 생략하면 해당 월 전국이 지워지므로 백필 외에는 쓰지 않는다."""
        start, end = month_range(deal_ymd)
        table = self.model.__table__

        conditions = [
            table.c.property_type == property_type,
            table.c.deal_date >= start,
            table.c.deal_date < end,
        ]
        if sgg_cd is not None:
            sido_code, sigungu_code = split_sgg_cd(sgg_cd)
            conditions.append(table.c.sido_code == sido_code)
            conditions.append(table.c.sigungu_code == sigungu_code)

        result = await self.session.execute(delete(table).where(*conditions))
        return result.rowcount


class SaleTransactionCleaner(TransactionCleaner):
    model = SaleTransaction


class RentTransactionCleaner(TransactionCleaner):
    model = RentTransaction


class RawTableCleaner:
    """raw 테이블에서 (계약년월, 시군구) 구간을 지운다.

    raw에는 property_type 컬럼이 없고 테이블 자체가 유형을 나타내서, 정제 테이블
    정리기와 시그니처가 다르다.
    """

    def __init__(self, session: AsyncSession, model: type[Base]) -> None:
        self.session = session
        self.model = model

    async def clean(self, deal_ymd: str, sgg_cd: str) -> int:
        """전국 삭제는 실수로 한 달치를 날릴 여지만 남기므로 열어두지 않는다."""
        year, month = parse_deal_ymd(deal_ymd)
        split_sgg_cd(sgg_cd)  # 형식 검증. raw는 sggCd를 5자리 그대로 쓴다.
        table = self.model.__table__

        result = await self.session.execute(
            delete(table).where(
                table.c.dealYear == year,
                func.lpad(table.c.dealMonth, 2, "0") == month,
                table.c.sggCd == sgg_cd,
            )
        )
        return result.rowcount
