"""재적재에 앞서 갱신 단위의 기존 데이터를 지우는 정리기."""

import logging
from typing import ClassVar

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from ..model import (
    Base,
    LegalDongCodeRawItem,
    PropertyType,
    RentTransaction,
    RTMSRawItem,
    SaleTransaction,
)
from .utils import month_range, parse_deal_ymd, split_sgg_cd

logger = logging.getLogger(__name__)


class TransactionCleaner:
    """정제 테이블에서 갱신 단위 1건을 지운다."""

    model: ClassVar[type[Base]]

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def clean(self, property_type: PropertyType, deal_ymd: str, sgg_cd: str | None = None) -> int:
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
        logger.debug(
            "부동산 실거래 silver 데이터 단위 정리 완료",
            extra={"stage": "clean_silver", "table": self.model.__tablename__, "deleted": result.rowcount},
        )
        return result.rowcount


class SaleTransactionCleaner(TransactionCleaner):
    model = SaleTransaction


class RentTransactionCleaner(TransactionCleaner):
    model = RentTransaction


class RTMSRawItemCleaner:
    """rtms_raw_items에서 갱신 단위(api_id, 계약년월, 시군구) 하나를 지운다.

    유형을 테이블이 아니라 api_id가 나타내므로 정제 테이블 정리기와 시그니처가 다르다.
    """

    def __init__(self, session: AsyncSession, api_id: str) -> None:
        self.session = session
        self.api_id = api_id

    async def clean(self, deal_ymd: str, lawd_cd: str) -> int:
        """전국 삭제는 실수로 한 달치를 날릴 여지만 남기므로 열어두지 않는다."""
        # 적재기가 넣은 요청 파라미터 원형("202307")과 같은 모양으로 비교한다. 두 값이
        # 어긋나면 삭제가 빈손으로 끝나고 재적재분이 그대로 중복된다.
        parse_deal_ymd(deal_ymd)
        split_sgg_cd(lawd_cd)  # 형식 검증. 5자리를 쪼개지 않고 그대로 쓴다.
        # api_id는 적재기가 화이트리스트로 막으므로 없는 값이 표에 들어와 있을 수 없다.

        table = RTMSRawItem.__table__
        result = await self.session.execute(
            delete(table).where(
                table.c.api_id == self.api_id,
                table.c.deal_ymd == deal_ymd,
                table.c.lawd_cd == lawd_cd,
            )
        )
        logger.debug(
            "부동산 실거래 bronze 데이터 단위 정리 완료", extra={"stage": "clean_bronze", "deleted": result.rowcount}
        )
        return result.rowcount


class LegalDongCodeRawItemCleaner:
    """legal_dong_code_raw_items를 통째로 비운다."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def clean(self) -> int:
        # 갱신 단위 키가 없는 API라 전량 교체뿐이다. 비운 직후 같은 트랜잭션에서 반드시
        # 다시 채워야 한다. 시군구 목록의 출처라 비어 있으면 이후 갱신이 통째로 멈춘다.
        result = await self.session.execute(delete(LegalDongCodeRawItem.__table__))
        logger.debug(
            "법정동코드 bronze 데이터 정리 완료", extra={"stage": "clean_legal_dong", "deleted": result.rowcount}
        )
        return result.rowcount
