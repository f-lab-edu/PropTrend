"""가공을 마친 row를 테이블에 적재하는 적재기."""

import logging
from typing import Any, ClassVar

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..model import (
    LEGAL_DONG_CODE_KNOWN_FIELDS,
    RTMS_KNOWN_FIELDS,
    Base,
    LegalDongCodeRawItem,
    RentTransaction,
    RTMSRawItem,
    SaleTransaction,
)

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1000


def _warn_schema_drift(api_id: str, items: list[dict[str, Any]], known: frozenset[str]) -> None:
    """payload 키를 기준선과 대조해 경고만 남긴다. 적재 자체는 어떤 키가 와도 막지 않는다."""
    seen = {key for item in items for key in item}
    if unknown := seen - known:
        logger.warning("%s: 기준선에 없는 필드가 왔다 %s", api_id, sorted(unknown))
    if missing := known - seen:
        # 응답은 값이 비어도 태그는 보내므로, 단위 전체에서 한 번도 안 보이면 개명·삭제다.
        logger.warning("%s: 기준선에 있던 필드가 응답에 없다 %s", api_id, sorted(missing))


async def _insert_chunked(session: AsyncSession, table: Any, rows: list[dict[str, Any]]) -> int:
    """모델이 아니라 Table을 넘긴다. 모델을 넘기면 ORM 경로를 타서 느리다."""
    for start in range(0, len(rows), CHUNK_SIZE):
        await session.execute(insert(table), rows[start : start + CHUNK_SIZE])
    return len(rows)


class RTMSRawItemLoader:
    """실거래가 오픈API 응답 item을 rtms_raw_items에 그대로 적재한다."""

    def __init__(self, session: AsyncSession, api_id: str) -> None:
        # 오타 난 api_id는 갱신 단위 삭제가 영영 못 찾는 행을 만들어 매일 중복을 쌓는다.
        # RTMS_KNOWN_FIELDS의 키가 곧 허용된 슬러그 8종이라 그대로 검사에 쓴다.
        if api_id not in RTMS_KNOWN_FIELDS:
            raise ValueError(f"모르는 api_id다: {api_id!r}")
        self.session = session
        self.api_id = api_id

    async def load(self, lawd_cd: str, deal_ymd: str, items: list[dict[str, Any]]) -> int:
        """응답 item을 한 건도 거르지 않고 넣고, 실제로 넣은 수를 반환한다."""
        # 빈 목록에 드리프트 검사를 돌리면 기준선 전 필드가 사라진 것으로 보여 오경보가 난다.
        if items:
            _warn_schema_drift(self.api_id, items, RTMS_KNOWN_FIELDS[self.api_id])

            # 갱신 단위 키는 응답이 아니라 요청 파라미터에서 온다. collected_at은 server_default에
            # 맡기려고 키 자체를 넣지 않는다(None을 넣으면 기본값을 덮어 NOT NULL 위반이 난다).
            rows = [
                {"api_id": self.api_id, "lawd_cd": lawd_cd, "deal_ymd": deal_ymd, "payload": item} for item in items
            ]
            loaded = await _insert_chunked(self.session, RTMSRawItem.__table__, rows)
        else:
            loaded = 0

        logger.debug("부동산 실거래 bronze 데이터 적재 완료", extra={"stage": "load_bronze", "loaded": loaded})
        return loaded


class LegalDongCodeRawItemLoader:
    """법정동코드 오픈API 응답 row를 legal_dong_code_raw_items에 그대로 적재한다."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def load(self, items: list[dict[str, Any]]) -> int:
        if items:
            _warn_schema_drift("legal_dong_code", items, LEGAL_DONG_CODE_KNOWN_FIELDS)

            rows = [{"payload": item} for item in items]
            loaded = await _insert_chunked(self.session, LegalDongCodeRawItem.__table__, rows)
        else:
            loaded = 0

        logger.debug("법정동코드 bronze 적재 완료", extra={"stage": "load_legal_dong", "loaded": loaded})
        return loaded


class TransactionLoader:
    """전처리기가 넘긴 row를 정제 테이블에 적재한다."""

    model: ClassVar[type[Base]]

    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.columns = frozenset(
            column.name for column in self.model.__table__.columns if column.name not in {"id", "created_at"}
        )

    async def load(self, rows: list[dict[str, Any]]) -> int:
        if rows:
            # 전처리기가 리터럴 dict 하나로 모든 행을 만들므로 첫 행만 검사하면 충분하다.
            keys = rows[0].keys()
            if keys != self.columns:
                raise ValueError(
                    f"적재 row의 키가 {self.model.__tablename__} 컬럼과 다르다: "
                    f"모르는 키={sorted(keys - self.columns)}, "
                    f"빠진 키={sorted(self.columns - keys)}"
                )

            loaded = await _insert_chunked(self.session, self.model.__table__, rows)
        else:
            loaded = 0

        logger.debug(
            "부동산 실거래 silver 데이터 적재 완료",
            extra={"stage": "load_silver", "table": self.model.__tablename__, "loaded": loaded},
        )
        return loaded


class SaleTransactionLoader(TransactionLoader):
    model = SaleTransaction


class RentTransactionLoader(TransactionLoader):
    model = RentTransaction
