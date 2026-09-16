"""SQLAlchemy 모델 패키지.

- raw: scripts/docs/data-api의 9개 오픈API 응답 item을 그대로 보관하는 bronze 테이블
- prop_transaction: rtms_raw_items를 가공해 채우는 매매/전월세 정제 테이블
- load_progress: 백필 스크립트가 적재를 마친 원본 파일 기록
"""

from .base import Base
from .load_progress import RawLoadProgress
from .prop_transaction import (
    PropertyType,
    RentTransaction,
    SaleTransaction,
    TransactionMixin,
)
from .raw import (
    LEGAL_DONG_CODE_KNOWN_FIELDS,
    RTMS_KNOWN_FIELDS,
    LegalDongCodeRawItem,
    RTMSRawItem,
)

__all__ = [
    "LEGAL_DONG_CODE_KNOWN_FIELDS",
    "RTMS_KNOWN_FIELDS",
    "Base",
    "LegalDongCodeRawItem",
    "PropertyType",
    "RTMSRawItem",
    "RawLoadProgress",
    "RentTransaction",
    "SaleTransaction",
    "TransactionMixin",
]
