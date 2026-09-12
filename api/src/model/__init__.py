"""SQLAlchemy 모델 패키지.

- raw: scripts/docs/data-api의 9개 오픈API 응답 원본을 그대로 저장하는 테이블
- prop_transaction: raw 8종을 가공해 채우는 매매/전월세 정제 테이블
- load_progress: 백필 스크립트가 적재를 마친 원본 파일 기록
"""

from .base import Base, RawRecordMixin
from .load_progress import RawLoadProgress
from .prop_transaction import (
    PropertyType,
    RentTransaction,
    SaleTransaction,
    TransactionMixin,
)
from .raw import (
    RawApartRent,
    RawApartSale,
    RawLegalDongCode,
    RawMultiflexRent,
    RawMultiflexSale,
    RawOfficetelRent,
    RawOfficetelSale,
    RawSingleMultiFamilyRent,
    RawSingleMultiFamilySale,
)

__all__ = [
    "Base",
    "PropertyType",
    "RawApartRent",
    "RawApartSale",
    "RawLegalDongCode",
    "RawLoadProgress",
    "RawMultiflexRent",
    "RawMultiflexSale",
    "RawOfficetelRent",
    "RawOfficetelSale",
    "RawRecordMixin",
    "RawSingleMultiFamilyRent",
    "RawSingleMultiFamilySale",
    "RentTransaction",
    "SaleTransaction",
    "TransactionMixin",
]
