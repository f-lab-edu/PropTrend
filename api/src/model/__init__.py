"""scripts/docs/data-api의 9개 오픈API 응답 원본을 저장하는 SQLAlchemy 모델."""

from .base import Base, RawRecordMixin
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
    "RawRecordMixin",
    "RawApartSale",
    "RawApartRent",
    "RawOfficetelSale",
    "RawOfficetelRent",
    "RawMultiflexSale",
    "RawMultiflexRent",
    "RawSingleMultiFamilySale",
    "RawSingleMultiFamilyRent",
    "RawLegalDongCode",
]
