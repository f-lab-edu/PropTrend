"""SQLAlchemy 모델 패키지.

- raw: scripts/docs/data-api의 9개 오픈API 응답 item을 그대로 보관하는 bronze 테이블
- prop_transaction: rtms_raw_items를 가공해 채우는 매매/전월세 정제 테이블
- load_progress: 백필 스크립트가 적재를 마친 원본 파일 기록
- refresh_unit_state: 갱신 파이프라인이 끝내지 못한 단위 목록
- user: 서비스 사용자 계정과 로그인 세션
- complex: 실거래 갱신과 무관하게 유지되는 아파트·오피스텔 단지
- favorite: 사용자가 즐겨찾기한 단지
"""

from .base import Base
from .complex import COMPLEX_INDEX_WHERE, COMPLEX_KEY_COLUMNS, COMPLEX_PROPERTY_TYPES, Complex
from .favorite import FavoriteComplex
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
from .refresh_unit_state import LAST_ERROR_MAX, RefreshUnitState, UnitStatus
from .user import User, UserSession

__all__ = [
    "COMPLEX_INDEX_WHERE",
    "COMPLEX_KEY_COLUMNS",
    "COMPLEX_PROPERTY_TYPES",
    "LAST_ERROR_MAX",
    "LEGAL_DONG_CODE_KNOWN_FIELDS",
    "RTMS_KNOWN_FIELDS",
    "Base",
    "Complex",
    "FavoriteComplex",
    "LegalDongCodeRawItem",
    "PropertyType",
    "RTMSRawItem",
    "RawLoadProgress",
    "RefreshUnitState",
    "RentTransaction",
    "SaleTransaction",
    "TransactionMixin",
    "UnitStatus",
    "User",
    "UserSession",
]
