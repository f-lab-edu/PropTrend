"""scripts/docs/data-api의 9개 오픈API 응답 item을 스키마 종속 없이 보관하는 bronze 모델.

docs:
- scripts/docs/data-api/apart-sale.md (getRTMSDataSvcAptTrade)
- scripts/docs/data-api/apart-rent.md (getRTMSDataSvcAptRent)
- scripts/docs/data-api/officetel-sale.md (getRTMSDataSvcOffiTrade)
- scripts/docs/data-api/officetel-rent.md (getRTMSDataSvcOffiRent)
- scripts/docs/data-api/multiflex-sale.md (getRTMSDataSvcRHTrade)
- scripts/docs/data-api/multiflex-rent.md (getRTMSDataSvcRHRent)
- scripts/docs/data-api/single-multi-family-sale.md (getRTMSDataSvcSHTrade)
- scripts/docs/data-api/single-multi-family-rent.md (getRTMSDataSvcSHRent)
- scripts/docs/data-api/legal-dong-code.md (getStanReginCdList)

값 검증과 타입 정규화는 이 표들을 읽어가는 silver(prop_transaction) 계층의 책임이다.
갱신 방식이 달라 표를 나눈다. 실거래가 8종은 (계약년월, 시군구) 구간을 갈아끼우고,
법정동코드는 표를 통째로 비우고 다시 채운다.
"""

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, Index, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class RTMSRawItem(Base):
    """실거래가 오픈API 8종(RTMSDataSvc*)의 응답 item 1건을 그대로 보관한다."""

    __tablename__ = "rtms_raw_items"

    # 갱신 단위 조회·삭제 전용. 항상 주어지는 api_id·deal_ymd가 앞에 와야 lawd_cd를 생략하는
    # 전국 조회도 인덱스를 탄다.
    __table_args__ = (Index("ix_rtms_raw_items_refresh_unit", "api_id", "deal_ymd", "lawd_cd"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # 오퍼레이션 이름이 아닌 짧은 슬러그. raw_load_progress.api_id와 같은 어휘다.
    api_id: Mapped[str] = mapped_column(String(40))

    # 갱신 단위 키는 응답 필드(sggCd/dealYear/dealMonth)가 아니라 요청 파라미터에서 가져온다.
    # 응답 필드로 잡으면 필드가 개명될 때 구간 삭제가 옛 형식 행만 지워 중복이 쌓인다.
    lawd_cd: Mapped[str] = mapped_column(String(5))
    deal_ymd: Mapped[str] = mapped_column(String(6))  # 파라미터 원형("202307") 그대로.

    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # 무손실은 아니다. XML을 item 단위 dict로 바꾸는 시점에 태그 순서·중복 태그가 사라진다.
    # 내부 값으로 조회할 일이 생기면 그때 생성 컬럼(Computed)을 붙인다.
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)


class LegalDongCodeRawItem(Base):
    """법정동코드 오픈API(getStanReginCdList)의 응답 row 1건을 그대로 보관한다."""

    # 갱신 단위 키가 없는 API이고 250건 규모라 인덱스를 두지 않는다.
    __tablename__ = "legal_dong_code_raw_items"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)


# 스키마 드리프트 감지의 기준선. 적재를 막지는 않고, 처음 보는 키나 사라진 키를 경고로 남긴다.
# 이 API는 실제로 전 필드를 한 번 개명한 이력이 있다(apart-sale.md의 신구대조표).
RTMS_KNOWN_FIELDS: dict[str, frozenset[str]] = {
    "apart_sale": frozenset(
        {
            "sggCd",
            "dealYear",
            "dealMonth",
            "dealDay",
            "umdNm",
            "aptNm",
            "jibun",
            "excluUseAr",
            "dealAmount",
            "floor",
            "buildYear",
            "cdealType",
            "cdealDay",
            "dealingGbn",
            "estateAgentSggNm",
            "rgstDate",
            "aptDong",
            "slerGbn",
            "buyerGbn",
            "landLeaseholdGbn",
        }
    ),
    "apart_rent": frozenset(
        {
            "sggCd",
            "dealYear",
            "dealMonth",
            "dealDay",
            "umdNm",
            "aptNm",
            "jibun",
            "excluUseAr",
            "deposit",
            "monthlyRent",
            "floor",
            "buildYear",
            "contractTerm",
            "contractType",
            "useRRRight",
            "preDeposit",
            "preMonthlyRent",
            "roadnm",
            "roadnmsggcd",
            "roadnmcd",
            "roadnmseq",
            "roadnmbcd",
            "roadnmbonbun",
            "roadnmbubun",
            "aptSeq",
        }
    ),
    "officetel_sale": frozenset(
        {
            "sggCd",
            "dealYear",
            "dealMonth",
            "dealDay",
            "sggNm",
            "umdNm",
            "jibun",
            "offiNm",
            "excluUseAr",
            "dealAmount",
            "floor",
            "buildYear",
            "cdealType",
            "cdealDay",
            "dealingGbn",
            "estateAgentSggNm",
            "slerGbn",
            "buyerGbn",
        }
    ),
    "officetel_rent": frozenset(
        {
            "sggCd",
            "dealYear",
            "dealMonth",
            "dealDay",
            "sggNm",
            "umdNm",
            "jibun",
            "offiNm",
            "excluUseAr",
            "deposit",
            "monthlyRent",
            "floor",
            "buildYear",
            "contractTerm",
            "contractType",
            "useRRRight",
            "preDeposit",
            "preMonthlyRent",
        }
    ),
    "multiflex_sale": frozenset(
        {
            "sggCd",
            "dealYear",
            "dealMonth",
            "dealDay",
            "umdNm",
            "mhouseNm",
            "jibun",
            "buildYear",
            "excluUseAr",
            "landAr",
            "dealAmount",
            "floor",
            "cdealType",
            "cdealDay",
            "dealingGbn",
            "estateAgentSggNm",
            "rgstDate",
            "slerGbn",
            "buyerGbn",
            "houseType",
        }
    ),
    "multiflex_rent": frozenset(
        {
            "sggCd",
            "dealYear",
            "dealMonth",
            "dealDay",
            "umdNm",
            "houseType",
            "mhouseNm",
            "jibun",
            "buildYear",
            "excluUseAr",
            "deposit",
            "monthlyRent",
            "floor",
            "contractTerm",
            "contractType",
            "useRRRight",
            "preDeposit",
            "preMonthlyRent",
        }
    ),
    "single_multi_family_sale": frozenset(
        {
            "sggCd",
            "dealYear",
            "dealMonth",
            "dealDay",
            "umdNm",
            "houseType",
            "jibun",
            "totalFloorAr",
            "plottageAr",
            "dealAmount",
            "buildYear",
            "cdealType",
            "cdealDay",
            "dealingGbn",
            "estateAgentSggNm",
            "slerGbn",
            "buyerGbn",
        }
    ),
    "single_multi_family_rent": frozenset(
        {
            "sggCd",
            "dealYear",
            "dealMonth",
            "dealDay",
            "houseType",
            "umdNm",
            "totalFloorAr",
            "deposit",
            "monthlyRent",
            "buildYear",
            "contractTerm",
            "contractType",
            "useRRRight",
            "preDeposit",
            "preMonthlyRent",
        }
    ),
}


# 법정동코드 API의 알려진 필드.
LEGAL_DONG_CODE_KNOWN_FIELDS: frozenset[str] = frozenset(
    {
        "region_cd",
        "sido_cd",
        "sgg_cd",
        "umd_cd",
        "ri_cd",
        "locatjumin_cd",
        "locatjijuk_cd",
        "locatadd_nm",
        "locat_order",
        "locat_rm",
        "locathigh_cd",
        "locallow_nm",
        "adpt_de",
    }
)
