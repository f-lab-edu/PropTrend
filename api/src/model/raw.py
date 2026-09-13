"""scripts/docs/data-api의 9개 오픈API 응답 원본(item)을 저장하는 모델.

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
"""

from typing import Any

from sqlalchemy import Index, String
from sqlalchemy.orm import Mapped, declared_attr, mapped_column

from .base import Base, RawRecordMixin


class RtmsRecordMixin(RawRecordMixin):
    """실거래가 오픈API 8종(RTMSDataSvc*)이 공유하는 갱신 단위 키와 인덱스.

    8종 모두 (계약년월, 시군구) 단위로 "구간 삭제 → 재적재" 순으로 갱신하므로, 그 키가
    되는 네 컬럼과 인덱스는 테이블마다 정의가 같다. 나머지 컬럼은 API마다 다르므로 각
    모델에 그대로 둔다. 법정동코드는 이 키가 없어 `RawRecordMixin`만 쓴다.
    """

    @declared_attr.directive
    def __table_args__(cls) -> tuple[Any, ...]:
        # 갱신 단위 조회·삭제 전용. 세 컬럼 모두 등치 비교라 단위 조회에서는 순서가
        # 무관하지만, dealMonth를 마지막이 아니라 sggCd 앞에 둬야 sggCd를 생략하는
        # 전국 조회가 skip scan으로 밀리지 않는다(docs/temp/refresh-unit-index.md).
        return (Index(f"ix_{cls.__tablename__}_refresh_unit", "dealYear", "dealMonth", "sggCd"),)

    # 갱신 단위를 가리키는 키. sort_order로 각 API의 나머지 필드보다 앞에 모아 둔다.
    sggCd: Mapped[str | None] = mapped_column(String(5), sort_order=-1)
    dealYear: Mapped[str | None] = mapped_column(String(4), sort_order=-1)
    dealMonth: Mapped[str | None] = mapped_column(String(2), sort_order=-1)
    dealDay: Mapped[str | None] = mapped_column(String(2), sort_order=-1)


class RawApartSale(Base, RtmsRecordMixin):
    __tablename__ = "raw_apart_sale"

    umdNm: Mapped[str | None] = mapped_column(String(60))
    aptNm: Mapped[str | None] = mapped_column(String(100))
    jibun: Mapped[str | None] = mapped_column(String(20))
    excluUseAr: Mapped[str | None] = mapped_column(String(22))
    dealAmount: Mapped[str | None] = mapped_column(String(40))
    floor: Mapped[str | None] = mapped_column(String(10))
    buildYear: Mapped[str | None] = mapped_column(String(4))
    cdealType: Mapped[str | None] = mapped_column(String(1))
    cdealDay: Mapped[str | None] = mapped_column(String(8))
    dealingGbn: Mapped[str | None] = mapped_column(String(10))
    estateAgentSggNm: Mapped[str | None] = mapped_column(String(3000))
    rgstDate: Mapped[str | None] = mapped_column(String(8))
    aptDong: Mapped[str | None] = mapped_column(String(400))
    slerGbn: Mapped[str | None] = mapped_column(String(100))
    buyerGbn: Mapped[str | None] = mapped_column(String(100))
    landLeaseholdGbn: Mapped[str | None] = mapped_column(String(1))


class RawApartRent(Base, RtmsRecordMixin):
    __tablename__ = "raw_apart_rent"

    umdNm: Mapped[str | None] = mapped_column(String(30))
    aptNm: Mapped[str | None] = mapped_column(String(100))
    jibun: Mapped[str | None] = mapped_column(String(20))
    excluUseAr: Mapped[str | None] = mapped_column(String(22))
    deposit: Mapped[str | None] = mapped_column(String(40))
    monthlyRent: Mapped[str | None] = mapped_column(String(40))
    floor: Mapped[str | None] = mapped_column(String(10))
    buildYear: Mapped[str | None] = mapped_column(String(4))
    contractTerm: Mapped[str | None] = mapped_column(String(12))
    contractType: Mapped[str | None] = mapped_column(String(4))
    useRRRight: Mapped[str | None] = mapped_column(String(4))
    preDeposit: Mapped[str | None] = mapped_column(String(40))
    preMonthlyRent: Mapped[str | None] = mapped_column(String(40))
    roadnm: Mapped[str | None] = mapped_column(String(100))
    roadnmsggcd: Mapped[str | None] = mapped_column(String(5))
    roadnmcd: Mapped[str | None] = mapped_column(String(7))
    roadnmseq: Mapped[str | None] = mapped_column(String(2))
    roadnmbcd: Mapped[str | None] = mapped_column(String(1))
    roadnmbonbun: Mapped[str | None] = mapped_column(String(5))
    roadnmbubun: Mapped[str | None] = mapped_column(String(5))
    aptSeq: Mapped[str | None] = mapped_column(String(20))


class RawOfficetelSale(Base, RtmsRecordMixin):
    __tablename__ = "raw_officetel_sale"

    sggNm: Mapped[str | None] = mapped_column(String(30))
    umdNm: Mapped[str | None] = mapped_column(String(60))
    jibun: Mapped[str | None] = mapped_column(String(20))
    offiNm: Mapped[str | None] = mapped_column(String(100))
    excluUseAr: Mapped[str | None] = mapped_column(String(22))
    dealAmount: Mapped[str | None] = mapped_column(String(40))
    floor: Mapped[str | None] = mapped_column(String(10))
    buildYear: Mapped[str | None] = mapped_column(String(4))
    cdealType: Mapped[str | None] = mapped_column(String(1))
    cdealDay: Mapped[str | None] = mapped_column(String(8))
    dealingGbn: Mapped[str | None] = mapped_column(String(10))
    estateAgentSggNm: Mapped[str | None] = mapped_column(String(3000))
    slerGbn: Mapped[str | None] = mapped_column(String(100))
    buyerGbn: Mapped[str | None] = mapped_column(String(100))


class RawOfficetelRent(Base, RtmsRecordMixin):
    __tablename__ = "raw_officetel_rent"

    sggNm: Mapped[str | None] = mapped_column(String(30))
    umdNm: Mapped[str | None] = mapped_column(String(60))
    jibun: Mapped[str | None] = mapped_column(String(20))
    offiNm: Mapped[str | None] = mapped_column(String(100))
    excluUseAr: Mapped[str | None] = mapped_column(String(22))
    deposit: Mapped[str | None] = mapped_column(String(40))
    monthlyRent: Mapped[str | None] = mapped_column(String(40))
    floor: Mapped[str | None] = mapped_column(String(10))
    buildYear: Mapped[str | None] = mapped_column(String(4))
    contractTerm: Mapped[str | None] = mapped_column(String(12))
    contractType: Mapped[str | None] = mapped_column(String(4))
    useRRRight: Mapped[str | None] = mapped_column(String(4))
    preDeposit: Mapped[str | None] = mapped_column(String(40))
    preMonthlyRent: Mapped[str | None] = mapped_column(String(40))


class RawMultiflexSale(Base, RtmsRecordMixin):
    __tablename__ = "raw_multiflex_sale"

    umdNm: Mapped[str | None] = mapped_column(String(60))
    mhouseNm: Mapped[str | None] = mapped_column(String(100))
    jibun: Mapped[str | None] = mapped_column(String(20))
    buildYear: Mapped[str | None] = mapped_column(String(4))
    excluUseAr: Mapped[str | None] = mapped_column(String(22))
    landAr: Mapped[str | None] = mapped_column(String(22))
    dealAmount: Mapped[str | None] = mapped_column(String(40))
    floor: Mapped[str | None] = mapped_column(String(10))
    cdealType: Mapped[str | None] = mapped_column(String(1))
    cdealDay: Mapped[str | None] = mapped_column(String(8))
    dealingGbn: Mapped[str | None] = mapped_column(String(10))
    estateAgentSggNm: Mapped[str | None] = mapped_column(String(3000))
    rgstDate: Mapped[str | None] = mapped_column(String(8))
    slerGbn: Mapped[str | None] = mapped_column(String(100))
    buyerGbn: Mapped[str | None] = mapped_column(String(100))
    houseType: Mapped[str | None] = mapped_column(String(10))


class RawMultiflexRent(Base, RtmsRecordMixin):
    __tablename__ = "raw_multiflex_rent"

    umdNm: Mapped[str | None] = mapped_column(String(30))
    houseType: Mapped[str | None] = mapped_column(String(6))
    mhouseNm: Mapped[str | None] = mapped_column(String(100))
    jibun: Mapped[str | None] = mapped_column(String(20))
    buildYear: Mapped[str | None] = mapped_column(String(4))
    excluUseAr: Mapped[str | None] = mapped_column(String(22))
    deposit: Mapped[str | None] = mapped_column(String(40))
    monthlyRent: Mapped[str | None] = mapped_column(String(40))
    floor: Mapped[str | None] = mapped_column(String(10))
    contractTerm: Mapped[str | None] = mapped_column(String(12))
    contractType: Mapped[str | None] = mapped_column(String(4))
    useRRRight: Mapped[str | None] = mapped_column(String(4))
    preDeposit: Mapped[str | None] = mapped_column(String(40))
    preMonthlyRent: Mapped[str | None] = mapped_column(String(40))


class RawSingleMultiFamilySale(Base, RtmsRecordMixin):
    __tablename__ = "raw_single_multi_family_sale"

    umdNm: Mapped[str | None] = mapped_column(String(60))
    houseType: Mapped[str | None] = mapped_column(String(6))
    jibun: Mapped[str | None] = mapped_column(String(20))
    totalFloorAr: Mapped[str | None] = mapped_column(String(22))
    plottageAr: Mapped[str | None] = mapped_column(String(22))
    dealAmount: Mapped[str | None] = mapped_column(String(40))
    buildYear: Mapped[str | None] = mapped_column(String(40))
    cdealType: Mapped[str | None] = mapped_column(String(1))
    cdealDay: Mapped[str | None] = mapped_column(String(8))
    dealingGbn: Mapped[str | None] = mapped_column(String(10))
    estateAgentSggNm: Mapped[str | None] = mapped_column(String(3000))
    slerGbn: Mapped[str | None] = mapped_column(String(100))
    buyerGbn: Mapped[str | None] = mapped_column(String(100))


class RawSingleMultiFamilyRent(Base, RtmsRecordMixin):
    __tablename__ = "raw_single_multi_family_rent"

    houseType: Mapped[str | None] = mapped_column(String(6))
    umdNm: Mapped[str | None] = mapped_column(String(30))
    totalFloorAr: Mapped[str | None] = mapped_column(String(22))
    deposit: Mapped[str | None] = mapped_column(String(40))
    monthlyRent: Mapped[str | None] = mapped_column(String(40))
    buildYear: Mapped[str | None] = mapped_column(String(4))
    contractTerm: Mapped[str | None] = mapped_column(String(12))
    contractType: Mapped[str | None] = mapped_column(String(4))
    useRRRight: Mapped[str | None] = mapped_column(String(4))
    preDeposit: Mapped[str | None] = mapped_column(String(40))
    preMonthlyRent: Mapped[str | None] = mapped_column(String(40))


class RawLegalDongCode(Base, RawRecordMixin):
    __tablename__ = "raw_legal_dong_code"

    region_cd: Mapped[str | None] = mapped_column(String(10))
    sido_cd: Mapped[str | None] = mapped_column(String(2))
    sgg_cd: Mapped[str | None] = mapped_column(String(3))
    umd_cd: Mapped[str | None] = mapped_column(String(3))
    ri_cd: Mapped[str | None] = mapped_column(String(2))
    locatjumin_cd: Mapped[str | None] = mapped_column(String(10))
    locatjijuk_cd: Mapped[str | None] = mapped_column(String(10))
    locatadd_nm: Mapped[str | None] = mapped_column(String(50))
    locat_order: Mapped[str | None] = mapped_column(String(3))
    locat_rm: Mapped[str | None] = mapped_column(String(200))
    locathigh_cd: Mapped[str | None] = mapped_column(String(10))
    locallow_nm: Mapped[str | None] = mapped_column(String(20))
    adpt_de: Mapped[str | None] = mapped_column(String(8))
