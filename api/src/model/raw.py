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

from sqlalchemy import Index, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, RawRecordMixin


class RawApartSale(Base, RawRecordMixin):
    __tablename__ = "raw_apart_sale"

    # 가공 단계가 (계약년월, 시군구) 단위로 읽어갈 때 쓰는 인덱스. dealMonth는
    # lpad를 거쳐 비교하므로 인덱스에 넣어도 타지 않아 뺐고, dealYear를 앞에 둬야
    # 시군구를 생략한 전국 조회에서도 선두 컬럼이 조건에 남는다.
    __table_args__ = (
        Index("ix_raw_apart_sale_deal_year_sgg_cd", "dealYear", "sggCd"),
    )

    sggCd: Mapped[str | None] = mapped_column(String(5))
    umdNm: Mapped[str | None] = mapped_column(String(60))
    aptNm: Mapped[str | None] = mapped_column(String(100))
    jibun: Mapped[str | None] = mapped_column(String(20))
    excluUseAr: Mapped[str | None] = mapped_column(String(22))
    dealYear: Mapped[str | None] = mapped_column(String(4))
    dealMonth: Mapped[str | None] = mapped_column(String(2))
    dealDay: Mapped[str | None] = mapped_column(String(2))
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


class RawApartRent(Base, RawRecordMixin):
    __tablename__ = "raw_apart_rent"

    # 가공 단계가 (계약년월, 시군구) 단위로 읽어갈 때 쓰는 인덱스. dealMonth는
    # lpad를 거쳐 비교하므로 인덱스에 넣어도 타지 않아 뺐고, dealYear를 앞에 둬야
    # 시군구를 생략한 전국 조회에서도 선두 컬럼이 조건에 남는다.
    __table_args__ = (
        Index("ix_raw_apart_rent_deal_year_sgg_cd", "dealYear", "sggCd"),
    )

    sggCd: Mapped[str | None] = mapped_column(String(5))
    umdNm: Mapped[str | None] = mapped_column(String(30))
    aptNm: Mapped[str | None] = mapped_column(String(100))
    jibun: Mapped[str | None] = mapped_column(String(20))
    excluUseAr: Mapped[str | None] = mapped_column(String(22))
    dealYear: Mapped[str | None] = mapped_column(String(4))
    dealMonth: Mapped[str | None] = mapped_column(String(2))
    dealDay: Mapped[str | None] = mapped_column(String(2))
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


class RawOfficetelSale(Base, RawRecordMixin):
    __tablename__ = "raw_officetel_sale"

    # 가공 단계가 (계약년월, 시군구) 단위로 읽어갈 때 쓰는 인덱스. dealMonth는
    # lpad를 거쳐 비교하므로 인덱스에 넣어도 타지 않아 뺐고, dealYear를 앞에 둬야
    # 시군구를 생략한 전국 조회에서도 선두 컬럼이 조건에 남는다.
    __table_args__ = (
        Index("ix_raw_officetel_sale_deal_year_sgg_cd", "dealYear", "sggCd"),
    )

    sggCd: Mapped[str | None] = mapped_column(String(5))
    sggNm: Mapped[str | None] = mapped_column(String(30))
    umdNm: Mapped[str | None] = mapped_column(String(60))
    jibun: Mapped[str | None] = mapped_column(String(20))
    offiNm: Mapped[str | None] = mapped_column(String(100))
    excluUseAr: Mapped[str | None] = mapped_column(String(22))
    dealYear: Mapped[str | None] = mapped_column(String(4))
    dealMonth: Mapped[str | None] = mapped_column(String(2))
    dealDay: Mapped[str | None] = mapped_column(String(2))
    dealAmount: Mapped[str | None] = mapped_column(String(40))
    floor: Mapped[str | None] = mapped_column(String(10))
    buildYear: Mapped[str | None] = mapped_column(String(4))
    cdealType: Mapped[str | None] = mapped_column(String(1))
    cdealDay: Mapped[str | None] = mapped_column(String(8))
    dealingGbn: Mapped[str | None] = mapped_column(String(10))
    estateAgentSggNm: Mapped[str | None] = mapped_column(String(3000))
    slerGbn: Mapped[str | None] = mapped_column(String(100))
    buyerGbn: Mapped[str | None] = mapped_column(String(100))


class RawOfficetelRent(Base, RawRecordMixin):
    __tablename__ = "raw_officetel_rent"

    # 가공 단계가 (계약년월, 시군구) 단위로 읽어갈 때 쓰는 인덱스. dealMonth는
    # lpad를 거쳐 비교하므로 인덱스에 넣어도 타지 않아 뺐고, dealYear를 앞에 둬야
    # 시군구를 생략한 전국 조회에서도 선두 컬럼이 조건에 남는다.
    __table_args__ = (
        Index("ix_raw_officetel_rent_deal_year_sgg_cd", "dealYear", "sggCd"),
    )

    sggCd: Mapped[str | None] = mapped_column(String(5))
    sggNm: Mapped[str | None] = mapped_column(String(30))
    umdNm: Mapped[str | None] = mapped_column(String(60))
    jibun: Mapped[str | None] = mapped_column(String(20))
    offiNm: Mapped[str | None] = mapped_column(String(100))
    excluUseAr: Mapped[str | None] = mapped_column(String(22))
    dealYear: Mapped[str | None] = mapped_column(String(4))
    dealMonth: Mapped[str | None] = mapped_column(String(2))
    dealDay: Mapped[str | None] = mapped_column(String(2))
    deposit: Mapped[str | None] = mapped_column(String(40))
    monthlyRent: Mapped[str | None] = mapped_column(String(40))
    floor: Mapped[str | None] = mapped_column(String(10))
    buildYear: Mapped[str | None] = mapped_column(String(4))
    contractTerm: Mapped[str | None] = mapped_column(String(12))
    contractType: Mapped[str | None] = mapped_column(String(4))
    useRRRight: Mapped[str | None] = mapped_column(String(4))
    preDeposit: Mapped[str | None] = mapped_column(String(40))
    preMonthlyRent: Mapped[str | None] = mapped_column(String(40))


class RawMultiflexSale(Base, RawRecordMixin):
    __tablename__ = "raw_multiflex_sale"

    # 가공 단계가 (계약년월, 시군구) 단위로 읽어갈 때 쓰는 인덱스. dealMonth는
    # lpad를 거쳐 비교하므로 인덱스에 넣어도 타지 않아 뺐고, dealYear를 앞에 둬야
    # 시군구를 생략한 전국 조회에서도 선두 컬럼이 조건에 남는다.
    __table_args__ = (
        Index("ix_raw_multiflex_sale_deal_year_sgg_cd", "dealYear", "sggCd"),
    )

    sggCd: Mapped[str | None] = mapped_column(String(5))
    umdNm: Mapped[str | None] = mapped_column(String(60))
    mhouseNm: Mapped[str | None] = mapped_column(String(100))
    jibun: Mapped[str | None] = mapped_column(String(20))
    buildYear: Mapped[str | None] = mapped_column(String(4))
    excluUseAr: Mapped[str | None] = mapped_column(String(22))
    landAr: Mapped[str | None] = mapped_column(String(22))
    dealYear: Mapped[str | None] = mapped_column(String(4))
    dealMonth: Mapped[str | None] = mapped_column(String(2))
    dealDay: Mapped[str | None] = mapped_column(String(2))
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


class RawMultiflexRent(Base, RawRecordMixin):
    __tablename__ = "raw_multiflex_rent"

    # 가공 단계가 (계약년월, 시군구) 단위로 읽어갈 때 쓰는 인덱스. dealMonth는
    # lpad를 거쳐 비교하므로 인덱스에 넣어도 타지 않아 뺐고, dealYear를 앞에 둬야
    # 시군구를 생략한 전국 조회에서도 선두 컬럼이 조건에 남는다.
    __table_args__ = (
        Index("ix_raw_multiflex_rent_deal_year_sgg_cd", "dealYear", "sggCd"),
    )

    sggCd: Mapped[str | None] = mapped_column(String(5))
    umdNm: Mapped[str | None] = mapped_column(String(30))
    houseType: Mapped[str | None] = mapped_column(String(6))
    mhouseNm: Mapped[str | None] = mapped_column(String(100))
    jibun: Mapped[str | None] = mapped_column(String(20))
    buildYear: Mapped[str | None] = mapped_column(String(4))
    excluUseAr: Mapped[str | None] = mapped_column(String(22))
    dealYear: Mapped[str | None] = mapped_column(String(4))
    dealMonth: Mapped[str | None] = mapped_column(String(2))
    dealDay: Mapped[str | None] = mapped_column(String(2))
    deposit: Mapped[str | None] = mapped_column(String(40))
    monthlyRent: Mapped[str | None] = mapped_column(String(40))
    floor: Mapped[str | None] = mapped_column(String(10))
    contractTerm: Mapped[str | None] = mapped_column(String(12))
    contractType: Mapped[str | None] = mapped_column(String(4))
    useRRRight: Mapped[str | None] = mapped_column(String(4))
    preDeposit: Mapped[str | None] = mapped_column(String(40))
    preMonthlyRent: Mapped[str | None] = mapped_column(String(40))


class RawSingleMultiFamilySale(Base, RawRecordMixin):
    __tablename__ = "raw_single_multi_family_sale"

    # 가공 단계가 (계약년월, 시군구) 단위로 읽어갈 때 쓰는 인덱스. dealMonth는
    # lpad를 거쳐 비교하므로 인덱스에 넣어도 타지 않아 뺐고, dealYear를 앞에 둬야
    # 시군구를 생략한 전국 조회에서도 선두 컬럼이 조건에 남는다.
    __table_args__ = (
        Index("ix_raw_single_multi_family_sale_deal_year_sgg_cd", "dealYear", "sggCd"),
    )

    sggCd: Mapped[str | None] = mapped_column(String(5))
    umdNm: Mapped[str | None] = mapped_column(String(60))
    houseType: Mapped[str | None] = mapped_column(String(6))
    jibun: Mapped[str | None] = mapped_column(String(20))
    totalFloorAr: Mapped[str | None] = mapped_column(String(22))
    plottageAr: Mapped[str | None] = mapped_column(String(22))
    dealYear: Mapped[str | None] = mapped_column(String(4))
    dealMonth: Mapped[str | None] = mapped_column(String(2))
    dealDay: Mapped[str | None] = mapped_column(String(2))
    dealAmount: Mapped[str | None] = mapped_column(String(40))
    buildYear: Mapped[str | None] = mapped_column(String(40))
    cdealType: Mapped[str | None] = mapped_column(String(1))
    cdealDay: Mapped[str | None] = mapped_column(String(8))
    dealingGbn: Mapped[str | None] = mapped_column(String(10))
    estateAgentSggNm: Mapped[str | None] = mapped_column(String(3000))
    slerGbn: Mapped[str | None] = mapped_column(String(100))
    buyerGbn: Mapped[str | None] = mapped_column(String(100))


class RawSingleMultiFamilyRent(Base, RawRecordMixin):
    __tablename__ = "raw_single_multi_family_rent"

    # 가공 단계가 (계약년월, 시군구) 단위로 읽어갈 때 쓰는 인덱스. dealMonth는
    # lpad를 거쳐 비교하므로 인덱스에 넣어도 타지 않아 뺐고, dealYear를 앞에 둬야
    # 시군구를 생략한 전국 조회에서도 선두 컬럼이 조건에 남는다.
    __table_args__ = (
        Index("ix_raw_single_multi_family_rent_deal_year_sgg_cd", "dealYear", "sggCd"),
    )

    sggCd: Mapped[str | None] = mapped_column(String(5))
    houseType: Mapped[str | None] = mapped_column(String(6))
    umdNm: Mapped[str | None] = mapped_column(String(30))
    totalFloorAr: Mapped[str | None] = mapped_column(String(22))
    dealYear: Mapped[str | None] = mapped_column(String(4))
    dealMonth: Mapped[str | None] = mapped_column(String(2))
    dealDay: Mapped[str | None] = mapped_column(String(2))
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
