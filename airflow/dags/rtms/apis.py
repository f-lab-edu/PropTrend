"""실거래가 오픈API 8종의 유형 목록과, 오픈API 응답 필드 드리프트를 가리는 기준선."""

from typing import NamedTuple

# 실거래가 오픈API 8종은 같은 서비스(1613000) 아래 오퍼레이션 이름만 다르다. 호스트는 Connection이 갖는다.
RTMS_API = "1613000"


class RtmsApi(NamedTuple):
    """실거래가 API 하나의 호출 경로와 정제 테이블로 옮기는 방식."""

    endpoint: str
    # api/src/model/prop_transaction.py의 PropertyType 값. 정제 테이블의 property_type 컬럼에 그대로 들어간다.
    property_type: str
    is_sale: bool
    # 건물명 필드 이름은 유형마다 다르고, 단독·다가구에는 아예 없다.
    building_name_field: str | None
    # 응답 필드 드리프트 감지의 기준선. 이 API는 실제로 전 필드를 한 번 개명한 이력이 있다.
    known_fields: frozenset[str]


# scripts/src/process_silver.py의 SPECS와 같아야 한다. DAG는 src를 import하지 않으므로 따로 든다.
# 키는 rtms_raw_items.api_id에 그대로 들어가는 슬러그다. 오타가 나면 silver가 그 행을 영영 못 찾는다.
# 필드 기준선은 여기에만 있다. 응답 필드가 바뀌어 정리 규칙을 고칠 때 함께 고친다.
RTMS_APIS = {
    # 단지 일련번호(aptSeq)는 상세 자료 API에만 있다. 나머지 필드는 기본 API와 같다.
    "apart_sale": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
        "APT",
        True,
        "aptNm",
        frozenset(
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
                # 상세 자료 API(RTMSDataSvcAptTradeDev)에만 있는 필드.
                "aptSeq",
                "umdCd",
                "landCd",
                "bonbun",
                "bubun",
                "roadNm",
                "roadNmSggCd",
                "roadNmCd",
                "roadNmSeq",
                "roadNmbCd",
                "roadNmBonbun",
                "roadNmBubun",
            }
        ),
    ),
    "apart_rent": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcAptRent/getRTMSDataSvcAptRent",
        "APT",
        False,
        "aptNm",
        frozenset(
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
    ),
    "officetel_sale": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcOffiTrade/getRTMSDataSvcOffiTrade",
        "OFFICETEL",
        True,
        "offiNm",
        frozenset(
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
    ),
    "officetel_rent": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent",
        "OFFICETEL",
        False,
        "offiNm",
        frozenset(
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
    ),
    "multiflex_sale": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade",
        "ROW_HOUSE",
        True,
        "mhouseNm",
        frozenset(
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
    ),
    "multiflex_rent": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcRHRent/getRTMSDataSvcRHRent",
        "ROW_HOUSE",
        False,
        "mhouseNm",
        frozenset(
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
    ),
    "single_multi_family_sale": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade",
        "SINGLE_MULTI",
        True,
        None,
        frozenset(
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
    ),
    "single_multi_family_rent": RtmsApi(
        f"{RTMS_API}/RTMSDataSvcSHRent/getRTMSDataSvcSHRent",
        "SINGLE_MULTI",
        False,
        None,
        frozenset(
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
    ),
}
API_IDS = tuple(RTMS_APIS)

# 법정동코드 응답 필드 드리프트 경고의 기준선. RtmsApi.known_fields처럼 여기에만 있다.
LEGAL_DONG_CODE_KNOWN_FIELDS = frozenset(
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
