from datetime import date
from decimal import Decimal

from app.model.prop_transaction import PropertyType, RentTransaction, SaleTransaction
from data_collection.processor import (
    LegalDongCodeProcessor,
    RTMSAptRentProcessor,
    RTMSAptTradeProcessor,
    RTMSOffiRentProcessor,
    RTMSOffiTradeProcessor,
    RTMSRHRentProcessor,
    RTMSRHTradeProcessor,
    RTMSSHRentProcessor,
    RTMSSHTradeProcessor,
)

SALE_COLUMNS = {column.name for column in SaleTransaction.__table__.columns} - {"id"}
RENT_COLUMNS = {column.name for column in RentTransaction.__table__.columns} - {"id"}


def test_legal_dong_code_processor_keeps_only_sigungu_level_rows() -> None:
    raw_items = [
        {
            "sido_cd": "11",
            "sgg_cd": "110",
            "umd_cd": "000",
            "ri_cd": "00",
            "locatadd_nm": "서울특별시 종로구",
        },
        {
            # 읍면동 단위 row는 제외되어야 한다.
            "sido_cd": "11",
            "sgg_cd": "110",
            "umd_cd": "101",
            "ri_cd": "00",
            "locatadd_nm": "서울특별시 종로구 청운동",
        },
        {
            # 리 단위 row는 제외되어야 한다.
            "sido_cd": "41",
            "sgg_cd": "111",
            "umd_cd": "000",
            "ri_cd": "01",
            "locatadd_nm": "경기도 수원시 어딘가리",
        },
    ]

    result = LegalDongCodeProcessor().process(raw_items)

    assert result == [{"code": "11110", "name": "서울특별시 종로구"}]


def test_apt_trade_processor_maps_fields() -> None:
    item = {
        "aptDong": "",
        "aptNm": "종로중흥S클래스",
        "buildYear": "2013",
        "buyerGbn": "개인",
        "cdealDay": "20240815",
        "cdealType": "해제",
        "dealAmount": "12,000",
        "dealDay": "23",
        "dealMonth": "7",
        "dealYear": "2024",
        "dealingGbn": "중개거래",
        "estateAgentSggNm": "서울 종로구",
        "excluUseAr": "17.811",
        "floor": "10",
        "jibun": "202-3",
        "landLeaseholdGbn": "N",
        "rgstDate": "",
        "sggCd": "11110",
        "slerGbn": "개인",
        "umdNm": "숭인동",
    }

    row = RTMSAptTradeProcessor().process([item])[0]

    assert row["property_type"] == PropertyType.APT
    assert row["house_type"] is None
    assert row["sido_code"] == "11"
    assert row["sigungu_code"] == "110"
    assert row["umd_name"] == "숭인동"
    assert row["jibun"] == "202-3"
    assert row["building_name"] == "종로중흥S클래스"
    assert row["deal_date"] == date(2024, 7, 23)
    assert row["exclusive_use_area"] == Decimal("17.811")
    assert row["floor"] == 10
    assert row["build_year"] == 2013
    assert row["deal_amount"] == 120_000_000
    assert row["dealing_type"] == "중개거래"
    assert row["estate_agent_sigungu_name"] == "서울 종로구"
    assert row["seller_type"] == "개인"
    assert row["buyer_type"] == "개인"
    assert row["cancel_deal_type"] == "해제"
    assert row["cancel_deal_date"] == date(2024, 8, 15)
    assert row["registration_date"] is None
    assert row["apartment_dong"] is None
    assert row["land_leasehold_type"] == "N"
    assert row.keys() <= SALE_COLUMNS


def test_offi_trade_processor_maps_officetel_specific_fields() -> None:
    item = {
        "buildYear": "2004",
        "buyerGbn": "개인",
        "cdealDay": "",
        "cdealType": "",
        "dealAmount": "25,200",
        "dealDay": "17",
        "dealMonth": "7",
        "dealYear": "2024",
        "dealingGbn": "중개거래",
        "estateAgentSggNm": "서울 종로구",
        "excluUseAr": "28.19",
        "floor": "9",
        "jibun": "75",
        "offiNm": "용비어천가",
        "sggCd": "11110",
        "sggNm": "종로구",
        "slerGbn": "개인",
        "umdNm": "내수동",
    }

    row = RTMSOffiTradeProcessor().process([item])[0]

    assert row["property_type"] == PropertyType.OFFICETEL
    assert row["building_name"] == "용비어천가"
    assert row["sigungu_name"] == "종로구"
    assert row["deal_amount"] == 252_000_000
    assert row.keys() <= SALE_COLUMNS


def test_rh_trade_processor_maps_house_type_and_land_area() -> None:
    item = {
        "buildYear": "2003",
        "buyerGbn": "개인",
        "cdealDay": "",
        "cdealType": "",
        "dealAmount": "36,900",
        "dealDay": "23",
        "dealMonth": "7",
        "dealYear": "2024",
        "dealingGbn": "중개거래",
        "estateAgentSggNm": "서울 종로구",
        "excluUseAr": "57.21",
        "floor": "2",
        "houseType": "다세대",
        "jibun": "178-84",
        "landAr": "29.17",
        "mhouseNm": "현진빌라B동",
        "rgstDate": "",
        "sggCd": "11110",
        "slerGbn": "개인",
        "umdNm": "숭인동",
    }

    row = RTMSRHTradeProcessor().process([item])[0]

    assert row["property_type"] == PropertyType.ROW_HOUSE
    assert row["house_type"] == "다세대"
    assert row["building_name"] == "현진빌라B동"
    assert row["land_area"] == Decimal("29.17")
    assert row["registration_date"] is None
    assert row.keys() <= SALE_COLUMNS


def test_sh_trade_processor_has_no_building_name_or_exclusive_area() -> None:
    item = {
        "buildYear": "1973",
        "buyerGbn": "개인",
        "cdealDay": "",
        "cdealType": "",
        "dealAmount": "98,700",
        "dealDay": "13",
        "dealMonth": "7",
        "dealYear": "2024",
        "dealingGbn": "중개거래",
        "estateAgentSggNm": "서울 종로구",
        "houseType": "단독",
        "jibun": "",
        "plottageAr": "83",
        "sggCd": "11110",
        "slerGbn": "개인",
        "totalFloorAr": "120.2",
        "umdNm": "옥인동",
    }

    row = RTMSSHTradeProcessor().process([item])[0]

    assert row["property_type"] == PropertyType.SINGLE_MULTI
    assert row["house_type"] == "단독"
    assert row["building_name"] is None
    assert row["jibun"] is None
    assert row["exclusive_use_area"] is None
    assert row["floor"] is None
    assert row["total_floor_area"] == Decimal("120.2")
    assert row["plottage_area"] == Decimal(83)
    assert row["deal_amount"] == 987_000_000
    assert row.keys() <= SALE_COLUMNS


def test_apt_rent_processor_maps_road_address_detail_and_serial_number() -> None:
    item = {
        "aptNm": "두산",
        "aptSeq": "11110-34",
        "buildYear": "1999",
        "contractTerm": "",
        "contractType": "",
        "dealDay": "20",
        "dealMonth": "7",
        "dealYear": "2024",
        "deposit": "50,000",
        "excluUseAr": "59.95",
        "floor": "3",
        "jibun": "232",
        "monthlyRent": "0",
        "preDeposit": "",
        "preMonthlyRent": "",
        "roadnm": "지봉로5길 7",
        "roadnmbcd": "0",
        "roadnmbonbun": "00007",
        "roadnmbubun": "00000",
        "roadnmcd": "4100390",
        "roadnmseq": "1",
        "roadnmsggcd": "11110",
        "sggCd": "11110",
        "umdNm": "창신동",
        "useRRRight": "",
    }

    row = RTMSAptRentProcessor().process([item])[0]

    assert row["property_type"] == PropertyType.APT
    assert row["deposit"] == 500_000_000
    assert row["monthly_rent"] == 0
    assert row["apartment_serial_number"] == "11110-34"
    assert row["previous_deposit"] is None
    assert row["previous_monthly_rent"] is None
    assert row["road_address_detail"] == {
        "roadnm": "지봉로5길 7",
        "roadnmsggcd": "11110",
        "roadnmcd": "4100390",
        "roadnmseq": "1",
        "roadnmbcd": "0",
        "roadnmbonbun": "00007",
        "roadnmbubun": "00000",
    }
    assert row.keys() <= RENT_COLUMNS


def test_offi_rent_processor_maps_sigungu_name_and_previous_contract() -> None:
    item = {
        "buildYear": "2022",
        "contractTerm": "24.09~26.09",
        "contractType": "신규",
        "dealDay": "31",
        "dealMonth": "7",
        "dealYear": "2024",
        "deposit": "19,400",
        "excluUseAr": "21.14",
        "floor": "13",
        "jibun": "1425",
        "monthlyRent": "0",
        "offiNm": "한라운종가",
        "preDeposit": "18,000",
        "preMonthlyRent": "0",
        "sggCd": "11110",
        "sggNm": "종로구",
        "umdNm": "숭인동",
        "useRRRight": "",
    }

    row = RTMSOffiRentProcessor().process([item])[0]

    assert row["sigungu_name"] == "종로구"
    assert row["contract_term"] == "24.09~26.09"
    assert row["contract_type"] == "신규"
    assert row["previous_deposit"] == 180_000_000
    assert row["previous_monthly_rent"] == 0
    assert row.keys() <= RENT_COLUMNS


def test_rh_rent_processor_maps_house_type() -> None:
    item = {
        "buildYear": "2019",
        "contractTerm": "",
        "contractType": "",
        "dealDay": "10",
        "dealMonth": "7",
        "dealYear": "2024",
        "deposit": "70,000",
        "excluUseAr": "84.99",
        "floor": "-1",
        "houseType": "연립",
        "jibun": "211-11",
        "mhouseNm": "북악더테라스2단지",
        "monthlyRent": "0",
        "preDeposit": "",
        "preMonthlyRent": "",
        "sggCd": "11110",
        "umdNm": "신영동",
        "useRRRight": "",
    }

    row = RTMSRHRentProcessor().process([item])[0]

    assert row["house_type"] == "연립"
    assert row["building_name"] == "북악더테라스2단지"
    assert row["floor"] == -1
    assert row.keys() <= RENT_COLUMNS


def test_sh_rent_processor_has_no_jibun_field_in_raw_response() -> None:
    item = {
        "contractTerm": "",
        "contractType": "단독",
        "dealDay": "26",
        "dealMonth": "7",
        "dealYear": "2024",
        "deposit": "10,000",
        "houseType": "단독",
        "monthlyRent": "100",
        "preDeposit": "",
        "preMonthlyRent": "",
        "sggCd": "11110",
        "totalFloorAr": "55",
        "umdNm": "청운동",
        "useRRRight": "",
    }

    row = RTMSSHRentProcessor().process([item])[0]

    assert row["jibun"] is None
    assert row["building_name"] is None
    assert row["total_floor_area"] == Decimal(55)
    assert row["deposit"] == 100_000_000
    assert row["monthly_rent"] == 1_000_000
    assert row.keys() <= RENT_COLUMNS


def test_trade_processor_normalizes_whitespace_padded_values() -> None:
    # 원본 응답은 값이 없는 항목을 빈 문자열이 아니라 공백 한 칸으로 채워 보내기도 한다.
    item = {
        "aptDong": " ",
        "aptNm": "종로중흥S클래스",
        "buildYear": " ",
        "buyerGbn": " ",
        "cdealDay": " ",
        "cdealType": " ",
        "dealAmount": "12,000",
        "dealDay": "23",
        "dealMonth": "7",
        "dealYear": "2024",
        "dealingGbn": " ",
        "estateAgentSggNm": " ",
        "excluUseAr": " ",
        "floor": " ",
        "jibun": " ",
        "landLeaseholdGbn": " ",
        "rgstDate": " ",
        "sggCd": " 11110 ",
        "slerGbn": " ",
        "umdNm": " 숭인동 ",
    }

    row = RTMSAptTradeProcessor().process([item])[0]

    assert row["sido_code"] == "11"
    assert row["sigungu_code"] == "110"
    assert row["umd_name"] == "숭인동"
    assert row["jibun"] is None
    assert row["build_year"] is None
    assert row["exclusive_use_area"] is None
    assert row["floor"] is None
    assert row["apartment_dong"] is None
    assert row["cancel_deal_type"] is None
    assert row["cancel_deal_date"] is None
    assert row["registration_date"] is None
    assert row["deal_amount"] == 120_000_000


def test_trade_processor_parses_dotted_two_digit_year_dates() -> None:
    # 등기일자/해제사유발생일은 명세상 8자리지만 실제 응답은 "25.11.17" 형식으로 온다.
    item = {
        "aptDong": "105",
        "aptNm": "현대",
        "buildYear": "2000",
        "buyerGbn": "개인",
        "cdealDay": "21.02.26",
        "cdealType": "해제",
        "dealAmount": "129,960",
        "dealDay": "22",
        "dealMonth": "6",
        "dealYear": "2025",
        "dealingGbn": "중개거래",
        "estateAgentSggNm": "서울 종로구",
        "excluUseAr": "84.92",
        "floor": "7",
        "jibun": "82",
        "landLeaseholdGbn": "N",
        "rgstDate": "25.11.17",
        "sggCd": "11110",
        "slerGbn": "개인",
        "umdNm": "무악동",
    }

    row = RTMSAptTradeProcessor().process([item])[0]

    assert row["registration_date"] == date(2025, 11, 17)
    assert row["cancel_deal_date"] == date(2021, 2, 26)


def test_rent_processor_normalizes_whitespace_padded_previous_contract() -> None:
    item = {
        "buildYear": "2019",
        "contractTerm": " ",
        "contractType": " ",
        "dealDay": "10",
        "dealMonth": "7",
        "dealYear": "2024",
        "deposit": "70,000",
        "excluUseAr": "84.99",
        "floor": "2",
        "houseType": "연립",
        "jibun": "211-11",
        "mhouseNm": "북악더테라스2단지",
        "monthlyRent": "0",
        "preDeposit": " ",
        "preMonthlyRent": " ",
        "sggCd": "11110",
        "umdNm": "신영동",
        "useRRRight": " ",
    }

    row = RTMSRHRentProcessor().process([item])[0]

    assert row["contract_term"] is None
    assert row["contract_type"] is None
    assert row["renewal_right_used"] is None
    assert row["previous_deposit"] is None
    assert row["previous_monthly_rent"] is None


def test_apt_rent_processor_drops_whitespace_only_road_address_fields() -> None:
    item = {
        "aptNm": "두산",
        "aptSeq": " ",
        "buildYear": "1999",
        "contractTerm": " ",
        "contractType": " ",
        "dealDay": "20",
        "dealMonth": "7",
        "dealYear": "2024",
        "deposit": "50,000",
        "excluUseAr": "59.95",
        "floor": "3",
        "jibun": "232",
        "monthlyRent": "0",
        "preDeposit": " ",
        "preMonthlyRent": " ",
        "roadnm": "지봉로5길 7",
        "roadnmbcd": " ",
        "roadnmbonbun": " ",
        "roadnmbubun": " ",
        "roadnmcd": " ",
        "roadnmseq": " ",
        "roadnmsggcd": "11110",
        "sggCd": "11110",
        "umdNm": "창신동",
        "useRRRight": " ",
    }

    row = RTMSAptRentProcessor().process([item])[0]

    assert row["apartment_serial_number"] is None
    assert row["road_address_detail"] == {
        "roadnm": "지봉로5길 7",
        "roadnmsggcd": "11110",
    }


def test_processor_drops_out_of_range_area_but_keeps_the_row() -> None:
    # 원본에 대지권면적 448㎢ 같은 오류값이 섞여 있다. 면적만 버리고 거래는 남겨야 한다.
    item = {
        "buildYear": "2011",
        "buyerGbn": "개인",
        "cdealDay": " ",
        "cdealType": " ",
        "dealAmount": "10,708",
        "dealDay": "28",
        "dealMonth": "3",
        "dealYear": "2013",
        "dealingGbn": " ",
        "estateAgentSggNm": " ",
        "excluUseAr": "59.94",
        "houseType": "다세대",
        "jibun": "1073-18",
        "landAr": "448128851.9",
        "mhouseNm": "휴먼1차",
        "rgstDate": " ",
        "sggCd": "48330",
        "slerGbn": "개인",
        "umdNm": "명동",
    }

    row = RTMSRHTradeProcessor().process([item])[0]

    assert row["land_area"] is None
    assert row["exclusive_use_area"] == Decimal("59.94")
    assert row["deal_amount"] == 107_080_000
    assert row["building_name"] == "휴먼1차"
