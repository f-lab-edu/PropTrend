"""서비스 DB Connection과, api/src/model의 테이블을 직접 다루는 컬럼 목록·SQL."""

# docker-compose.yml이 AIRFLOW_CONN_* 환경변수로 만드는 Connection. 메타 DB에는 저장하지 않는다.
PROP_TREND_DB_CONN_ID = "prop_trend_db"

# 아래 컬럼은 api/src/model의 정제 테이블과 같아야 한다. id·created_at은 DB 기본값에 맡긴다.
TRANSACTION_COMMON_COLUMNS = (
    "property_type",
    "house_type",
    "sido_code",
    "sigungu_code",
    "umd_name",
    "jibun",
    "building_name",
    "deal_date",
    "exclusive_use_area",
    "floor",
    "build_year",
    "total_floor_area",
    "apartment_serial_number",
    "sigungu_name",
)
SALE_COLUMNS = (
    *TRANSACTION_COMMON_COLUMNS,
    "plottage_area",
    "land_area",
    "deal_amount",
    "dealing_type",
    "estate_agent_sigungu_name",
    "seller_type",
    "buyer_type",
    "cancel_deal_type",
    "cancel_deal_date",
    "registration_date",
    "apartment_dong",
    "land_leasehold_type",
)
RENT_COLUMNS = (
    *TRANSACTION_COMMON_COLUMNS,
    "deposit",
    "monthly_rent",
    "contract_term",
    "contract_type",
    "renewal_right_used",
    "previous_deposit",
    "previous_monthly_rent",
    "road_address_detail",
)


def _transaction_sql(table: str, columns: tuple[str, ...]) -> tuple[str, str]:
    """정제 테이블 하나의 단위 삭제문과 적재문을 만든다."""
    # 테이블·컬럼 이름은 이 모듈의 상수에서만 온다. 값은 전부 바인드 파라미터다.
    placeholders = ", ".join("%s::jsonb" if column == "road_address_detail" else "%s" for column in columns)
    delete_sql = (
        f"DELETE FROM {table} WHERE property_type = %s AND sido_code = %s AND sigungu_code = %s "  # noqa: S608
        "AND deal_date >= %s AND deal_date < %s"
    )
    insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"  # noqa: S608
    return delete_sql, insert_sql


SALE_DELETE_SQL, SALE_INSERT_SQL = _transaction_sql("sale_transactions", SALE_COLUMNS)
RENT_DELETE_SQL, RENT_INSERT_SQL = _transaction_sql("rent_transactions", RENT_COLUMNS)

# api/src/model/complex.py의 COMPLEX_KEY_COLUMNS와 같아야 한다. 단지를 만드는 유형은 이 둘뿐이다.
# 아파트는 단지명이 과거 거래까지 소급해 바뀌므로 이름이 아니라 단지 일련번호로 식별한다.
COMPLEX_KEY_COLUMNS = {
    "APT": ("apartment_serial_number",),
    "OFFICETEL": ("sido_code", "sigungu_code", "umd_name", "jibun", "building_name", "build_year"),
}
COMPLEX_COLUMNS = (
    "sido_code",
    "sigungu_code",
    "umd_name",
    "jibun",
    "building_name",
    "build_year",
    "apartment_serial_number",
)
COMPLEX_UPSERT_SQL = {
    # 키가 아닌 속성은 최근 거래 값으로 맞추되, 매일 같은 값으로 덮어 updated_at과 행 버전만 쌓이지 않도록
    # 달라졌을 때만 고친다. ON CONFLICT가 부분 유니크 인덱스를 추론하려면 인덱스와 같은 WHERE를 넘겨야 한다.
    "APT": """
        INSERT INTO complexes (property_type, sido_code, sigungu_code, umd_name, jibun, building_name, build_year,
                               apartment_serial_number)
        VALUES ('APT', %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (apartment_serial_number) WHERE property_type = 'APT'
        DO UPDATE SET sido_code = EXCLUDED.sido_code, sigungu_code = EXCLUDED.sigungu_code,
                      umd_name = EXCLUDED.umd_name, jibun = EXCLUDED.jibun,
                      building_name = EXCLUDED.building_name, build_year = EXCLUDED.build_year,
                      updated_at = now()
        WHERE (complexes.sido_code, complexes.sigungu_code, complexes.umd_name, complexes.jibun,
               complexes.building_name, complexes.build_year)
              IS DISTINCT FROM
              (EXCLUDED.sido_code, EXCLUDED.sigungu_code, EXCLUDED.umd_name, EXCLUDED.jibun,
               EXCLUDED.building_name, EXCLUDED.build_year)
    """,
    # 오피스텔은 키 컬럼이 곧 속성 전부라 고칠 값이 없다.
    "OFFICETEL": """
        INSERT INTO complexes (property_type, sido_code, sigungu_code, umd_name, jibun, building_name, build_year,
                               apartment_serial_number)
        VALUES ('OFFICETEL', %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (sido_code, sigungu_code, umd_name, jibun, building_name, build_year)
            WHERE property_type = 'OFFICETEL'
        DO NOTHING
    """,
}

# api/src/model/region.py의 컬럼과 같아야 한다. 값은 preprocess_regions가 만든 dict에서 이름으로 꺼낸다.
REGION_COLUMNS = ("sido_code", "sigungu_code", "sido_name", "sigungu_name")
REGION_INSERT_SQL = (
    f"INSERT INTO regions ({', '.join(REGION_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join(f'%({column})s' for column in REGION_COLUMNS)})"
)
