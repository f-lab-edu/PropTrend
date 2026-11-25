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
# 단위의 신규 거래가 속한 단지 id를 단지 키로 찾는다. 결과 컬럼은 id 뒤에 COMPLEX_KEY_COLUMNS 순서다.
# 아파트는 단지 일련번호로, 오피스텔은 시군구로 좁힌다. 둘 다 부분 유니크 인덱스의 선두 컬럼이다.
COMPLEX_ID_SQL = {
    "APT": "SELECT id, apartment_serial_number FROM complexes "
    "WHERE property_type = 'APT' AND apartment_serial_number = ANY(%(serials)s)",
    "OFFICETEL": f"SELECT id, {', '.join(COMPLEX_KEY_COLUMNS['OFFICETEL'])} FROM complexes "  # noqa: S608
    "WHERE property_type = 'OFFICETEL' AND sido_code = %(sido)s AND sigungu_code = %(sigungu)s",
}

# 신규 거래를 가르는 지문. 같은 거래가 갱신마다 같은 값을 갖는 컬럼만 둔다. 해제여부·등기일자처럼 신고 뒤에 채워지는
# 값과, 소급해 바뀌는 아파트 단지명은 넣지 않는다. 앞에 단지 키를 둬 신규 거래의 단지를 지문에서 바로 꺼낸다.
SALE_FINGERPRINT_DEAL_COLUMNS = ("deal_date", "exclusive_use_area", "floor", "apartment_dong", "deal_amount")
RENT_FINGERPRINT_DEAL_COLUMNS = (
    "deal_date",
    "exclusive_use_area",
    "floor",
    "deposit",
    "monthly_rent",
    "contract_term",
    "contract_type",
)


def _fingerprint_sql(delete_sql: str, deal_columns: tuple[str, ...]) -> dict[str, tuple[tuple[str, ...], str]]:
    """단지 유형마다 지문 컬럼과, 단위를 지우며 지운 행의 지문을 돌려받는 삭제문을 만든다."""
    fingerprints: dict[str, tuple[tuple[str, ...], str]] = {}
    for property_type, key_columns in COMPLEX_KEY_COLUMNS.items():
        columns = (*key_columns, *deal_columns)
        fingerprints[property_type] = (columns, f"{delete_sql} RETURNING {', '.join(columns)}")
    return fingerprints


SALE_FINGERPRINTS = _fingerprint_sql(SALE_DELETE_SQL, SALE_FINGERPRINT_DEAL_COLUMNS)
RENT_FINGERPRINTS = _fingerprint_sql(RENT_DELETE_SQL, RENT_FINGERPRINT_DEAL_COLUMNS)

# api/src/model/notification.py의 대기열 테이블과 같아야 한다. 회차가 끝나기 전에는 단지마다 열린 행 하나에 건수를
# 더한다. ON CONFLICT가 부분 유니크 인덱스를 추론하려면 인덱스와 같은 WHERE를 넘겨야 한다.
TRANSACTION_EVENT_UPSERT_SQL = """
    INSERT INTO transaction_events (complex_id, sale_count, rent_count) VALUES (%s, %s, %s)
    ON CONFLICT (complex_id) WHERE available_at IS NULL
    DO UPDATE SET sale_count = transaction_events.sale_count + EXCLUDED.sale_count,
                  rent_count = transaction_events.rent_count + EXCLUDED.rent_count
"""
# 열린 행을 모두 연다. 이번 회차의 것만 고르지 않으므로, 앞 회차가 발행 전에 끝났어도 남은 행이 함께 열린다.
TRANSACTION_EVENT_PUBLISH_SQL = "UPDATE transaction_events SET available_at = now() WHERE available_at IS NULL"

# api/src/model/region.py의 컬럼과 같아야 한다. 값은 preprocess_regions가 만든 dict에서 이름으로 꺼낸다.
REGION_COLUMNS = ("sido_code", "sigungu_code", "sido_name", "sigungu_name")
REGION_INSERT_SQL = (
    f"INSERT INTO regions ({', '.join(REGION_COLUMNS)}) "  # noqa: S608
    f"VALUES ({', '.join(f'%({column})s' for column in REGION_COLUMNS)})"
)

# api/src/model/market.py의 결과 테이블과 같아야 한다. id는 DB 기본값에 맡긴다.
MARKET_PRICE_MOVER_COLUMNS = (
    "change_rate",
    "latest_id",
    "sido_code",
    "sigungu_code",
    "umd_name",
    "jibun",
    "building_name",
    "apartment_dong",
    "floor",
    "build_year",
    "exclusive_use_area",
    "deal_date",
    "deal_amount",
    "dealing_type",
    "cancel_deal_date",
    "complex_id",
    "previous_id",
    "previous_deal_date",
    "previous_deal_amount",
    "previous_floor",
)
# 이 기간 안의 거래만 비교에 쓴다.
PRICE_MOVER_LOOKBACK_YEARS = 5
# 최근 거래가 이 기간 안에 있는 단지만 순위에 넣는다. 오래전 거래끼리 비교한 결과가 섞이지 않게 한다.
PRICE_MOVER_RECENT_YEARS = 1
PRICE_MOVER_LIMIT = 5
# 결과 테이블 이름이 곧 market_snapshots의 name이다. API가 같은 이름으로 기준일을 찾는다.
MARKET_PRICE_MOVERS_TABLE = "market_price_movers"
# 단지는 api/src/services/prop_transaction.py의 PRICE_TREND_GROUP_COLUMNS[APT](단지 일련번호 + 전용면적)로 묶는다.
# 기준일이 2/29여도 깨지지 않도록 연 단위 경계는 interval 연산으로 구한다.
MARKET_PRICE_MOVERS_INSERT_SQL = f"""
    WITH ranked AS (
        SELECT id AS latest_id, sido_code, sigungu_code, umd_name, jibun, building_name, apartment_dong, floor,
               build_year, exclusive_use_area, deal_date, deal_amount, dealing_type, cancel_deal_date,
               apartment_serial_number,
               row_number() OVER w AS rn,
               lead(id) OVER w AS previous_id,
               lead(deal_date) OVER w AS previous_deal_date,
               lead(deal_amount) OVER w AS previous_deal_amount,
               lead(floor) OVER w AS previous_floor
        FROM sale_transactions
        WHERE property_type = 'APT'
          AND deal_date >= %(base_date)s::date - make_interval(years => {PRICE_MOVER_LOOKBACK_YEARS})
          -- 해제된 거래는 실제로 성사되지 않은 가격이라 비교에서 뺀다.
          AND cancel_deal_type IS DISTINCT FROM 'O'
          -- 묶음 컬럼이 NULL이면 같은 단지를 특정할 수 없어 뺀다.
          AND apartment_serial_number IS NOT NULL
          AND exclusive_use_area IS NOT NULL
        WINDOW w AS (
            PARTITION BY sido_code, sigungu_code, apartment_serial_number, exclusive_use_area
            ORDER BY deal_date DESC, id DESC
        )
    ),
    candidates AS (
        -- 단지의 최근 거래 한 건에 직전 거래가 있으면 거래가 2건 이상인 단지다.
        -- 직전 거래액 0원은 막는 제약이 없어 0 나누기로 갱신 전체가 실패하지 않게 NULL로 두고 부호 조건에서 뺀다.
        SELECT *,
               round((deal_amount - previous_deal_amount)::numeric * 100 / NULLIF(previous_deal_amount, 0), 2)
                   AS change_rate
        FROM ranked
        WHERE rn = 1
          AND previous_id IS NOT NULL
          AND deal_date >= %(base_date)s::date - make_interval(years => {PRICE_MOVER_RECENT_YEARS})
    ),
    movers AS (
        -- 변동률이 같으면 id가 작은 거래를 먼저 둔다.
        (SELECT * FROM candidates WHERE change_rate > 0 ORDER BY change_rate DESC, latest_id
         LIMIT {PRICE_MOVER_LIMIT})
        UNION ALL
        (SELECT * FROM candidates WHERE change_rate < 0 ORDER BY change_rate, latest_id
         LIMIT {PRICE_MOVER_LIMIT})
    )
    INSERT INTO {MARKET_PRICE_MOVERS_TABLE} ({", ".join(MARKET_PRICE_MOVER_COLUMNS)})
    SELECT {", ".join(MARKET_PRICE_MOVER_COLUMNS)}
    FROM (
        SELECT movers.*, complexes.id AS complex_id
        FROM movers
        LEFT JOIN complexes
            ON complexes.property_type = 'APT'
           AND complexes.apartment_serial_number = movers.apartment_serial_number
    ) AS joined
"""  # noqa: S608 - 테이블·컬럼 이름과 숫자는 이 모듈의 상수에서만 온다. 기준일은 바인드 파라미터다.
MARKET_SNAPSHOT_UPSERT_SQL = """
    INSERT INTO market_snapshots (name, base_date, refreshed_at) VALUES (%(name)s, %(base_date)s, now())
    ON CONFLICT (name) DO UPDATE SET base_date = EXCLUDED.base_date, refreshed_at = EXCLUDED.refreshed_at
"""
