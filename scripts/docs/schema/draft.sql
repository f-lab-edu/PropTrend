-- 국토교통부 실거래가 8종 API(아파트/오피스텔/연립다세대/단독·다가구 x 매매/전월세)
-- 저장용 스키마 초안. 설계 근거/필드 매핑은 docs/schema/draft.md 참고.
-- PostgreSQL 문법 기준 (ERD Cloud Import SQL - PostgreSQL 옵션으로 불러오기 위함).

-- ============================================================
-- 1. sale_transactions (매매)
-- ============================================================

CREATE TABLE sale_transactions (
    id                         BIGSERIAL      PRIMARY KEY,

    -- 공통 컬럼
    property_type              VARCHAR(20)    NOT NULL,
    house_type                 VARCHAR(10),
    sido_code                  CHAR(2)        NOT NULL,
    sigungu_code               CHAR(3)        NOT NULL,
    umd_name                   VARCHAR(60)    NOT NULL,
    jibun                      VARCHAR(20),
    building_name              VARCHAR(100),
    deal_date                  DATE           NOT NULL,
    exclusive_use_area         NUMERIC(10,4),
    floor                      SMALLINT,
    build_year                 SMALLINT,

    -- 매매 전용 컬럼
    total_floor_area           NUMERIC(10,4),
    plottage_area              NUMERIC(10,4),
    land_area                  NUMERIC(10,4),
    deal_amount                BIGINT         NOT NULL,
    dealing_type               VARCHAR(20),
    estate_agent_sigungu_name  VARCHAR(100),
    seller_type                VARCHAR(20),
    buyer_type                 VARCHAR(20),
    cancel_deal_type           VARCHAR(10),
    cancel_deal_date           DATE,
    registration_date          DATE,
    apartment_dong             VARCHAR(50),
    land_leasehold_type        CHAR(1),
    sigungu_name               VARCHAR(30),

    CONSTRAINT chk_sale_transactions_property_type
        CHECK (property_type IN ('APT', 'OFFICETEL', 'ROW_HOUSE', 'SINGLE_MULTI')),

    CONSTRAINT uq_sale_transactions_dedup
        UNIQUE (sido_code, sigungu_code, umd_name, jibun, building_name, deal_date, floor, exclusive_use_area, deal_amount)
);

-- CREATE INDEX idx_sale_transactions_sido_sigungu_deal_date
--     ON sale_transactions (sido_code, sigungu_code, deal_date);

-- CREATE INDEX idx_sale_transactions_property_type_deal_date
--     ON sale_transactions (property_type, deal_date);

-- CREATE INDEX idx_sale_transactions_building_name
--     ON sale_transactions (building_name);

-- COMMENT ON TABLE sale_transactions IS '아파트/오피스텔/연립다세대/단독·다가구 매매 실거래가';
-- COMMENT ON COLUMN sale_transactions.id IS 'PK (surrogate key)';
-- COMMENT ON COLUMN sale_transactions.property_type IS '부동산 유형: APT/OFFICETEL/ROW_HOUSE/SINGLE_MULTI';
-- COMMENT ON COLUMN sale_transactions.house_type IS '원본 houseType (연립/다세대/단독/다가구). 아파트·오피스텔은 NULL';
-- COMMENT ON COLUMN sale_transactions.sido_code IS '시도코드(법정동코드 1~2번째 자리)';
-- COMMENT ON COLUMN sale_transactions.sigungu_code IS '시군구코드(법정동코드 3~5번째 자리). 원본 RTMS API의 sggCd(5자리)는 sido_code+sigungu_code를 합친 값';
-- COMMENT ON COLUMN sale_transactions.umd_name IS '법정동명';
-- COMMENT ON COLUMN sale_transactions.jibun IS '지번';
-- COMMENT ON COLUMN sale_transactions.building_name IS '단지/건물명 (aptNm/offiNm/mhouseNm 통합). 단독·다가구는 NULL';
-- COMMENT ON COLUMN sale_transactions.deal_date IS '계약일 (dealYear+dealMonth+dealDay 통합)';
-- COMMENT ON COLUMN sale_transactions.exclusive_use_area IS '전용면적(㎡). 단독·다가구는 NULL';
-- COMMENT ON COLUMN sale_transactions.floor IS '층. 단독·다가구는 NULL';
-- COMMENT ON COLUMN sale_transactions.build_year IS '건축년도';
-- COMMENT ON COLUMN sale_transactions.total_floor_area IS '연면적(㎡). 단독·다가구만';
-- COMMENT ON COLUMN sale_transactions.plottage_area IS '대지면적(㎡). 단독·다가구 매매만';
-- COMMENT ON COLUMN sale_transactions.land_area IS '대지권면적(㎡). 연립다세대 매매만';
-- COMMENT ON COLUMN sale_transactions.deal_amount IS '거래금액(원). 원본 콤마 문자열(만원 단위)을 정수 원 단위로 환산';
-- COMMENT ON COLUMN sale_transactions.dealing_type IS '거래유형(중개거래/직거래)';
-- COMMENT ON COLUMN sale_transactions.estate_agent_sigungu_name IS '중개사소재지(시군구 단위)';
-- COMMENT ON COLUMN sale_transactions.seller_type IS '매도자 구분(개인/법인/공공기관/기타)';
-- COMMENT ON COLUMN sale_transactions.buyer_type IS '매수자 구분(개인/법인/공공기관/기타)';
-- COMMENT ON COLUMN sale_transactions.cancel_deal_type IS '해제여부';
-- COMMENT ON COLUMN sale_transactions.cancel_deal_date IS '해제사유발생일';
-- COMMENT ON COLUMN sale_transactions.registration_date IS '등기일자. 아파트·연립다세대 매매만 원본에 존재';
-- COMMENT ON COLUMN sale_transactions.apartment_dong IS '아파트 동명. 아파트 매매만';
-- COMMENT ON COLUMN sale_transactions.land_leasehold_type IS '토지임대부 아파트 여부(Y/N). 아파트 매매만';
-- COMMENT ON COLUMN sale_transactions.sigungu_name IS '시군구명. 오피스텔 매매만 원본에 존재';

-- ============================================================
-- 2. rent_transactions (전월세)
-- ============================================================

CREATE TABLE rent_transactions (
    id                       BIGSERIAL PRIMARY KEY,

    -- 공통 컬럼
    property_type            VARCHAR(20)    NOT NULL,
    house_type               VARCHAR(10),
    sido_code                CHAR(2)        NOT NULL,
    sigungu_code             CHAR(3)        NOT NULL,
    umd_name                 VARCHAR(60)    NOT NULL,
    jibun                    VARCHAR(20),
    building_name            VARCHAR(100),
    deal_date                DATE           NOT NULL,
    exclusive_use_area       NUMERIC(10,4),
    floor                    SMALLINT,
    build_year               SMALLINT,

    -- 전월세 전용 컬럼
    total_floor_area         NUMERIC(10,4),
    deposit                  BIGINT         NOT NULL,
    monthly_rent             BIGINT         NOT NULL,
    contract_term            VARCHAR(20),
    contract_type            VARCHAR(10),
    renewal_right_used       VARCHAR(10),
    previous_deposit         BIGINT,
    previous_monthly_rent    BIGINT,
    sigungu_name             VARCHAR(30),
    apartment_serial_number  VARCHAR(20),
    road_address_detail      JSONB,

    CONSTRAINT chk_rent_transactions_property_type
        CHECK (property_type IN ('APT', 'OFFICETEL', 'ROW_HOUSE', 'SINGLE_MULTI')),

    CONSTRAINT uq_rent_transactions_dedup
        UNIQUE (sido_code, sigungu_code, umd_name, jibun, building_name, deal_date, floor, exclusive_use_area, deposit, monthly_rent)
);

-- CREATE INDEX idx_rent_transactions_sido_sigungu_deal_date
--     ON rent_transactions (sido_code, sigungu_code, deal_date);

-- CREATE INDEX idx_rent_transactions_property_type_deal_date
--     ON rent_transactions (property_type, deal_date);

-- CREATE INDEX idx_rent_transactions_building_name
--     ON rent_transactions (building_name);

-- COMMENT ON TABLE rent_transactions IS '아파트/오피스텔/연립다세대/단독·다가구 전월세 실거래가';
-- COMMENT ON COLUMN rent_transactions.id IS 'PK (surrogate key)';
-- COMMENT ON COLUMN rent_transactions.property_type IS '부동산 유형: APT/OFFICETEL/ROW_HOUSE/SINGLE_MULTI';
-- COMMENT ON COLUMN rent_transactions.house_type IS '원본 houseType (연립/다세대/단독/다가구). 아파트·오피스텔은 NULL';
-- COMMENT ON COLUMN rent_transactions.sido_code IS '시도코드(법정동코드 1~2번째 자리)';
-- COMMENT ON COLUMN rent_transactions.sigungu_code IS '시군구코드(법정동코드 3~5번째 자리). 원본 RTMS API의 sggCd(5자리)는 sido_code+sigungu_code를 합친 값';
-- COMMENT ON COLUMN rent_transactions.umd_name IS '법정동명';
-- COMMENT ON COLUMN rent_transactions.jibun IS '지번. 단독·다가구 전월세는 원본에 필드 자체가 없어 NULL';
-- COMMENT ON COLUMN rent_transactions.building_name IS '단지/건물명 (aptNm/offiNm/mhouseNm 통합). 단독·다가구는 NULL';
-- COMMENT ON COLUMN rent_transactions.deal_date IS '계약일 (dealYear+dealMonth+dealDay 통합)';
-- COMMENT ON COLUMN rent_transactions.exclusive_use_area IS '전용면적(㎡). 단독·다가구는 NULL';
-- COMMENT ON COLUMN rent_transactions.floor IS '층. 단독·다가구는 NULL';
-- COMMENT ON COLUMN rent_transactions.build_year IS '건축년도';
-- COMMENT ON COLUMN rent_transactions.total_floor_area IS '연면적(㎡). 단독·다가구만';
-- COMMENT ON COLUMN rent_transactions.deposit IS '보증금(원). 원본 콤마 문자열(만원 단위)을 정수 원 단위로 환산';
-- COMMENT ON COLUMN rent_transactions.monthly_rent IS '월세(원). 전세는 0';
-- COMMENT ON COLUMN rent_transactions.contract_term IS '계약기간(예: 24.09~26.09)';
-- COMMENT ON COLUMN rent_transactions.contract_type IS '계약구분(신규/갱신)';
-- COMMENT ON COLUMN rent_transactions.renewal_right_used IS '갱신요구권 사용여부';
-- COMMENT ON COLUMN rent_transactions.previous_deposit IS '종전계약 보증금(원)';
-- COMMENT ON COLUMN rent_transactions.previous_monthly_rent IS '종전계약 월세(원)';
-- COMMENT ON COLUMN rent_transactions.sigungu_name IS '시군구명. 오피스텔 전월세만 원본에 존재';
-- COMMENT ON COLUMN rent_transactions.apartment_serial_number IS '단지 일련번호. 아파트 전월세만';
-- COMMENT ON COLUMN rent_transactions.road_address_detail IS '도로명주소 상세(roadnm/roadnmsggcd/roadnmcd/roadnmseq/roadnmbcd/roadnmbonbun/roadnmbubun). 아파트 전월세에만 존재';
