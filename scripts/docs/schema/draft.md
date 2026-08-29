# DB 스키마 초안

국토교통부 실거래가 8종 API(아파트/오피스텔/연립다세대/단독·다가구 × 매매/전월세) 응답을
저장하기 위한 스키마 초안. 근거 분석은 `docs/data-api/` 아래 8개 API 문서 비교 결과.

> 현재 프로젝트에는 아직 DB 엔진이 정해져 있지 않음(`pyproject.toml`에 DB 드라이버 없음,
> `collect_rtms.py`는 원본 JSON을 로컬/S3에 적재하는 단계). 아래 DDL은 PostgreSQL 문법으로
> 작성했으며, 엔진이 확정되면 타입만 맞춰주면 되는 수준으로 최대한 표준 SQL에 가깝게 유지함.

## 1. 설계 원칙

- **매매/전월세는 테이블을 분리한다.** 두 그룹은 필드 집합이 근본적으로 다르고(거래금액 vs
  보증금·월세, 해제여부 vs 갱신요구권 등), 합치면 컬럼의 절반이 항상 NULL이 된다.
- **부동산 유형(아파트/오피스텔/연립다세대/단독·다가구)은 한 테이블 안에서
  `property_type` 컬럼으로 구분한다.** 4개 유형 API가 공유하는 필드가 많고, 실거래가
  분석 관점에서는 유형을 넘나드는 조회(예: 특정 동네의 모든 매매 내역)가 잦을 것으로
  예상되기 때문. 유형별로만 존재하는 필드는 nullable 컬럼으로 둔다.
- **지역코드는 `sido_code`(시도)/`sigungu_code`(시군구) 두 컬럼으로 분리한다.** 행정안전부
  법정동코드는 10자리가 시도(2)+시군구(3)+읍면동(3)+리(2)로 고정 구성되고, RTMS 8종 API가
  요구하는 5자리 `LAWD_CD`는 그중 앞 5자리(시도+시군구)다 — 즉 5자리는 2+3으로만 나뉜다
  (근거는 문서 하단 참고자료 참조). 시군구 3자리를 다시 "시/군 2자리 + 구 1자리"로
  나누는 공식 규칙은 없다: 수원·성남처럼 일반구를 둔 시는 구 코드가 111/113/115/117처럼
  연속 부여돼 그렇게 보일 수 있지만, 서울 등 자치구 체계(종로구 110, 중구 140 …)에서는
  성립하지 않는 우연일 뿐이라 구 단위 세 번째 컬럼은 만들지 않는다. 시도명/시군구명이
  필요하면 이미 수집한 `legal_dong_code` 참조 데이터와 `(sido_code, sigungu_code)`로 조인해서
  가져오는 편이 안전하다.
- **계약일은 `dealYear`/`dealMonth`/`dealDay` 3개 필드를 하나의 `DATE` 컬럼으로 합친다.**
  쿼리(기간 필터, 정렬)가 원본 3분할 구조보다 훨씬 쉬워진다.
- **금액 필드는 정수(원 단위)로 정규화한다.** 원본 응답은 `"36,900"`처럼 콤마가 포함된
  문자열이고 단위는 만원이다. 적재 시 콤마 제거 → 정수 변환 → ×10,000 하여 원 단위
  `BIGINT`로 저장한다(만원 단위로 저장하지 않는 이유: 값이 항상 만원 단위로 딱 떨어지지
  않을 수 있는 유형이 섞여 있어 원 단위가 더 안전).
- **빈 태그(`<field></field>`)는 적재 전에 `NULL`로 정규화한다.**
- **단지/건물명 컬럼을 통합한다.** 원본은 아파트 `aptNm`, 오피스텔 `offiNm`, 연립다세대
  `mhouseNm`으로 필드명이 제각각이지만 의미가 같으므로 `building_name` 하나로 합친다.
  단독·다가구는 건물명 개념이 없어 NULL.
- **단 하나의 API에만 존재하고 구조가 깊은 필드 묶음은 JSONB로 격리한다.** 아파트 전월세
  전용 도로명주소 상세 7종(`roadnm`, `roadnmsggcd`, `roadnmcd`, `roadnmseq`, `roadnmbcd`,
  `roadnmbonbun`, `roadnmbubun`)이 여기 해당. 나머지 테이블 전체에서 이 7개 컬럼은 거의
  항상 NULL이 되므로 컬럼을 늘리는 대신 `road_address_detail JSONB` 하나로 묶는다.
- **거래 고유 ID가 원본에 없다.** 공공데이터포털 응답에는 트랜잭션 PK가 없으므로,
  재수집 시 중복 적재를 막기 위한 자연키(unique constraint) 후보를 각 테이블에 정의한다.
  다만 동일 단지·같은 날·같은 층·같은 면적·같은 금액의 거래가 실제로 2건 이상 발생하면
  자연키가 충돌해 유실될 수 있다는 한계가 있음 (5절 참고).

## 2. 공통 컬럼 (두 테이블 동일)

| 컬럼 | 타입 | NULL | 설명 |
|---|---|---|---|
| `id` | `BIGSERIAL` | NOT NULL | PK (surrogate key) |
| `property_type` | `VARCHAR(20)` | NOT NULL | `APT`(아파트) / `OFFICETEL`(오피스텔) / `ROW_HOUSE`(연립다세대) / `SINGLE_MULTI`(단독·다가구) |
| `house_type` | `VARCHAR(10)` | NULL | 원본 `houseType` 그대로(`연립`/`다세대`/`단독`/`다가구`). 아파트·오피스텔은 NULL |
| `sido_code` | `CHAR(2)` | NOT NULL | 시도코드(법정동코드 1~2번째 자리). `legal_dong_code.sido_cd`와 동일 값으로 조인 가능 |
| `sigungu_code` | `CHAR(3)` | NOT NULL | 시군구코드(법정동코드 3~5번째 자리). `legal_dong_code.sgg_cd`(3자리)와 동일 값으로 조인 가능. 원본 RTMS API 응답의 `sggCd`는 이 둘을 합친 5자리임에 주의 |
| `umd_name` | `VARCHAR(60)` | NOT NULL | 법정동명 |
| `jibun` | `VARCHAR(20)` | NULL | 지번. 단독·다가구 전월세만 원본에 필드 자체가 없어 항상 NULL |
| `building_name` | `VARCHAR(100)` | NULL | 단지/건물명 (`aptNm`/`offiNm`/`mhouseNm` 통합). 단독·다가구는 NULL |
| `deal_date` | `DATE` | NOT NULL | `dealYear`+`dealMonth`+`dealDay` 통합 |
| `exclusive_use_area` | `NUMERIC(10,4)` | NULL | 전용면적(㎡). 단독·다가구는 개념이 없어 NULL |
| `floor` | `SMALLINT` | NULL | 층. 단독·다가구는 NULL |
| `build_year` | `SMALLINT` | NULL | 건축년도 |

## 3. `sale_transactions` (매매)

원본: 아파트/오피스텔/연립다세대/단독·다가구 매매 4종 API.

| 컬럼 | 타입 | NULL | 원본 필드 | 설명 |
|---|---|---|---|---|
| *(2절 공통 컬럼)* | | | | |
| `total_floor_area` | `NUMERIC(10,4)` | NULL | `totalFloorAr` | 연면적. 단독·다가구만 |
| `plottage_area` | `NUMERIC(10,4)` | NULL | `plottageAr` | 대지면적. 단독·다가구 매매만 |
| `land_area` | `NUMERIC(10,4)` | NULL | `landAr` | 대지권면적. 연립다세대 매매만 |
| `deal_amount` | `BIGINT` | NOT NULL | `dealAmount` | 거래금액(원). 콤마 제거 후 ×10,000 |
| `dealing_type` | `VARCHAR(20)` | NULL | `dealingGbn` | 거래유형(중개거래/직거래) |
| `estate_agent_sigungu_name` | `VARCHAR(100)` | NULL | `estateAgentSggNm` | 중개사소재지(시군구 단위) |
| `seller_type` | `VARCHAR(20)` | NULL | `slerGbn` | 매도자 구분(개인/법인/공공기관/기타) |
| `buyer_type` | `VARCHAR(20)` | NULL | `buyerGbn` | 매수자 구분 |
| `cancel_deal_type` | `VARCHAR(10)` | NULL | `cdealType` | 해제여부 |
| `cancel_deal_date` | `DATE` | NULL | `cdealDay` | 해제사유발생일 |
| `registration_date` | `DATE` | NULL | `rgstDate` | 등기일자. 아파트·연립다세대 매매만 존재(오피스텔·단독다가구는 원본에 필드 없음) |
| `apartment_dong` | `VARCHAR(50)` | NULL | `aptDong` | 아파트 동명. 아파트 매매만 |
| `land_leasehold_type` | `CHAR(1)` | NULL | `landLeaseholdGbn` | 토지임대부 아파트 여부(Y/N). 아파트 매매만 |
| `sigungu_name` | `VARCHAR(30)` | NULL | `sggNm` | 시군구명. 오피스텔 매매만 원본에 포함 |

**키**

- PK: `id`
- Unique(dedup key) 후보: `(sido_code, sigungu_code, umd_name, jibun, building_name, deal_date, floor, exclusive_use_area, deal_amount)`
- 인덱스: `(sido_code, sigungu_code, deal_date)`, `(property_type, deal_date)`, `building_name`

## 4. `rent_transactions` (전월세)

원본: 아파트/오피스텔/연립다세대/단독·다가구 전월세 4종 API.

| 컬럼 | 타입 | NULL | 원본 필드 | 설명 |
|---|---|---|---|---|
| *(2절 공통 컬럼)* | | | | |
| `total_floor_area` | `NUMERIC(10,4)` | NULL | `totalFloorAr` | 연면적. 단독·다가구만 |
| `deposit` | `BIGINT` | NOT NULL | `deposit` | 보증금(원). 콤마 제거 후 ×10,000 |
| `monthly_rent` | `BIGINT` | NOT NULL | `monthlyRent` | 월세(원). 전세는 0 |
| `contract_term` | `VARCHAR(20)` | NULL | `contractTerm` | 계약기간(예: `24.09~26.09`) |
| `contract_type` | `VARCHAR(10)` | NULL | `contractType` | 계약구분(신규/갱신) |
| `renewal_right_used` | `VARCHAR(10)` | NULL | `useRRRight` | 갱신요구권 사용여부 |
| `previous_deposit` | `BIGINT` | NULL | `preDeposit` | 종전계약 보증금(원) |
| `previous_monthly_rent` | `BIGINT` | NULL | `preMonthlyRent` | 종전계약 월세(원) |
| `sigungu_name` | `VARCHAR(30)` | NULL | `sggNm` | 시군구명. 오피스텔 전월세만 |
| `apartment_serial_number` | `VARCHAR(20)` | NULL | `aptSeq` | 단지 일련번호. 아파트 전월세만 |
| `road_address_detail` | `JSONB` | NULL | `roadnm` 등 7종 | 도로명주소 상세. 아파트 전월세에만 존재하는 7개 필드(`roadnm`/`roadnmsggcd`/`roadnmcd`/`roadnmseq`/`roadnmbcd`/`roadnmbonbun`/`roadnmbubun`)를 그대로 묶어서 저장 |

**키**

- PK: `id`
- Unique(dedup key) 후보: `(sido_code, sigungu_code, umd_name, jibun, building_name, deal_date, floor, exclusive_use_area, deposit, monthly_rent)`
- 인덱스: `(sido_code, sigungu_code, deal_date)`, `(property_type, deal_date)`, `building_name`

## 5. 미해결/추후 논의 사항

- **자연키 충돌 가능성**: 같은 단지·같은 날·같은 층·같은 면적·같은 금액의 거래가 실제로
  2건 이상 발생하면 위 unique 제약으로는 구분이 안 되어 하나만 남을 수 있음. 원본 API가
  거래 ID를 제공하지 않아 완전한 해결은 불가능 — 실제 중복 발생 빈도를 데이터로 확인한
  뒤 허용 여부를 결정 필요. (`source_api` 컬럼을 제외하면서 키 차원이 하나 더 줄었으므로
  충돌 가능성은 이전보다 약간 더 높아짐)
- **DB 엔진 미정**: PostgreSQL 기준으로 작성(JSONB 사용). 다른 엔진으로 정해지면 해당
  타입 대체 필요(JSONB → JSON/TEXT 등).
- **`legal_dong_code` 테이블 연동**: 시도/시군구/읍면동 전체 주소 체계가 필요하면 이미
  수집된 법정동코드 데이터(`docs/data-api/legal-dong-code.md`)를 별도 참조 테이블로
  적재하고 `(sido_code, sigungu_code)` 기준으로 조인하는 방식을 권장. `sido_code`/`sigungu_code`로
  분리해둔 덕분에 legal_dong_code의 `sido_cd`(2자리)/`sgg_cd`(3자리) 필드와 자릿수가 그대로
  맞아 별도 변환 없이 조인 가능.
- **`property_type`/`house_type` 값 집합 확정**: 위 표의 값(`APT`/`OFFICETEL`/
  `ROW_HOUSE`/`SINGLE_MULTI`)은 초안 표기이며 실제 구현 시 ENUM 타입 사용 여부와 함께
  확정 필요.

## 6. 참고자료 (법정동코드 구조 확인)

- [지역 관련 코드 종류 및 연결 방법](https://velog.io/@jt2_92/%EC%A7%80%EC%97%AD-%EA%B5%AC%EC%97%AD-%EA%B4%80%EB%A0%A8-%EC%BD%94%EB%93%9C-%EC%A2%85%EB%A5%98-%EB%B0%8F-%EC%97%B0%EA%B2%B0-%EB%B0%A9%EB%B2%95)
- [행정동코드, 법정동코드, 행정구역코드 뽀개기 - Haram Park](https://blog.harampark.com/blog/korea-admin-codes/)
- [주소 데이터 활용을 위한 우리나라 행정구역 이해하기 - 카카오모빌리티 디벨로퍼스](https://developers.kakaomobility.com/docs/techblogs/address-structure-1/)
- 프로젝트 내 1차 자료: `docs/data-api/legal-dong-code.md` (행정안전부 API 문서, `sido_cd` 2자리 / `sgg_cd` 3자리 / `umd_cd` 3자리 / `ri_cd` 2자리로 항목크기 명시)
