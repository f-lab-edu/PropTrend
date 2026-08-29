# 행정안전부 행정·안전 공공데이터 Open API 활용가이드

## 1. 서비스 명세

### 1.1 API 서비스 개요

| 항목 | 내용 |
|---|---|
| API명(영문) | `StanReginCd` |
| API명(국문) | 행정안전부_행정표준코드_법정동코드 |
| API 설명 | 행정표준코드관리시스템에서 제공중인 법정동코드 정보 |

### 1.2 보안 적용 / 기술 수준

| 항목 | 적용 여부 |
|---|---|
| 서비스 인증/권한 | ServiceKey ✅ / 인증서(GPKI‧NPKI) ❌ / Basic(ID‧PW) ❌ / 없음 ❌ |
| 메시지 레벨 암호화 | 전자서명 ❌ / 암호화 ❌ / 없음 ✅ |
| 전송 레벨 암호화 | SSL ❌ / 없음 ✅ |
| 인터페이스 표준 | SOAP 1.2 ❌ / **REST(GET) ✅** / RSS 1.0‧2.0 ❌ / Atom 1.0 ❌ / 기타 ❌ |
| 교환 데이터 표준 (중복 선택 가능) | XML ✅ / JSON ✅ / MIME ❌ / MTOM ❌ |

### 1.3 배포 정보

| 항목 | 내용 |
|---|---|
| 서비스 URL | `http://apis.data.go.kr/1741000/StanReginCd` |
| 서비스 URL (WSDL/WADL) | `http://apis.data.go.kr/1741000/StanReginCd?_wadl&type=xml` |
| 서비스 버전 | 1.0 |
| 서비스 시작일 | 2021-04-01 |
| 서비스 배포일 | 2021-04-01 |
| 서비스 이력 | 2021-04-01 : 서비스 시작 |
| 메시지 교환 유형 | Request-Response ✅ / Publish-Subscribe ❌ / Fire-and-Forget ❌ / Notification ❌ |
| 서비스 제공자 | 장태호 / 정보통계담당관 / 044-205-1644 / thjang2414@korea.kr |
| 데이터 갱신주기 | 수시 |

## 2. 상세기능 목록

| 번호 | API명(국문) | 상세기능명(영문) | 상세기능명(국문) |
|---|---|---|---|
| 1 | 행정안전부_행정표준코드_법정동코드 | `getStanReginCdList` | 법정동코드 조회 |

## 3. 상세기능 내역

### 3.1 [법정동코드 조회] 상세기능 정보

| 항목 | 내용 |
|---|---|
| 상세기능 번호 | 1 |
| 상세기능 유형 | 조회(목록) |
| 상세기능명(국문) | 법정동코드 조회 |
| 상세기능 설명 | 법정동코드 정보의 지역코드, 시도코드, 읍면동코드, 리코드, 지역주소명 등을 조회한다. |
| Call Back URL | `http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList` |
| 최대 메시지 사이즈 | 300 bytes |
| 평균 응답 시간 | 500 ms |
| 초당 최대 트랜잭션 | 30 tps |

### 3.2 요청 메시지 명세

| 항목명(영문) | 항목명(국문) | 항목크기 | 항목구분 | 샘플데이터 | 항목설명 |
|---|---|---|---|---|---|
| `ServiceKey` | 인증키 | 100 | 1 | 인증키 (URL Encode) | 공공데이터포털에서 발급받은 인증키 |
| `type` | 호출문서(xml, json) | 4 | 1 | xml | 호출문서(xml, json), default: xml |
| `pageNo` | 페이지 위치 | 4 | 1 | 1 | 페이지번호 (default: 1) |
| `numOfRows` | 페이지 당 요청 숫자 | 4 | 1 | 3 | 한 페이지 결과 수 (default: 10) |
| `flag` | 신규 API | 2 | 1 | Y | 신규 API |
| `locatadd_nm` | 지역주소명 | 50 | 0 | 서울특별시 | 지역주소명 |

> ※ 항목구분: 필수(1), 옵션(0), 1건 이상 복수건(1..n), 0건 또는 복수건(0..n)

### 3.3 응답 메시지 명세

**공통 헤더**

| 항목명(영문) | 항목명(국문) | 항목크기 | 항목구분 | 샘플데이터 | 항목설명 |
|---|---|---|---|---|---|
| `totalCount` | 전체 결과 수 | 4 | 1 | 1 | 전체 결과 수 |
| `numOfRows` | 한 페이지결과 수 | 4 | 1 | 3 | 한 페이지결과 수 |
| `pageNo` | 페이지 번호 | 4 | 1 | 1 | 페이지 번호 |
| `type` | 수신 문서형식 | 4 | 1 | XML | 수신 문서형식 |
| `resultCode` | 결과코드 | 10 | 1 | INFO-0 | 결과코드 |
| `resultMsg` | 결과메세지 | 50 | 1 | NOMAL SERVICE | 결과메세지 |

**개별 데이터 (`row`)**

| 항목명(영문) | 항목명(국문) | 항목크기 | 항목구분 | 샘플데이터 | 항목설명 |
|---|---|---|---|---|---|
| `region_cd` | 지역코드 | 10 | 1 | 1100000000 | 지역코드 |
| `sido_cd` | 시도코드 | 2 | 0 | 11 | 시도코드 |
| `sgg_cd` | 시군구코드 | 3 | 0 | 000 | 시군구코드 |
| `umd_cd` | 읍면동코드 | 3 | 0 | 000 | 읍면동코드 |
| `ri_cd` | 리코드 | 2 | 0 | 00 | 리코드 |
| `locatjumin_cd` | 지역코드_주민 | 10 | 0 | 1100000000 | 지역코드_주민 |
| `locatjijuk_cd` | 지역코드_지적 | 10 | 0 | 1100000000 | 지역코드_지적 |
| `locatadd_nm` | 지역주소명 | 50 | 0 | 서울특별시 | 지역주소명 |
| `locat_order` | 서열 | 3 | 0 | 11 | 서열 |
| `locat_rm` | 비고 | 200 | 0 | (없음) | 비고 |
| `locathigh_cd` | 상위지역코드 | 10 | 0 | 0000000000 | 상위지역코드 |
| `locallow_nm` | 최하위지역명 | 20 | 0 | 서울특별시 | 최하위지역명 |
| `adpt_de` | 생성일 | 8 | 0 | 20000101 | 생성일 |

> ※ 항목구분: 필수(1), 옵션(0), 1건 이상 복수건(1..n), 0건 또는 복수건(0..n)

### 3.4 요청/응답 메시지 예제

**요청 메시지**

```
http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList?ServiceKey=인증키&type=xml&pageNo=1&numOfRows=3&flag=Y&locatadd_nm=서울특별시
```

> 단, 익스플로러에서 확인 시 파라미터 입력이 한글인 경우 UTF-8로 인코딩 필요

**응답 메시지**

```xml
<StanReginCd>
    <head>
        <totalCount>1</totalCount>
        <numOfRows>3</numOfRows>
        <pageNo>1</pageNo>
        <type>XML</type>
        <RESULT>
            <resultCode>INFO-0</resultCode>
            <resultMsg>NOMAL SERVICE</resultMsg>
        </RESULT>
    </head>
    <row>
        <region_cd>1100000000</region_cd>
        <sido_cd>11</sido_cd>
        <sgg_cd>000</sgg_cd>
        <umd_cd>000</umd_cd>
        <ri_cd>00</ri_cd>
        <locatjumin_cd>1100000000</locatjumin_cd>
        <locatjijuk_cd>1100000000</locatjijuk_cd>
        <locatadd_nm>서울특별시</locatadd_nm>
        <locat_order>11</locat_order>
        <locat_rm/>
        <locathigh_cd>0000000000</locathigh_cd>
        <locallow_nm>서울특별시</locallow_nm>
        <adpt_de>20000101</adpt_de>
    </row>
</StanReginCd>
```

## 4. Open API 에러 코드정리

| 에러코드 | 에러메시지 | 설명 |
|---|---|---|
| 290 | ERROR | 인증키가 유효하지 않습니다. 인증키가 없는 경우 홈페이지에서 인증키를 신청하십시오. |
| 310 | ERROR | 해당하는 서비스를 찾을 수 없습니다. 요청인자 중 SERVICE를 확인하십시오. |
| 333 | ERROR | 요청위치 값의 타입이 유효하지 않습니다. 요청위치 값은 정수를 입력하세요. |
| 336 | ERROR | 데이터 요청은 한번에 최대 1,000건을 넘을 수 없습니다. |
| 337 | ERROR | 일별 트래픽 제한을 넘은 호출입니다. 오늘은 더이상 호출할 수 없습니다. |
| 500 | ERROR | 서버 오류입니다. 지속적으로 발생시 홈페이지로 문의(Q&A) 바랍니다. |
| 600 | ERROR | 데이터베이스 연결 오류입니다. 지속적으로 발생시 홈페이지로 문의(Q&A) 바랍니다. |
| 601 | ERROR | SQL 문장 오류입니다. 지속적으로 발생시 홈페이지로 문의(Q&A) 바랍니다. |
| 0 | INFO | 정상 처리되었습니다. |
| 300 | INFO | 관리자에 의해 인증키 사용이 제한되었습니다. |
| 200 | INFO | 해당하는 데이터가 없습니다. |