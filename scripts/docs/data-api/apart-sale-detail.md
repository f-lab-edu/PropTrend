# 국토교통부 실거래가 정보 오픈API 활용 가이드
## 아파트 매매 실거래가 상세 자료

---

## Ⅰ. 서비스 명세

### 가. API 서비스 개요

| 항목 | 내용 |
|---|---|
| API명 (영문) | Detailed data on actual apartment sales prices |
| API명 (국문) | 아파트 매매 실거래가 상세 자료 |
| API 설명 | 지역코드와 기간을 설정하여 해당지역, 해당기간의 아파트 매매 상세 자료를 제공하는 아파트 매매 실거래가 상세 자료 조회 |

**API 서비스 보안 적용 기술 수준**

| 구분 | 내용 |
|---|---|
| 서비스 인증/권한 | Service Key (인증서/BASID 없음) |
| 메시지 레벨 암호화 | 전자서명/암호화 없음 |
| 전송 레벨 암호화 | SSL 없음 |
| 인터페이스 표준 | REST (GET) |
| 교환 데이터 표준 | XML (중복 선택 가능) |

**API 서비스 배포정보**

| 항목 | 내용 |
|---|---|
| 서비스 URL | `http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev` |
| 서비스 명세 URL (WSDL/WADL) | `http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev?_wadl&type=xml` |
| 서비스 버전 | 1.0 |
| 서비스 시작일 | 2024.07.17 |
| 서비스 배포일 | 2024.07.17 |
| 메시지 교환유형 | Request-Response |
| 서비스 제공자 | 한국부동산원 (운영: 박규은 / 053-663-8637) |
| 데이터 갱신주기 | 1일 1회 |

---

### 나. 상세기능 목록

| 번호 | API명 (영문) | 상세기능명 (국문) |
|---|---|---|
| 1 | getRTMSDataSvcAptTradeDev | 아파트 매매 신고 상세자료 |

---

### 다. 상세기능내역

#### a) 상세기능정보

| 항목 | 내용 |
|---|---|
| 상세기능 번호 | 1 |
| 상세기능 유형 | 조회 (자료) |
| 상세기능명 (국문) | 아파트 매매 실거래가 상세 자료 |
| 상세기능 설명 | 행정표준코드관리시스템(www.code.go.kr)의 법정동 코드 중 앞 5자리(예시: 서울 종로구 - 11110), 계약년월(예시: 201801)로 해당 지역, 해당 기간의 아파트 매매신고 상세자료 조회 |
| Call Back URL | `http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev` |
| 최대 메시지 사이즈 | 1000 bytes |
| 평균 응답 시간 | 500 ms |
| 초당 최대 트랜잭션 | 30 tps |

> ※ 항목구분: 필수(1), 옵션(0), 1건 이상 복수건(1..n), 0건 또는 복수건(0..n)

#### b) 요청 메시지 명세

| 항목명(영문) | 항목명(국문) | 항목크기 | 항목구분 | 샘플데이터 | 항목설명 |
|---|---|---|---|---|---|
| serviceKey | 인증키 | 100 | 1 | 인증키(URL Encode) | 공공데이터포털에서 발급받은 인증키 |
| pageNo | 페이지번호 | 4 | 0 | 1 | 페이지번호 |
| numOfRows | 한 페이지 결과 수 | 4 | 0 | 10 | 한 페이지 결과 수 |
| LAWD_CD | 지역코드 | 5 | 1 | 11110 | 각 지역별 코드. 행정표준코드관리시스템(www.code.go.kr)의 법정동코드 10자리 중 앞 5자리 |
| DEAL_YMD | 계약월 | 6 | 1 | 202407 | 실거래 자료의 계약년월(6자리) |

#### c) 응답 메시지 명세

> ※ 원문 표에서 umdCd와 umdNm의 샘플데이터가 서로 바뀌어 있음(응답 예제 기준 umdCd는 `17500`, umdNm은 `숭인동`). 아래 표는 원문 그대로 옮김

| 항목명(영문) | 항목명(국문) | 항목설명 | 항목크기 | 항목구분 | 샘플데이터 |
|---|---|---|---|---|---|
| resultCode | 결과코드 | 결과코드 | 3 | 1 | 000 |
| resultMsg | 결과메세지 | 결과메세지 | 100 | 1 | OK |
| numOfRows | 한 페이지 결과 수 | 한 페이지 결과 수 | 4 | 1 | 10 |
| pageNo | 페이지 번호 | 페이지 번호 | 4 | 1 | 1 |
| totalCount | 전체 결과 수 | 전체 결과 수 | 4 | 1 | 40 |
| sggCd | 법정동시군구코드 | 법정동시군구코드 | 5 | 1 | 11110 |
| umdCd | 법정동읍면동코드 | 법정동읍면동코드 | 5 | 1 | 숭인동 |
| landCd | 법정동지번코드 | 법정동지번코드 | 1 | 0 | 1 |
| bonbun | 법정동본번코드 | 법정동본번코드 | 4 | 0 | 0202 |
| bubun | 법정동부번코드 | 법정동부번코드 | 4 | 0 | 0003 |
| roadNm | 도로명 | 도로명 | 100 | 0 | 종로66길 |
| roadNmSggCd | 도로명시군구코드 | 도로명시군구코드 | 5 | 0 | 11110 |
| roadNmCd | 도로명코드 | 도로명코드 | 7 | 0 | 4100372 |
| roadNmSeq | 도로명일련번호코드 | 도로명일련번호코드 | 2 | 0 | 01 |
| roadNmbCd | 도로명지상지하코드 | 도로명지상지하코드 | 1 | 0 | 0 |
| roadNmBonbun | 도로명건물본번호코드 | 도로명건물본번호코드 | 5 | 0 | 00028 |
| roadNmBubun | 도로명건물부번호코드 | 도로명건물부번호코드 | 5 | 0 | 00000 |
| umdNm | 법정동 | 법정동 | 60 | 1 | 17500 |
| aptNm | 단지명 | 단지명 | 100 | 1 | 종로중흥S클래스 |
| jibun | 지번 | 지번 | 20 | 0 | 202-3 |
| excluUseAr | 전용면적 | 전용면적 | 22 | 0 | 17.811 |
| dealYear | 계약년도 | 계약년도 | 4 | 1 | 2024 |
| dealMonth | 계약월 | 계약월 | 2 | 1 | 7 |
| dealDay | 계약일 | 계약일 | 2 | 1 | 23 |
| dealAmount | 거래금액 | 거래금액(만원) | 40 | 1 | 12,000 |
| floor | 층 | 층 | 10 | 0 | 10 |
| buildYear | 건축년도 | 건축년도 | 4 | 0 | 2013 |
| aptSeq | 단지 일련번호 | 단지 일련번호 | 20 | 1 | 11110-2339 |
| cdealType | 해제여부 | 해제여부 | 1 | 0 | |
| cdealDay | 해제사유발생일 | 해제사유발생일 | 8 | 0 | |
| dealingGbn | 거래유형 | 거래유형(중개및직거래여부) | 10 | 0 | 중개거래 |
| estateAgentSggNm | 중개사소재지 | 중개사소재지(시군구단위) | 3000 | 0 | 서울 종로구 |
| rgstDate | 등기일자 | 등기일자 | 8 | 0 | |
| aptDong | 아파트 동명 | 아파트 동명 | 400 | 0 | |
| slerGbn | 매도자 | 거래주체정보(개인/법인/공공기관/기타) | 100 | 0 | 개인 |
| buyerGbn | 매수자 | 거래주체정보(개인/법인/공공기관/기타) | 100 | 0 | 개인 |
| landLeaseholdGbn | 토지임대부 아파트 여부 | 토지임대부 아파트 여부 | 1 | 0 | N |

#### d) 요청/응답 메시지 예제

**요청 메시지**

```
https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev?serviceKey=서비스키&LAWD_CD=11110&DEAL_YMD=202407&pageNo=1&numOfRows=1
```

**응답 메시지**

```xml
<response>
  <header>
    <resultCode>000</resultCode>
    <resultMsg>OK</resultMsg>
  </header>
  <body>
    <items>
      <item>
        <aptDong></aptDong>
        <aptNm>종로중흥S클래스</aptNm>
        <aptSeq>11110-2339</aptSeq>
        <bonbun>0202</bonbun>
        <bubun>0003</bubun>
        <buildYear>2013</buildYear>
        <buyerGbn>개인</buyerGbn>
        <cdealDay></cdealDay>
        <cdealType></cdealType>
        <dealAmount>12,000</dealAmount>
        <dealDay>23</dealDay>
        <dealMonth>7</dealMonth>
        <dealYear>2024</dealYear>
        <dealingGbn>중개거래</dealingGbn>
        <estateAgentSggNm>서울 종로구</estateAgentSggNm>
        <excluUseAr>17.811</excluUseAr>
        <floor>10</floor>
        <jibun>202-3</jibun>
        <landCd>1</landCd>
        <landLeaseholdGbn>N</landLeaseholdGbn>
        <rgstDate></rgstDate>
        <roadNm>종로66길</roadNm>
        <roadNmBonbun>00028</roadNmBonbun>
        <roadNmBubun>00000</roadNmBubun>
        <roadNmCd>4100372</roadNmCd>
        <roadNmSeq>01</roadNmSeq>
        <roadNmSggCd>11110</roadNmSggCd>
        <roadNmbCd>0</roadNmbCd>
        <sggCd>11110</sggCd>
        <slerGbn>개인</slerGbn>
        <umdCd>17500</umdCd>
        <umdNm>숭인동</umdNm>
      </item>
    </items>
    <numOfRows>1</numOfRows>
    <pageNo>1</pageNo>
    <totalCount>40</totalCount>
  </body>
</response>
```

---

## Ⅱ. OpenAPI 에러 코드정리

| code | 코드값 | 설명 | 조치방안 |
|---|---|---|---|
| 01 | Application Error | 제공기관 서비스 제공 상태가 원활하지 않습니다. | 서비스 제공기관의 관리자에게 문의하시기 바랍니다. |
| 02 | DB Error | 제공기관 서비스 제공 상태가 원활하지 않습니다. | 서비스 제공기관의 관리자에게 문의하시기 바랍니다. |
| 03 | No Data | 데이터없음 | 에러 |
| 04 | HTTP Error | 제공기관 서비스 제공 상태가 원활하지 않습니다. | 서비스 제공기관의 관리자에게 문의하시기 바랍니다. |
| 05 | service time out | 제공기관 서비스 제공 상태가 원활하지 않습니다. | 서비스 제공기관의 관리자에게 문의하시기 바랍니다. |
| 10 | 잘못된 요청 파라미터 에러 | OpenApi 요청시 ServiceKey 파라미터가 없음 | OpenAPI 요청 값에서 ServiceKey 파라미터가 누락되었습니다. OpenAPI 요청 URL을 확인하시기 바랍니다. |
| 11 | 필수 요청 파라미터가 없음 | 요청하신 OpenApi의 필수 파라미터가 누락되었습니다. | 기술문서를 다시 한 번 확인하시어 주시기 바랍니다. |
| 12 | 해당 오픈 API 서비스가 없거나 폐기됨 | OpenApi 호출시 URL이 잘못됨 | 제공기관 관리자에게 폐기된 서비스인지 확인합니다. 폐기된 서비스가 아니면 개발가이드에서 OpenApi 요청 URL을 다시 확인하시기 바랍니다. |
| 20 | 서비스 접근 거부 | 활용승인이 되지 않은 OpenApi 호출 | OpenApi활용 신청정보의 승인상태를 확인하시기 바랍니다. 활용신청에 대해 제공기관 담당자가 확인 후 '승인'이후부터 사용할 수 있습니다. 신청 후 2~3일이 소요되고 결과는 회원가입 시 등록한 e-mail로 발송합니다. |
| 22 | 서비스 요청 제한 횟수 초과 에러 | 일일 활용건수가 초과함 (활용건수 증가 필요) | OpenAPI 활용신청정보의 서비스 상세기능별 일일트래픽량을 확인하시기 바랍니다. 개발계정의 경우 제공기관에서 정의한 트래픽을 초과하여 활용할 수 없습니다. 운영계정의 경우 변경신청을 통해서 일일트래픽량을 변경할 수 있습니다. |
| 30 | 등록되지 않은 서비스키 | 잘못된 서비스키를 사용하였거나 서비스키를 URL 인코딩하지 않음 | OpenAPI 활용신청정보의 발급받은 서비스키를 다시 확인하시기 바랍니다. 서비스키 값이 같다면 서비스키가 URL 인코딩 되었는지 다시 확인하시기 바랍니다. |
| 31 | 기간 만료된 서비스키 | OpenApi 사용기간이 만료됨 (활용연장신청 후 사용가능) | OpenAPI 활용신청정보의 활용기간을 확인합니다. 활용기간이 지난 서비스는 이용할 수 없으며 연장신청을 통해 승인받은 후 다시 이용이 가능합니다. |
| 32 | 등록되지 않은 도메인명 또는 IP 주소 | 활용신청한 서버의 IP와 실제 OpenAPI 호출한 서버가 다를 경우 | OpenAPI 활용신청정보의 등록된 도메인명이나 IP주소를 다시 확인합니다. IP나 도메인의 정보를 변경하기 위해 변경신청을 할 수 있습니다. |

---

## 참고: OPEN API 코드 신구대조표

**아파트매매 실거래 상세자료 (구 API) → 아파트 매매 실거래가 상세자료 (신규 API)**

| 구 컬럼명 | 구 항목명 | 신 컬럼명 | 신 항목명 |
|---|---|---|---|
| sggcd | 법정동시군구코드 | sggCd | 법정동시군구코드 |
| umdcd | 법정동읍면동코드 | umdCd | 법정동읍면동코드 |
| landcd | 법정동지번코드 | landCd | 법정동지번코드 |
| bonbun | 법정동본번코드 | bonbun | 법정동본번코드 |
| bubun | 법정동부번코드 | bubun | 법정동부번코드 |
| roadnm | 도로명 | roadNm | 도로명 |
| roadnmsggcd | 도로명시군구코드 | roadNmSggCd | 도로명시군구코드 |
| roadnmcd | 도로명코드 | roadNmCd | 도로명코드 |
| roadnmseq | 도로명일련번호코드 | roadNmSeq | 도로명일련번호코드 |
| roadnmbcd | 도로명지상지하코드 | roadNmbCd | 도로명지상지하코드 |
| roadnmbonbun | 도로명건물본번호코드 | roadNmBonbun | 도로명건물본번호코드 |
| roadnmbubun | 도로명건물부번호코드 | roadNmBubun | 도로명건물부번호코드 |
| umdnm | 법정동 | umdNm | 법정동 |
| aptname | 아파트 | aptNm | 단지명 |
| jibun | 지번 | jibun | 지번 |
| excluusear | 전용면적 | excluUseAr | 전용면적 |
| dealyear | 년 | dealYear | 계약년도 |
| dealmonth | 월 | dealMonth | 계약월 |
| dealday | 일 | dealDay | 계약일 |
| dealamount | 거래금액 | dealAmount | 거래금액(만원) |
| floor | 층 | floor | 층 |
| buildyear | 건축년도 | buildYear | 건축년도 |
| aptSeq | 일련번호 | aptSeq | 단지 일련번호 |
| cdealtype | 해제여부 | cdealType | 해제여부 |
| cdealday | 해제사유발생일 | cdealDay | 해제사유발생일 |
| reqgbn | 거래유형 | dealingGbn | 거래유형(중개및직거래여부) |
| rdealerlawdnm | 중개사소재지 | estateAgentSggNm | 중개사소재지(시군구단위) |
| rgstdate | 등기일자 | rgstDate | 등기일자 |
| aptdong | 동 | aptDong | 아파트 동명 |
| slergbn | 매도자 | slerGbn | 거래주체정보_매도자(개인/법인/공공기관/기타) |
| buyergbn | 매수자 | buyerGbn | 거래주체정보_매수자(개인/법인/공공기관/기타) |
| hllandgbn | 토지임대부 아파트 여부 | landLeaseholdGbn | 토지임대부 아파트 여부 |
