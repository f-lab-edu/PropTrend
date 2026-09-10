# PropTrend

국토교통부 공공데이터 포털의 부동산 실거래 데이터와 이외에 다양한 연계 데이터를 수집·가공해 부동산 거래에 필요한 유용한 데이터와 지표를 제공하는 서비스.

## 프로젝트 목표

공공데이터로 부동산 실거래 데이터, 부동산 건축물 상세정보, 주변 편의시설 등에 대한 정보를 얻을 수 있지만, 정제되지 않은 원본 데이터는 실제로 유용하게 사용되기 힘듭니다. 프로젝트에서 개발할 PropTrend 서비스는 이러한 원본 데이터들을 수집하고 가공 및 갱신하여 부동산 관련 정보를 쉽게 접할 수 있도록 합니다.

## 프로젝트 시스템

![project_infra_structure](/prop-trend-system.drawio.png)

- 공공 데이터 API: 다양한 원본 데이터를 제공하는 API
- API Application Server: 스케줄러에 의해서 데이터 수집, 가공, 적재 작업과 다양한 비즈니스 로직을 수행하는 API
- DB: 원본 데이터를 필요에 맞게 가공하여 적재하는 데이터베이스
- WebServer: 사용자 UI 웹 페이지 제공 서버

## 핵심 기능

- 단지별 부동산 매매/전월세 실거래가 검색
  - 필터 기반 검색
  - 단지 상세 정보 (해당 단지 실거래가 추세 및 인근 편의시설, 상권 정보)
- 부동산 거래 관련 대시보드
  - 오늘의 최고가 거래, 최저가 거래, 거래건수
  - 거래액 급등/급락 단지 TOP 5
  - 거래량 급등 지역
  - 신고가 경신 단지
  - 전세가율 상위/하위 단지 랭킹
  - 인기 단지 랭킹
- 사용자 즐겨찾기 단지 신규 실거래가 알림
- 사용자 행동 로그 수집 시스템 및 홈 대시보드 위젯 재순위화

## 실행

Docker Compose로 PostgreSQL과 API 애플리케이션 서버를 함께 띄운다.

```bash
cp api/.env.example api/.env   # DATA_GO_KR_SERVICE_KEY(공공데이터포털 인증키) 입력
docker compose up -d
```

- API 서버: <http://localhost:8000> (OpenAPI 문서 <http://localhost:8000/docs>)
- PostgreSQL: `localhost:5432` (`postgres` / `postgres` / `prop_trend`)
- 테이블은 API 서버 기동 시 모델 정의대로 만들어진다. 없는 테이블만 만들 뿐이라 컬럼 변경은 반영되지 않는다.
- `api/.env`의 `DATABASE_URL`은 컴포즈가 컨테이너 네트워크 주소로 덮어쓰므로 로컬 실행에만 쓰인다.

데이터 갱신은 매일 03시(KST) 스케줄러가 돌린다. 기다리지 않고 지금 돌리려면 강제 실행 API를 쓴다.

```bash
curl -X POST localhost:8000/jobs/refresh   # 백그라운드로 시작(202), 실행 중이면 409
curl localhost:8000/jobs/refresh           # 진행 상황과 마지막 실행 결과

docker compose logs -f api                 # 갱신 로그
docker compose down                        # 중지(-v를 붙이면 DB 데이터까지 삭제)
```

DB만 컨테이너로 띄우고 서버는 로컬에서 실행할 수도 있다.

```bash
docker compose up -d postgres
cd api && uv run uvicorn src.main:app --reload
```

## 프로젝트 이슈

추가 예정

## 라이선스
