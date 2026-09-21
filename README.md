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

Docker Compose로 PostgreSQL과 API 애플리케이션 서버를 띄운다.

```bash
cp api/.env.example api/.env   # DATA_GO_KR_SERVICE_KEY(공공데이터포털 인증키) 입력
docker compose up -d
```

- API 서버: <http://localhost:8000> (OpenAPI 문서 <http://localhost:8000/docs>)
- PostgreSQL: `localhost:5432` (`postgres` / `postgres` / `prop_trend`)
- 테이블은 API 서버 기동 시 모델 정의대로 만들어진다. 없는 테이블만 만들 뿐이라 컬럼 변경은 반영되지 않는다.
  `refresh_unit_states`가 새로 생겼으므로 갱신을 돌리기 전에 API 서버를 한 번 띄워야 한다.
- `api/.env`는 로컬 실행에서만 읽는다. 컴포즈로 띄운 API 서버는 접속 주소를 컴포즈에서 직접 받는다.

## 데이터 갱신

갱신 파이프라인은 API 서버와 **별개의 프로세스**로 도는, 한 번 돌고 끝나는 배치다.
컴포즈에는 올리지 않고 개발 단계에서는 명령으로 직접 띄운다.
테이블은 API 서버가 만들어 두므로 먼저 한 번 띄워야 한다.

```bash
cd api

uv run python -m src.jobs                 # 기본값(최근 2개월)으로 갱신
uv run python -m src.jobs --months 3      # 최근 3개월
uv run python -m src.jobs --help          # 인자 확인
```

| 인자 | 설명 |
|---|---|
| `--months N` | 이번 달부터 거슬러 갱신할 개월 수 (1~24, 기본 2) |
| `--concurrency N` | 동시에 처리할 갱신 단위 수 (1~8, 기본 4) |
| `--only 좌표` | 이 갱신 단위만 다시 돌린다. 여러 번 줄 수 있다 |
| `--retry-failed` | 상태 표에 남은 미완 단위를 다시 돌린다 |
| `--with-collect` | 위 둘을 오픈API 수집부터 다시 돌린다 (기본은 정제만) |

접속 정보는 `api/.env`의 `DATABASE_URL`을 읽는다. 컴포즈로 띄운 DB를 그대로 쓰면 된다.

### 실패한 단위 다시 돌리기

갱신 단위는 `api_id:시군구코드:계약년월`로 가리킨다.

```bash
uv run python -m src.jobs --retry-failed                  # 밀린 정제만 (오픈API 미사용)
uv run python -m src.jobs --retry-failed --with-collect   # 수집부터 다시
uv run python -m src.jobs --only apart_sale:11110:202602  # 좌표를 직접 지정
```

무엇이 밀렸는지는 `refresh_unit_states` 표가 들고 있다.

- **끝난 단위는 행이 없다.** 표가 비어 있는 것이 정상이고, 그 자체로 건강 지표다.
- `COLLECTED` — 응답은 bronze에 커밋됐고 정제가 안 끝났다. 오픈API 없이 다시 돌릴 수 있다.
- `FAILED` — 아무것도 커밋되지 않았다. `--with-collect`가 있어야 대상이 된다.
- `attempts`가 10을 넘으면 자동 재시도에서 빠진다. 코드를 고친 뒤 `--only`로 지목하면 된다.
- 정기 회차가 어차피 같은 구간을 다시 도므로, `--months` 창 안의 잔여분은 다음 회차에 저절로
  정리된다. 손으로 돌릴 일은 주로 창 밖 구간이나 급히 메워야 할 때다.

`--months`는 위 두 인자와 함께 쓸 수 없다. 지목 재실행은 정제 실패도 종료 코드 1로 알린다
(정기 회차는 0으로 두고 로그로만 알린다 — 전량 재실행으로는 고쳐지지 않기 때문이다).

주기 실행은 아직 붙이지 않았다. AWS EKS로 배포할 때 `CronJob`으로 이 명령을 띄우는 것이
계획이며, 같은 이미지에서 실행 명령만 바꾸면 된다.

API 서버는 `/health`만 열려 있다(컴포즈 헬스체크가 쓴다).

## 로컬 실행

DB만 컨테이너로 띄우고 API 서버를 로컬에서 돌릴 수도 있다.

```bash
docker compose up -d postgres
cd api && uv run uvicorn src.main:app --reload
```

## 로그

터미널에는 사람이 읽는 한 줄이 나간다. `LOG_FILE`을 주면 같은 내용이 JSON으로도 쌓인다.

```bash
cd api && LOG_FILE=logs/pipeline.log uv run python -m src.jobs --months 1

jq 'select(.level == "ERROR") | .error.type' api/logs/pipeline.log   # 실패 유형만
jq 'select(.stage == "unit_state_pending") | .units' api/logs/pipeline.log  # 안 끝난 단위 좌표
jq 'select(.stage == "api_summary") | .by_api' api/logs/pipeline.log        # API별 집계
```

`unit_state_pending`의 `units` 값은 `--only`에 그대로 넣을 수 있는 모양이다.

| 변수 | 기본 | 설명 |
|---|---|---|
| `LOG_FILE` | (없음) | 지정하면 그 경로에 JSON 로그를 쌓는다. 10MB마다 로테이션, 5개 보관 |
| `LOG_LEVEL` | `INFO` | 알 수 없는 값이면 `INFO`로 떨어진다 |
| `LOG_FORMAT` | `text` | `json`이면 터미널 출력도 JSON. stdout을 그대로 수집하는 환경용 |

`LOG_FILE`은 로컬 실행용이다. 나중에 컨테이너에서 파일로 남기려면 볼륨을 붙여야 하는데,
EKS에서는 stdout을 노드 에이전트가 걷어가므로 `LOG_FORMAT=json`만 주는 편이 맞다.

## 프로젝트 이슈

추가 예정

## 라이선스
