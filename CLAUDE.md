# Project instructions

## Command

- 코드 포맷팅: `uv run ruff format .`
- 코드 린팅: `uv run ruff check --fix .`
- 타입 체크: `api/`와 `scripts/`에서 각각 `uv run pyright` — 두 프로젝트는 가상환경이 달라 따로 실행합니다.
- 전체 검사(포맷·린트·타입·중복): `./lint.sh` — SonarCloud 봇이 PR에서 보는 항목을 미리 확인합니다. PR마다 GitHub Actions(`.github/workflows/lint.yml`)에서도 실행됩니다.
- 테스트: `uv run pytest`
- 코드를 수정한 뒤에는 반드시 포맷팅과 린팅을 실행해 통과를 확인합니다.

## Project structure

```text
prop_trend/
├── api/                      # FastAPI 서버 + 데이터 갱신 파이프라인 (uv 프로젝트)
│   ├── src/
│   │   ├── main.py           # 앱 생성, 라우터 등록, public/ 정적 파일 서빙
│   │   ├── config.py         # 환경 설정
│   │   ├── db.py             # 비동기 엔진·세션
│   │   ├── dependencies.py   # FastAPI 의존성
│   │   ├── exceptions.py     # 도메인 예외 정의
│   │   ├── exception_handlers.py  # 전역 예외 핸들러
│   │   ├── middlewares.py
│   │   ├── logging_config.py
│   │   ├── routers/          # API 엔드포인트, 트랜잭션 경계
│   │   ├── services/         # 비즈니스 로직 (키워드 인자만 받음)
│   │   ├── schemas/          # Pydantic 요청·응답 스키마
│   │   ├── model/            # SQLAlchemy ORM 모델 (bronze 원본·정제 테이블·사용자 등)
│   │   ├── jobs/             # 갱신 파이프라인 (`python -m src.jobs`), API 서버와 별개 프로세스
│   │   │   ├── pipeline.py   # 갱신 단위(유형·시군구·계약년월) 실행 흐름
│   │   │   ├── collector.py / cleaner.py / preprocessor.py / loader.py  # 수집 → 정제 → 가공 → 적재
│   │   │   ├── state.py / lock.py / context.py  # 단위 상태 관리, advisory lock, 로그 컨텍스트
│   │   │   └── backfill_*.py # 1회성 백필 배치 (완료 후 삭제 예정)
│   │   └── docs/             # 라이브러리 참고 문서
│   └── tests/
│       ├── api/              # 라우터별 API 테스트 스위트
│       └── jobs/             # 파이프라인 단계별 테스트
├── scripts/                  # 공공데이터 원본 수집·bronze 적재용 독립 스크립트 (별도 uv 프로젝트, api를 import하지 않음)
│   ├── src/                  # 법정동 코드·실거래가 수집, 원본 적재
│   └── docs/                 # 공공데이터 API 명세, 스키마 초안
├── public/                   # 프론트엔드 정적 페이지 (HTML + 바닐라 JS), API 서버가 서빙
├── docs/
│   ├── design/               # 기능·비기능 요구사항
│   └── issues/               # 이슈 분석 노트
├── docker-compose.yml        # PostgreSQL + API 서버
└── lint.sh                   # ruff·중복·shellcheck·hadolint 및 파이프라인 경계 검사
```

- `api/src/jobs`, `api/src/model`, `api/src/db.py`는 웹 계층(fastapi, starlette, `main`)을 참조하지 않습니다. `lint.sh`가 이를 검사합니다.
- `scripts/`는 `api/src/model/raw.py`, `api/src/model/load_progress.py`와 DDL을 따로 갖고 있으므로, 해당 모델이 바뀌면 스크립트도 함께 수정합니다.


## Coding instructions

- 함수와 클래스 docstring은 별도의 요구가 없으면 간단한 핵심목적만 "~한다." 평서문으로 기재해주세요.
- 주석은 코드가 무엇을 하는지가 아니라 왜 그렇게 했는지를 적습니다.
- 별도의 요구가 없다면, 함수 내부 알고리즘의 일부를 별도의 함수로 분리하지마세요.
- 데이터베이스 세션 객체를 통한 트랜잭션 관리는 라우터 함수 계층에서 `async with session.begin():`로 관리합니다.
- 서비스 함수는 `session`을 제외한 모든 매개변수를 `*` 뒤의 키워드 인자로 받고, Pydantic·ORM 인스턴스를 통째로 받지 않습니다. 라우터는 필요한 값을 꺼내 키워드 인자로 전달합니다.
- 단순 조회·생성 서비스는 ORM 객체를 반환하고, 여러 출처를 조합하는 서비스는 응답 스키마를 만들어 반환합니다.
- 예외 처리는 FastAPI exception_handler로 전역 관리합니다. 그러므로 특별히 요구된 상황 외에는 모든 예외를 라우터 계층 바깥으로 내보냅니다.
- 예외는 `src/exceptions.py`에 `PropTrendError`를 상속하고 `status_code`를 지정해 정의합니다. `HTTPException`은 쓰지 않으며, 예외 메시지는 사용자에게 그대로 보이는 한국어로 적습니다.

### Router instructions

- 라우트 함수 이름에는 `_route` 같은 접미사를 붙이지 않고, 가져다 쓰는 서비스 함수와 겹치지 않는 이름을 짓습니다. (예: 라우트 `get_region_list` → 서비스 `get_regions`)
- 응답 타입은 `response_model=` 대신 반환 타입 어노테이션으로 지정합니다. 서비스가 ORM 객체를 반환하면 라우터에서 `XxxResponse.model_validate(...)`로 바꿔 반환합니다.
- 의존성은 `Annotated[..., Depends(...)]`, 경로의 id는 `Annotated[int, Path(ge=1)]`로 받습니다. 쿼리 파라미터는 `XxxQuery` 모델 하나로 묶어 `Annotated[XxxQuery, Query()]`로 받습니다.
- `summary`는 한국어 한 줄, `description`은 한국어 요약 뒤에 빈 줄을 두고 `- ` 목록으로 필드 의미·단위·정렬 기준·null 조건과 각 오류 상황의 상태 코드("~이면 404를 반환합니다")를 적습니다. API 동작을 바꾸면 `description`도 함께 갱신합니다.
- 서비스 기준 날짜는 `dependencies.get_today()`(KST)를 주입받아 `base_date=`로 서비스에 넘깁니다. 코드에서 만드는 시각은 `datetime.now(UTC)`를 씁니다.

### Schema·Model instructions

- 스키마는 `PropTrendCoreModel`을 상속하고 이름은 `XxxRequest`, `XxxResponse`, `XxxListResponse`, `XxxQuery`로 짓습니다.
- 목록 응답은 `ListResponse`(`items`), limit·offset 페이지 응답은 `PageResponse`(`items`·`total`·`limit`·`offset`)를 상속합니다.
- 시각 컬럼은 `DateTime(timezone=True)`로 정의합니다.
- 마이그레이션 도구 없이 기동 시 `create_all`로 없는 테이블만 만듭니다. 모델의 컬럼·인덱스를 바꾸면 기존 DB에 적용할 `ALTER` 문을 함께 안내합니다.

### Test instructions

- FastAPI의 API는 1개의 성공 케이스와 각 예외 처리 상황에 대한 테스트가 필수적으로 있어야합니다.
- API 테스트를 작성 시 API마다 클래스 기반으로 스위트를 구분합니다.
- 시드 데이터는 `api/tests/api/conftest.py`의 `seed_rows`로 스위트 내부에서 넣고 정리합니다.
- API 테스트 파일은 `pytestmark = pytest.mark.asyncio(loop_scope="session")`를 두고, 클래스 시드 픽스처는 `@pytest_asyncio.fixture(scope="class", loop_scope="session")`로 선언합니다. 세션 범위 엔진과 이벤트 루프를 맞추기 위함입니다.
- API에 대한 업데이트 진행 시 테스트 코드 변경이 필요한 경우 즉시 진행하지 않고, 어떤 부분에 어떻게 변경이 필요한지 확인을 받은 후 진행합니다.

## Commit convention

- 커밋 메시지는 `feat|fix|refactor|style|docs: 한국어 설명` 형식으로, 설명은 "~한다"로 끝맺습니다. (예: `feat: 매매 응답에 해제일을 더한다`)

## Document convention

- 별도의 요청이 없는한 `README.md`파일에 작업 내용을 옮기지 않습니다.
