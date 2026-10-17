# Project instructions

## Command

- 코드 포맷팅: `uv run ruff format .`
- 코드 린팅: `uv run ruff check --fix .`
- 타입 체크: `api/`, `scripts/`, `airflow/`에서 각각 `uv run pyright` — 세 프로젝트는 가상환경이 달라 따로 실행합니다.
- 전체 검사(포맷·린트·타입·중복): `./lint.sh` — SonarCloud 봇이 PR에서 보는 항목을 미리 확인합니다. PR마다 GitHub Actions(`.github/workflows/lint.yml`)에서도 실행됩니다.
- 테스트: `api/`와 `airflow/`에서 각각 `uv run pytest` — 둘 다 로컬 Postgres(5432)가 떠 있어야 합니다. airflow의 DB 테스트는 api 가상환경으로 모델 스키마를 만듭니다.
- 코드를 수정한 뒤에는 반드시 포맷팅과 린팅을 실행해 통과를 확인합니다.

## Project structure

- `scripts/`는 공공데이터 원본 수집·bronze 적재용 별도 uv 프로젝트로, api를 import하지 않습니다. 예외로 1회성 정제 백필 `process_silver.py`는 api 가상환경에서 돌며 `api/src/model`과 `api/src/db.py`를 쓰고, 정기 갱신의 전처리 규칙(`airflow/dags/rtms/preprocess.py`)과 유형 목록(`rtms/apis.py`)을 DAG 폴더에서 import합니다. 그래서 `rtms/__init__.py`·`apis.py`·`preprocess.py`는 Airflow를 import하지 않고 표준 라이브러리만 씁니다. `lint.sh`가 이를 검사합니다.
- `airflow/`는 실거래 정기 갱신 DAG(`airflow/dags/rtms_daily.py`)의 별도 uv 프로젝트입니다. DAG 파일은 태스크 그래프만 선언하고, 오픈API 호출·전처리·SQL·적재 루프·태스크 정의는 `airflow/dags/rtms/` 패키지에 기능별로 둡니다(`.airflowignore`로 파싱 대상에서 뺍니다). DAG는 `src`를 import하지 않고 `api/src`도 Airflow를 import하지 않습니다. `lint.sh`가 이를 검사합니다.
- `airflow/dags/rtms/db.py`는 `api/src/model`의 테이블을 SQL로 직접 다룹니다. 모델을 바꾸면 함께 수정합니다. 전처리 규칙(`rtms/preprocess.py`)과 유형 목록(`rtms/apis.py`의 `RTMS_APIS`)은 정기 갱신과 `process_silver.py`가 한 벌을 같이 씁니다. 오픈API 응답 필드 기준선(`RtmsApi.known_fields`, `LEGAL_DONG_CODE_KNOWN_FIELDS`)은 `rtms/apis.py`에만 있습니다. `airflow/pyproject.toml`의 Airflow 버전과 태스크용 패키지 버전은 `airflow/Dockerfile`과 맞춥니다.
- `api/src/model`, `api/src/db.py`는 웹 계층(fastapi, starlette, `main`)을 참조하지 않습니다. `lint.sh`가 이를 검사합니다.
- `scripts/`는 `api/src/model/raw.py`, `api/src/model/load_progress.py`와 DDL을 따로 갖고 있으므로, 해당 모델이 바뀌면 스크립트도 함께 수정합니다.

## Coding instructions

- 함수와 클래스 docstring은 별도의 요구가 없으면 간단한 핵심목적만 "~한다." 평서문으로 기재해주세요.
- 주석은 코드가 무엇을 하는지가 아니라 왜 그렇게 했는지를 적습니다.
- 별도의 요구가 없다면, 함수 내부 알고리즘의 일부를 별도의 함수로 분리하지마세요.
- `api/` 아래 FastAPI 계층별 규칙은 `api/CLAUDE.md`에 있습니다.

## Commit convention

- 커밋 메시지는 `feat|fix|refactor|style|docs: 한국어 설명` 형식으로, 설명은 "~한다"로 끝맺습니다. (예: `feat: 매매 응답에 해제일을 더한다`)

## Document convention

- 별도의 요청이 없는한 `README.md`파일에 작업 내용을 옮기지 않습니다.
