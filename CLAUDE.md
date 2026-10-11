# Project instructions

## Command

- 코드 포맷팅: `uv run ruff format .`
- 코드 린팅: `uv run ruff check --fix .`
- 타입 체크: `api/`와 `scripts/`에서 각각 `uv run pyright` — 두 프로젝트는 가상환경이 달라 따로 실행합니다.
- 전체 검사(포맷·린트·타입·중복): `./lint.sh` — SonarCloud 봇이 PR에서 보는 항목을 미리 확인합니다. PR마다 GitHub Actions(`.github/workflows/lint.yml`)에서도 실행됩니다.
- 테스트: `uv run pytest`
- 코드를 수정한 뒤에는 반드시 포맷팅과 린팅을 실행해 통과를 확인합니다.

## Project structure

- `api/src/jobs`는 API 서버와 별개 프로세스(`python -m src.jobs`)로 도는 갱신 파이프라인입니다. `backfill_*.py`는 1회성 백필 배치로 완료 후 삭제 예정입니다.
- `scripts/`는 공공데이터 원본 수집·bronze 적재용 별도 uv 프로젝트로, api를 import하지 않습니다.
- `api/src/jobs`, `api/src/model`, `api/src/db.py`는 웹 계층(fastapi, starlette, `main`)을 참조하지 않습니다. `lint.sh`가 이를 검사합니다.
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
