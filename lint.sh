#!/usr/bin/env bash
# SonarCloud 봇이 PR에서 보는 항목을 로컬에서 미리 확인한다.
# ruff는 api/scripts/airflow 각각의 pyproject.toml 설정을 자동으로 적용한다.
#
# 실행 도구는 모두 버전을 고정하고 설치 스크립트 실행을 막는다. 고정하지 않으면
# 매번 임의 버전이 내려받아지고 그 과정에서 setup/lifecycle 스크립트가 돈다
# (SonarCloud shell:S8541, shell:S6505).
set -uo pipefail

JSCPD_VERSION=5.2.0
SHELLCHECK_VERSION=0.11.0.1
HADOLINT_IMAGE=hadolint/hadolint:v2.12.0-alpine

status=0

# ruff는 pytest처럼 api의 dev 의존성 그룹에 고정돼 있어 uv.lock이 버전을 정한다.
# --project로 실행 환경만 api에서 가져오고 검사 대상은 저장소 전체(.)로 둔다.
echo "==> ruff format"
uv run --project api --frozen --no-build ruff format --check . || status=1

echo "==> ruff check"
uv run --project api --frozen --no-build ruff check . || status=1

echo "==> pyright (타입 체크)"
# api, scripts, airflow는 가상환경이 달라 각 프로젝트에서 따로 돈다. 검사 범위는 각 pyproject.toml에 있다.
(cd api && uv run --frozen --no-build pyright) || status=1
(cd scripts && uv run --frozen --no-build pyright) || status=1
(cd airflow && uv run --frozen --no-build pyright) || status=1

echo "==> 모델 경계 (모델·DB 계층이 웹 계층을 참조하지 않는다)"
# scripts/src/process_silver.py가 API 서버 밖에서 모델과 DB 세션을 import한다. 같은 패키지 안에 있어
# 임포트를 막는 것이 없으므로, 웹 계층으로 되돌아가는 의존이 생기지 않았는지 여기서 확인한다.
if grep -rnE --include="*.py" "fastapi|starlette|from \\.\\.?main\\b" \
    api/src/model api/src/db.py; then
    echo "위 파일은 웹 계층과 무관해야 합니다. 임포트를 걷어내세요."
    status=1
fi

echo "==> Airflow 경계 (api·scripts는 Airflow를 모르고, DAG는 api 코드를 import하지 않는다)"
# 서로 다른 가상환경이라 import가 실패하긴 하지만, 그 실패는 실행 시점에야 드러난다.
# 주석에 이름이 나오는 것은 허용하도록 import 문만 본다.
if grep -rnE --include="*.py" "^[[:space:]]*(from|import)[[:space:]]+airflow([[:space:].]|$)" api/src scripts/src; then
    echo "위 파일은 Airflow를 몰라야 합니다. 임포트를 걷어내세요."
    status=1
fi
if grep -rnE --include="*.py" "^[[:space:]]*(from|import)[[:space:]]+src([[:space:].]|$)" airflow/dags; then
    echo "DAG는 api의 src를 import하지 않습니다. 필요한 코드는 DAG 쪽으로 옮기세요."
    status=1
fi

echo "==> pytest"
# 테스트 실행기는 각 프로젝트의 dev 의존성 그룹에 고정돼 있어 uv.lock이 버전을 정한다.
(cd api && uv run --frozen --no-build pytest -q) || status=1
(cd airflow && uv run --frozen --no-build pytest -q) || status=1

echo "==> jscpd (중복 코드)"
npx --ignore-scripts --yes "jscpd@${JSCPD_VERSION}" . || status=1

echo "==> shellcheck (셸 스크립트)"
uvx --no-build --from "shellcheck-py==${SHELLCHECK_VERSION}" shellcheck lint.sh || status=1

echo "==> hadolint (Dockerfile)"
if command -v docker >/dev/null 2>&1; then
    docker run --rm -i "$HADOLINT_IMAGE" hadolint - < api/Dockerfile || status=1
    docker run --rm -i "$HADOLINT_IMAGE" hadolint - < airflow/Dockerfile || status=1
else
    echo "docker가 없어 건너뜁니다."
fi

exit $status
