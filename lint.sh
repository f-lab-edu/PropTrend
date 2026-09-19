#!/usr/bin/env bash
# SonarCloud 봇이 PR에서 보는 항목을 로컬에서 미리 확인한다.
# ruff는 api/scripts 각각의 pyproject.toml 설정을 자동으로 적용한다.
#
# 실행 도구는 모두 버전을 고정하고 설치 스크립트 실행을 막는다. 고정하지 않으면
# 매번 임의 버전이 내려받아지고 그 과정에서 setup/lifecycle 스크립트가 돈다
# (SonarCloud shell:S8541, shell:S6505).
set -uo pipefail

RUFF_VERSION=0.16.7
JSCPD_VERSION=5.2.0
SHELLCHECK_VERSION=0.11.0.1
HADOLINT_IMAGE=hadolint/hadolint:v2.12.0-alpine

status=0

echo "==> ruff format"
uvx --no-build "ruff@${RUFF_VERSION}" format --check . || status=1

echo "==> ruff check"
uvx --no-build "ruff@${RUFF_VERSION}" check . || status=1

echo "==> 파이프라인 경계 (파이프라인·모델 계층이 웹 계층을 참조하지 않는다)"
# 파이프라인은 API 서버와 별개 프로세스로 돈다. 같은 패키지 안에 있어 임포트를 막는
# 것이 없으므로, 웹 계층으로 되돌아가는 의존이 생기지 않았는지 여기서 확인한다.
if grep -rnE --include="*.py" "fastapi|starlette|from \\.\\.?main\\b" \
    api/src/jobs api/src/model api/src/db.py; then
    echo "위 파일은 웹 계층과 무관해야 합니다. 임포트를 걷어내세요."
    status=1
fi

echo "==> pytest"
# 테스트 실행기는 api의 dev 의존성 그룹에 고정돼 있어 uv.lock이 버전을 정한다.
(cd api && uv run --frozen --no-build pytest -q) || status=1

echo "==> jscpd (중복 코드)"
npx --ignore-scripts --yes "jscpd@${JSCPD_VERSION}" . || status=1

echo "==> shellcheck (셸 스크립트)"
uvx --no-build --from "shellcheck-py==${SHELLCHECK_VERSION}" shellcheck lint.sh || status=1

echo "==> hadolint (Dockerfile)"
if command -v docker >/dev/null 2>&1; then
    docker run --rm -i "$HADOLINT_IMAGE" hadolint - < api/Dockerfile || status=1
else
    echo "docker가 없어 건너뜁니다."
fi

exit $status
