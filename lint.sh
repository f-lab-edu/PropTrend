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
