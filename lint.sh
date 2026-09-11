#!/usr/bin/env bash
# SonarCloud 봇이 PR에서 보는 항목을 로컬에서 미리 확인한다.
# ruff는 api/scripts 각각의 pyproject.toml 설정을 자동으로 적용한다.
set -uo pipefail

status=0

echo "==> ruff format"
uvx ruff format --check . || status=1

echo "==> ruff check"
uvx ruff check . || status=1

echo "==> jscpd (중복 코드)"
npx --yes jscpd . || status=1

exit $status
