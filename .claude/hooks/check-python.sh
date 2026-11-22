#!/usr/bin/env bash
# 에이전트가 응답을 마치기 전에 Python 변경분의 포맷·린트·타입·경계 검사를 강제한다.
#
# Stop·SubagentStop 훅으로 돈다. decision이 block이면 reason이 에이전트에게 돌아가 고칠 때까지 마치지 못한다.
# 편집마다 도는 PostToolUse 대신 Stop을 쓴 이유: 여러 파일을 고치는 중간의 타입 에러를 잡지 않고,
# ruff format이 에이전트가 편집 중인 파일을 바꾸지 않으며, Bash(sed·heredoc)로 한 편집도 git 상태로 잡힌다.
# pytest·jscpd·hadolint는 Postgres·docker가 필요하거나 느려서 lint.sh와 PR CI에 맡긴다.
set -uo pipefail
# 세션이 켜 둔 가상환경을 물려받으면 uv가 프로젝트마다 경고를 내므로 각 프로젝트의 .venv를 쓰게 한다.
unset VIRTUAL_ENV

input=$(cat)
# jq가 없는 환경에서도 돌도록, jq가 없으면 단순 문자열·불리언 필드만 sed로 꺼낸다.
field() {
  local key=$1
  if command -v jq >/dev/null 2>&1; then
    printf '%s' "$input" | jq -r --arg key "$key" '.[$key] // empty'
  else
    printf '%s' "$input" | sed -nE "s/.*\"$key\"[[:space:]]*:[[:space:]]*\"?([^\",}]*)\"?.*/\1/p" | head -n 1
  fi
}

# worktree에 들어가 있으면 CLAUDE_PROJECT_DIR는 원래 checkout을 가리키므로 훅 입력의 cwd로 실제 작업 위치를 찾는다.
# 다른 저장소로 옮겨 가 있으면 그곳에는 이 훅이 없으므로 원래 checkout을 검사한다.
root=$(git -C "$(field cwd)" rev-parse --show-toplevel 2>/dev/null)
if [[ -z "$root" || ! -f "$root/.claude/hooks/check-python.sh" ]]; then
  root=$CLAUDE_PROJECT_DIR
fi
cd "$root" || exit 0

# 에이전트가 고칠 수 없는 오류(검사 범위가 저장소 전체라 기존 오류도 걸린다)로 응답을 영영 못 끝내는 것을 막는다.
# 고칠 기회를 MAX_ATTEMPTS번 주고, 그 뒤에도 실패하면 종료를 허용한다.
MAX_ATTEMPTS=3
passed=.claude/.check-python-passed
# 시도 횟수는 요청 하나에 묶여야 한다. 같은 checkout의 다른 세션이나 서브에이전트가 멈출 때 이 카운터를
# 지우지 않도록 세션·이벤트·에이전트별로 파일을 나눈다.
owner="$(field session_id)-$(field hook_event_name)-$(field agent_id)"
attempts=".claude/.check-python-attempts-${owner//[^A-Za-z0-9_-]/_}"

# stop_hook_active가 true가 아니면 이 세션의 새 요청에서 처음 멈추려는 것이므로 시도 횟수를 다시 센다.
if [[ "$(field stop_hook_active)" != "true" ]]; then
  rm -f "$attempts"
fi

# Python 변경이 없으면 검사하지 않는다. 대화만 한 턴에서도 pyright가 도는 것을 막는다.
if [[ -z "$(git status --porcelain -- '*.py' '*.pyi')" ]]; then
  # 실패를 되돌려 변경이 사라진 경우도 해소된 것이므로 시도 횟수를 비운다.
  rm -f "$attempts"
  exit 0
fi

# 직전 통과 이후 바뀐 것이 없으면 다시 돌지 않는다. 사용자가 미리 고쳐둔 파일 때문에 매 턴 검사하는 것을 막는다.
# 보안 용도가 아닌 변경 감지용 지문이지만, 약한 해시 경고(SonarCloud shell:S4790)를 피하려고 SHA-256을 쓴다.
fingerprint() {
  {
    git diff HEAD -- '*.py' '*.pyi'
    while IFS= read -r -d '' f; do
      printf '%s\n' "$f"
      cat "$f"
    done < <(git ls-files -z --others --exclude-standard -- '*.py' '*.pyi')
  } | shasum -a 256 | cut -d' ' -f1
}
[[ -f "$passed" && "$(cat "$passed")" == "$(fingerprint)" ]] && exit 0

log=$(mktemp)
trap 'rm -f "$log"' EXIT
fail=0
run() {
  echo "\$ $*" >>"$log"
  "$@" >>"$log" 2>&1 || fail=1
}

# 포맷과 린트는 고칠 수 있는 것을 직접 고치고, 남은 것만 에이전트에게 돌려준다.
run uv run --project api --frozen --no-build ruff format .
run uv run --project api --frozen --no-build ruff check --fix .
# 지문은 포맷이 파일을 고친 뒤, 나머지 검사를 돌리기 전의 상태로 뜬다. 검사 도중 다른 세션이나
# 서브에이전트가 파일을 고치면 저장한 지문과 어긋나 다음 번에 다시 검사한다.
checked=$(fingerprint)
# 두 프로젝트는 가상환경이 달라 따로 돈다.
for p in api airflow; do
  run uv run --directory "$p" --frozen --no-build pyright
done

# 경계 검사는 grep이 찾아내면 실패다. 패턴은 lint.sh와 같으므로 한쪽을 바꾸면 함께 고친다.
forbid() {
  local message=$1
  shift
  echo "\$ grep -rnE $*" >>"$log"
  if grep -rnE --include="*.py" "$@" >>"$log" 2>&1; then
    echo "$message" >>"$log"
    fail=1
  fi
}
forbid "위 파일은 웹 계층과 무관해야 합니다. 임포트를 걷어내세요." \
  "fastapi|starlette|from \\.\\.?main\\b" api/src/model api/src/db.py
forbid "위 파일은 Airflow를 몰라야 합니다. 임포트를 걷어내세요." \
  "^[[:space:]]*(from|import)[[:space:]]+airflow([[:space:].]|$)" api/src
forbid "DAG는 api의 src를 import하지 않습니다. 필요한 코드는 DAG 쪽으로 옮기세요." \
  "^[[:space:]]*(from|import)[[:space:]]+src([[:space:].]|$)" airflow/dags

if [[ "$fail" -eq 0 ]]; then
  echo "$checked" >"$passed"
  rm -f "$attempts"
  exit 0
fi

count=$(($(cat "$attempts" 2>/dev/null || echo 0) + 1))
if [[ "$count" -gt "$MAX_ATTEMPTS" ]]; then
  rm -f "$attempts"
  # 에이전트를 놓아주되 사용자는 실패를 알아야 하므로 systemMessage로 화면에 띄운다.
  printf '{"systemMessage": "포맷·린트·타입·경계 검사가 %d번 막은 뒤에도 실패해 훅이 종료를 허용했습니다. ./lint.sh로 확인하세요."}\n' "$MAX_ATTEMPTS"
  exit 0
fi
echo "$count" >"$attempts"

reason="포맷·린트·타입·경계 검사가 실패했습니다($count/$MAX_ATTEMPTS). 아래 오류를 고친 뒤 마치세요.
$(tail -n 100 "$log")"
# exit 2와 stderr로 막지 않고 JSON decision으로 막는다. Claude Code는 exit 2의 stderr에 dlopen의
# "(no such file)" 같은 문구가 섞이면 훅 스크립트가 없는 것으로 오인해 막지 않고 넘어간다.
if command -v jq >/dev/null 2>&1; then
  jq -n --arg reason "$reason" '{decision: "block", reason: $reason}'
  exit 0
fi
echo "$reason" >&2
exit 2
