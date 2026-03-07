#!/bin/bash

APP_BASE_URL="${APP_BASE_URL:-http://localhost:9090}"
API_BASE_URL="${API_BASE_URL:-${APP_BASE_URL%/}/api/v1}"
OTLP_HTTP_BASE_URL="${OTLP_HTTP_BASE_URL:-http://localhost:4318}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
WAIT_SECONDS="${WAIT_SECONDS:-4}"

CLICKHOUSE_CONTAINER="${CLICKHOUSE_CONTAINER:-clickhouse}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-clickhouse123}"

HTTP_STATUS=""
HTTP_BODY=""

TEAM_ID=""
API_KEY=""
TOKEN=""
TEST_RUN_ID=""

START_MS=0
END_MS=0

API_TOTAL=0
API_FAIL=0
API_REPORT_LINES=()

print_section() {
  echo ""
  echo "=========================================="
  echo "$1"
  echo "=========================================="
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1"
    exit 1
  fi
}

ensure_prereqs() {
  require_command curl
  require_command "$PYTHON_BIN"
}

now_ms() {
  "$PYTHON_BIN" -c 'import time; print(int(time.time() * 1000))'
}

now_ns() {
  "$PYTHON_BIN" -c 'import time; print(int(time.time() * 1_000_000_000))'
}

build_test_run_id() {
  "$PYTHON_BIN" -c 'import random, time; print(f"{int(time.time())}{random.randint(1000, 9999)}")'
}

json_path_or_empty() {
  local body="$1"
  local path="$2"

  JSON_INPUT="$body" "$PYTHON_BIN" - "$path" <<'PY' || true
import json
import os
import re
import sys

raw = os.environ.get("JSON_INPUT", "")
path = sys.argv[1]

try:
    obj = json.loads(raw)
except Exception:
    sys.exit(0)

for part in [p for p in path.split(".") if p]:
    match = re.fullmatch(r"([^\[]+)(?:\[(\d+)\])?", part)
    if not match:
        obj = None
        break

    key = match.group(1)
    index = match.group(2)

    if isinstance(obj, dict):
        obj = obj.get(key)
    else:
        obj = None

    if index is not None:
        idx = int(index)
        if isinstance(obj, list) and idx < len(obj):
            obj = obj[idx]
        else:
            obj = None

    if obj is None:
        break

if obj is None:
    sys.exit(0)

if isinstance(obj, bool):
    print("true" if obj else "false")
elif isinstance(obj, (dict, list)):
    print(json.dumps(obj))
else:
    print(obj)
PY
}

response_is_success() {
  local body="$1"

  JSON_INPUT="$body" "$PYTHON_BIN" - <<'PY'
import json
import os
import sys

try:
    data = json.loads(os.environ.get("JSON_INPUT", ""))
except Exception:
    sys.exit(1)

sys.exit(0 if data.get("success") is True else 1)
PY
}

response_data_non_empty() {
  local body="$1"

  JSON_INPUT="$body" "$PYTHON_BIN" - <<'PY'
import json
import os
import sys

def non_empty(value):
    if value is None:
        return False
    if isinstance(value, list):
        return len(value) > 0
    if isinstance(value, dict):
        return len(value) > 0
    if isinstance(value, str):
        return value != ""
    return True

try:
    payload = json.loads(os.environ.get("JSON_INPUT", "")).get("data")
except Exception:
    sys.exit(1)

sys.exit(0 if non_empty(payload) else 1)
PY
}

response_summary() {
  local body="$1"

  JSON_INPUT="$body" "$PYTHON_BIN" - <<'PY' || true
import json
import os

def summarize(value):
    if value is None:
        return "null"
    if isinstance(value, list):
        return f"list[{len(value)}]"
    if isinstance(value, dict):
        for key in ("logs", "traces", "buckets", "before", "after", "contextLogs", "points", "values"):
            nested = value.get(key)
            if isinstance(nested, list):
                return f"object[{key}={len(nested)}]"
        keys = list(value.keys())[:4]
        suffix = ",".join(keys)
        return f"object[{len(value)}:{suffix}]"
    return type(value).__name__

raw = os.environ.get("JSON_INPUT", "")
try:
    data = json.loads(raw)
except Exception:
    print("non-json")
    raise SystemExit(0)

print(f"success={data.get('success')} data={summarize(data.get('data'))}")
PY
}

response_error_summary() {
  local body="$1"
  local code
  local message

  code="$(json_path_or_empty "$body" "error.code")"
  message="$(json_path_or_empty "$body" "error.message")"

  if [[ -n "$code" || -n "$message" ]]; then
    echo "error=${code:-unknown}:${message:-unknown}"
  fi
}

perform_curl() {
  local method="$1"
  local url="$2"
  local data="$3"
  shift 3

  local tmp
  tmp="$(mktemp)"

  local status=""
  if [[ -n "$data" ]]; then
    status="$(curl -sS -o "$tmp" -w "%{http_code}" -X "$method" "$url" "$@" --data "$data" || true)"
  else
    status="$(curl -sS -o "$tmp" -w "%{http_code}" -X "$method" "$url" "$@" || true)"
  fi

  HTTP_STATUS="${status:-000}"
  if [[ -f "$tmp" ]]; then
    HTTP_BODY="$(cat "$tmp")"
    rm -f "$tmp"
  else
    HTTP_BODY=""
  fi
}

api_post_json() {
  local path="$1"
  local payload="$2"
  perform_curl "POST" "${API_BASE_URL%/}${path}" "$payload" \
    -H "Content-Type: application/json"
}

authed_api_get() {
  local path="$1"
  perform_curl "GET" "${API_BASE_URL%/}${path}" "" \
    -H "Authorization: Bearer ${TOKEN}" \
    -H "X-Team-Id: ${TEAM_ID}"
}

otlp_post_json() {
  local path="$1"
  local payload="$2"
  perform_curl "POST" "${OTLP_HTTP_BASE_URL%/}${path}" "$payload" \
    -H "Content-Type: application/json" \
    -H "x-api-key: ${API_KEY}"
}

require_last_request_ok() {
  local action="$1"
  if [[ "$HTTP_STATUS" != "200" ]] || ! response_is_success "$HTTP_BODY"; then
    echo "Failed to ${action}"
    echo "HTTP status: ${HTTP_STATUS}"
    echo "${HTTP_BODY}"
    exit 1
  fi
}

bootstrap_test_identity() {
  local suite="$1"
  ensure_prereqs

  TEST_RUN_ID="$(build_test_run_id)"

  local team_name="Codex ${suite} ${TEST_RUN_ID}"
  local team_slug="codex-${suite}-${TEST_RUN_ID}"
  local org_name="Codex ${suite} Org ${TEST_RUN_ID}"
  local user_email="codex-${suite}-${TEST_RUN_ID}@example.com"
  local user_password="CodexPass123!"

  print_section "Bootstrap ${suite} identity"
  echo "API base: ${API_BASE_URL}"
  echo "OTLP base: ${OTLP_HTTP_BASE_URL}"

  api_post_json "/teams" "$(TEAM_NAME="$team_name" TEAM_SLUG="$team_slug" ORG_NAME="$org_name" "$PYTHON_BIN" - <<'PY'
import json
import os

print(json.dumps({
    "team_name": os.environ["TEAM_NAME"],
    "slug": os.environ["TEAM_SLUG"],
    "description": "Codex telemetry smoke test team",
    "org_name": os.environ["ORG_NAME"],
}))
PY
)"
  require_last_request_ok "create team"

  TEAM_ID="$(json_path_or_empty "$HTTP_BODY" "data.id")"
  API_KEY="$(json_path_or_empty "$HTTP_BODY" "data.api_key")"
  if [[ -z "$API_KEY" ]]; then
    API_KEY="$(json_path_or_empty "$HTTP_BODY" "data.apiKey")"
  fi
  if [[ -z "$TEAM_ID" || -z "$API_KEY" ]]; then
    echo "Team bootstrap response was missing id or api_key"
    echo "${HTTP_BODY}"
    exit 1
  fi

  echo "Created team ${TEAM_ID}"

  api_post_json "/users" "$(USER_EMAIL="$user_email" USER_PASSWORD="$user_password" TEAM_ID="$TEAM_ID" "$PYTHON_BIN" - <<'PY'
import json
import os

print(json.dumps({
    "email": os.environ["USER_EMAIL"],
    "name": "Codex Telemetry Tester",
    "role": "admin",
    "password": os.environ["USER_PASSWORD"],
    "teamIds": [int(os.environ["TEAM_ID"])],
}))
PY
)"
  require_last_request_ok "create user"

  api_post_json "/auth/login" "$(USER_EMAIL="$user_email" USER_PASSWORD="$user_password" "$PYTHON_BIN" - <<'PY'
import json
import os

print(json.dumps({
    "email": os.environ["USER_EMAIL"],
    "password": os.environ["USER_PASSWORD"],
}))
PY
)"
  require_last_request_ok "login"

  TOKEN="$(json_path_or_empty "$HTTP_BODY" "data.token")"
  if [[ -z "$TOKEN" ]]; then
    echo "Login response was missing token"
    echo "${HTTP_BODY}"
    exit 1
  fi

  echo "Authenticated user for team ${TEAM_ID}"
}

refresh_time_window() {
  local now
  now="$(now_ms)"
  START_MS=$((now - 3600000))
  END_MS=$((now + 300000))
}

wait_for_async_ingest() {
  echo ""
  echo "Waiting ${WAIT_SECONDS} seconds for async inserts to complete..."
  sleep "${WAIT_SECONDS}"
  refresh_time_window
}

clickhouse_query() {
  local query="$1"
  docker exec -i "${CLICKHOUSE_CONTAINER}" clickhouse-client \
    --user "${CLICKHOUSE_USER}" \
    --password "${CLICKHOUSE_PASSWORD}" \
    --query "${query}"
}

clickhouse_ready() {
  clickhouse_query "SELECT 1" >/dev/null 2>&1
}

api_suite_reset() {
  API_TOTAL=0
  API_FAIL=0
  API_REPORT_LINES=()
}

record_api_result() {
  local passed="$1"
  local label="$2"
  local path="$3"
  local expectation="$4"
  local status="$5"
  local summary="$6"

  API_TOTAL=$((API_TOTAL + 1))
  if [[ "$passed" -ne 1 ]]; then
    API_FAIL=$((API_FAIL + 1))
  fi

  local state="PASS"
  if [[ "$passed" -ne 1 ]]; then
    state="FAIL"
  fi

  API_REPORT_LINES[${#API_REPORT_LINES[@]}]="[${state}] ${label} | ${path} | expect=${expectation} | http=${status} | ${summary}"
}

run_api_check() {
  local label="$1"
  local path="$2"
  local expectation="${3:-success}"

  authed_api_get "$path"

  local passed=0
  if [[ "$HTTP_STATUS" == "200" ]] && response_is_success "$HTTP_BODY"; then
    if [[ "$expectation" == "non-empty" ]]; then
      if response_data_non_empty "$HTTP_BODY"; then
        passed=1
      fi
    else
      passed=1
    fi
  fi

  local summary
  summary="$(response_summary "$HTTP_BODY")"

  if [[ "$passed" -ne 1 ]]; then
    local err
    err="$(response_error_summary "$HTTP_BODY")"
    if [[ -n "$err" ]]; then
      summary="${summary} | ${err}"
    fi
  fi

  record_api_result "$passed" "$label" "$path" "$expectation" "$HTTP_STATUS" "$summary"
}

print_api_summary() {
  local title="$1"

  print_section "${title}"
  for line in "${API_REPORT_LINES[@]}"; do
    echo "${line}"
  done
  echo ""
  echo "API checks: ${API_TOTAL}"
  echo "API failures: ${API_FAIL}"
}
