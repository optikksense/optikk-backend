#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./test_telemetry_common.sh
source "${SCRIPT_DIR}/test_telemetry_common.sh"

ENVIRONMENT="staging"
LOG_SERVICE=""
ALT_LOG_SERVICE=""
HOST_NAME=""
POD_NAME=""
CONTAINER_NAME=""

TRACE_MAIN="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
SPAN_MAIN="bbbbbbbbbbbbbbbb"
TRACE_AUX="cccccccccccccccccccccccccccccccc"
SPAN_AUX="dddddddddddddddd"

ANCHOR_LOG_ID=""
ANCHOR_LOG_TS_MS=""
ANCHOR_TRACE_ID=""
ANCHOR_SPAN_ID=""

init_log_context() {
  LOG_SERVICE="logs-app-${TEST_RUN_ID}"
  ALT_LOG_SERVICE="logs-worker-${TEST_RUN_ID}"
  HOST_NAME="logs-host-${TEST_RUN_ID}"
  POD_NAME="logs-pod-${TEST_RUN_ID}"
  CONTAINER_NAME="logs-container-${TEST_RUN_ID}"
}

build_logs_payload() {
  LOG_SERVICE="${LOG_SERVICE}" \
  ALT_LOG_SERVICE="${ALT_LOG_SERVICE}" \
  HOST_NAME="${HOST_NAME}" \
  POD_NAME="${POD_NAME}" \
  CONTAINER_NAME="${CONTAINER_NAME}" \
  ENVIRONMENT="${ENVIRONMENT}" \
  TRACE_MAIN="${TRACE_MAIN}" \
  SPAN_MAIN="${SPAN_MAIN}" \
  TRACE_AUX="${TRACE_AUX}" \
  SPAN_AUX="${SPAN_AUX}" \
  "${PYTHON_BIN}" - <<'PY'
import json
import os
import time

base_ts = int(time.time() * 1_000_000_000)

def ts(offset_ms):
    return str(base_ts + offset_ms * 1_000_000)

def sval(value):
    return {"stringValue": str(value)}

def ival(value):
    return {"intValue": str(int(value))}

def fval(value):
    return {"doubleValue": float(value)}

def bval(value):
    return {"boolValue": bool(value)}

def kv(key, value):
    return {"key": key, "value": value}

def attrs(pairs):
    return [kv(key, value) for key, value in pairs]

def resource(service):
    return {
        "attributes": attrs([
            ("service.name", sval(service)),
            ("deployment.environment", sval(os.environ["ENVIRONMENT"])),
            ("host.name", sval(os.environ["HOST_NAME"])),
            ("k8s.pod.name", sval(os.environ["POD_NAME"])),
            ("container.name", sval(os.environ["CONTAINER_NAME"])),
            ("telemetry.sdk.language", sval("python")),
            ("telemetry.sdk.version", sval("1.30.0")),
        ])
    }

main_logs = [
    {
        "timeUnixNano": ts(0),
        "observedTimeUnixNano": ts(1),
        "severityNumber": 1,
        "severityText": "TRACE",
        "body": sval("TRACE log from comprehensive logs test"),
        "attributes": attrs([
            ("http.method", sval("GET")),
            ("http.status_code", ival(200)),
            ("sampled", bval(True)),
        ]),
        "traceId": os.environ["TRACE_MAIN"],
        "spanId": os.environ["SPAN_MAIN"],
    },
    {
        "timeUnixNano": ts(20),
        "observedTimeUnixNano": ts(21),
        "severityNumber": 5,
        "severityText": "DEBUG",
        "body": sval("DEBUG log from comprehensive logs test"),
        "attributes": attrs([
            ("component", sval("cache")),
            ("retry_count", ival(1)),
        ]),
        "traceId": os.environ["TRACE_MAIN"],
        "spanId": os.environ["SPAN_MAIN"],
    },
    {
        "timeUnixNano": ts(40),
        "observedTimeUnixNano": ts(41),
        "severityNumber": 9,
        "severityText": "INFO",
        "body": sval("INFO log from comprehensive logs test"),
        "attributes": attrs([
            ("http.method", sval("POST")),
            ("http.status_code", ival(202)),
            ("request_cost", fval(1.23)),
        ]),
        "traceId": os.environ["TRACE_MAIN"],
        "spanId": os.environ["SPAN_MAIN"],
    },
    {
        "timeUnixNano": "",
        "observedTimeUnixNano": ts(60),
        "severityNumber": 13,
        "severityText": "WARN",
        "body": sval("WARN log from comprehensive logs test"),
        "attributes": attrs([
            ("queue", sval("payments")),
            ("delayed", bval(True)),
        ]),
        "traceId": os.environ["TRACE_MAIN"],
        "spanId": os.environ["SPAN_MAIN"],
    },
    {
        "timeUnixNano": ts(80),
        "observedTimeUnixNano": ts(81),
        "severityNumber": 17,
        "severityText": "ERROR",
        "body": sval("ERROR log from comprehensive logs test"),
        "attributes": attrs([
            ("error", bval(True)),
            ("exception.type", sval("PaymentFailedException")),
        ]),
        "traceId": os.environ["TRACE_MAIN"],
        "spanId": os.environ["SPAN_MAIN"],
    },
]

aux_logs = [
    {
        "timeUnixNano": ts(100),
        "observedTimeUnixNano": ts(101),
        "severityNumber": 21,
        "severityText": "FATAL",
        "body": sval("FATAL log from comprehensive logs test"),
        "attributes": attrs([
            ("job", sval("worker")),
            ("error", bval(True)),
        ]),
        "traceId": os.environ["TRACE_AUX"],
        "spanId": os.environ["SPAN_AUX"],
    }
]

payload = {
    "resourceLogs": [
        {
            "resource": resource(os.environ["LOG_SERVICE"]),
            "scopeLogs": [{
                "scope": {"name": "codex.logs.main", "version": "1.0.0"},
                "logRecords": main_logs,
            }],
        },
        {
            "resource": resource(os.environ["ALT_LOG_SERVICE"]),
            "scopeLogs": [{
                "scope": {"name": "codex.logs.worker", "version": "1.0.0"},
                "logRecords": aux_logs,
            }],
        },
    ]
}

print(json.dumps(payload))
PY
}

verify_logs_ingestion() {
  print_section "Logs Ingestion Verification"

  if ! clickhouse_ready; then
    echo "Skipping ClickHouse verification because ${CLICKHOUSE_CONTAINER} is not reachable."
    return
  fi

  echo "Severity coverage:"
  clickhouse_query "
    SELECT severity_text, count()
    FROM observability.logs
    WHERE team_id = '${TEAM_ID}'
      AND service IN ('${LOG_SERVICE}', '${ALT_LOG_SERVICE}')
    GROUP BY severity_text
    ORDER BY severity_text
    FORMAT Pretty
  "

  echo ""
  echo "Recent logs:"
  clickhouse_query "
    SELECT severity_text, service, body, trace_id, span_id, timestamp
    FROM observability.logs
    WHERE team_id = '${TEAM_ID}'
      AND service IN ('${LOG_SERVICE}', '${ALT_LOG_SERVICE}')
    ORDER BY timestamp DESC
    LIMIT 10
    FORMAT Pretty
  "
}

prepare_log_anchor() {
  local q="?start=${START_MS}&end=${END_MS}&limit=20&services=${LOG_SERVICE}"

  authed_api_get "/logs${q}"
  require_last_request_ok "query logs for anchor context"

  ANCHOR_LOG_ID="$(json_path_or_empty "${HTTP_BODY}" "data.logs[0].id")"
  ANCHOR_TRACE_ID="$(json_path_or_empty "${HTTP_BODY}" "data.logs[0].traceId")"
  ANCHOR_SPAN_ID="$(json_path_or_empty "${HTTP_BODY}" "data.logs[0].spanId")"

  local anchor_ts_ns
  anchor_ts_ns="$(json_path_or_empty "${HTTP_BODY}" "data.logs[0].timestamp")"
  if [[ -n "${anchor_ts_ns}" ]]; then
    ANCHOR_LOG_TS_MS="$((anchor_ts_ns / 1000000))"
  fi

  if [[ -z "${ANCHOR_LOG_ID}" || -z "${ANCHOR_TRACE_ID}" || -z "${ANCHOR_SPAN_ID}" || -z "${ANCHOR_LOG_TS_MS}" ]]; then
    echo "Failed to derive log anchor context from /logs response"
    echo "${HTTP_BODY}"
    exit 1
  fi
}

run_logs_api_suite() {
  print_section "Logs API Suite"
  api_suite_reset

  local q="?start=${START_MS}&end=${END_MS}&services=${LOG_SERVICE}"

  local checks=(
    "Logs search|/logs${q}&limit=20|non-empty"
    "Logs facets|/logs/facets${q}|non-empty"
    "Logs histogram|/logs/histogram${q}|non-empty"
    "Logs volume|/logs/volume${q}|non-empty"
    "Logs stats|/logs/stats${q}|non-empty"
    "Logs fields|/logs/fields${q}&field=severity_text|non-empty"
    "Logs surrounding|/logs/surrounding?id=${ANCHOR_LOG_ID}&before=2&after=2|non-empty"
    "Logs detail|/logs/detail?traceId=${ANCHOR_TRACE_ID}&spanId=${ANCHOR_SPAN_ID}&timestamp=${ANCHOR_LOG_TS_MS}&contextWindow=30|non-empty"
    "Trace logs|/traces/${TRACE_MAIN}/logs|non-empty"
  )

  local entry
  for entry in "${checks[@]}"; do
    IFS='|' read -r label path expectation <<<"${entry}"
    run_api_check "${label}" "${path}" "${expectation}"
  done

  print_api_summary "Logs API Results"
}

main() {
  print_section "OTLP Logs Comprehensive Test"
  bootstrap_test_identity "logs"
  init_log_context

  echo "Sending comprehensive OTLP logs payload..."
  otlp_post_json "/v1/logs" "$(build_logs_payload)"
  if [[ "${HTTP_STATUS}" != "200" ]]; then
    echo "Logs ingestion request failed"
    echo "HTTP status: ${HTTP_STATUS}"
    echo "${HTTP_BODY}"
    exit 1
  fi
  echo "Logs payload accepted"

  wait_for_async_ingest
  verify_logs_ingestion
  prepare_log_anchor
  run_logs_api_suite

  if [[ "${API_FAIL}" -gt 0 ]]; then
    exit 1
  fi
}

main "$@"
