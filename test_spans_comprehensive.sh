#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./test_telemetry_common.sh
source "${SCRIPT_DIR}/test_telemetry_common.sh"

ENVIRONMENT="staging"
GATEWAY_SERVICE=""
PAYMENT_SERVICE=""
WORKER_SERVICE=""
BILLING_SERVICE=""
CATALOG_SERVICE=""
GATEWAY_HOST=""
PAYMENT_HOST=""
WORKER_HOST=""
BILLING_HOST=""
CATALOG_HOST=""
NAMESPACE_NAME=""

TRACE_CHECKOUT="11111111111111111111111111111111"
TRACE_ERROR="22222222222222222222222222222222"
TRACE_CATALOG_OK="33333333333333333333333333333333"
TRACE_CATALOG_ERR="44444444444444444444444444444444"

SPAN_GATEWAY_ROOT="1111111111111111"
SPAN_CLIENT_PAYMENT="2222222222222222"
SPAN_PAYMENT_SERVER="3333333333333333"
SPAN_PAYMENT_INTERNAL="4444444444444444"
SPAN_CLIENT_EXTERNAL="5555555555555555"
SPAN_PAYMENT_PRODUCER="6666666666666666"
SPAN_WORKER_CONSUMER="7777777777777777"
SPAN_GATEWAY_UNSPECIFIED="8888888888888888"
SPAN_BILLING_ROOT="9999999999999999"
SPAN_BILLING_INTERNAL="aaaaaaaaaaaaaaaa"
SPAN_CATALOG_OK="bbbbbbbbbbbbbbbb"
SPAN_CATALOG_ERR="cccccccccccccccc"

init_span_context() {
  GATEWAY_SERVICE="gateway-${TEST_RUN_ID}"
  PAYMENT_SERVICE="payment-${TEST_RUN_ID}"
  WORKER_SERVICE="worker-${TEST_RUN_ID}"
  BILLING_SERVICE="billing-${TEST_RUN_ID}"
  CATALOG_SERVICE="catalog-${TEST_RUN_ID}"
  GATEWAY_HOST="edge-${TEST_RUN_ID}"
  PAYMENT_HOST="payments-${TEST_RUN_ID}"
  WORKER_HOST="worker-${TEST_RUN_ID}"
  BILLING_HOST="billing-${TEST_RUN_ID}"
  CATALOG_HOST="catalog-${TEST_RUN_ID}"
  NAMESPACE_NAME="spans-ns-${TEST_RUN_ID}"
}

build_spans_payload() {
  GATEWAY_SERVICE="${GATEWAY_SERVICE}" \
  PAYMENT_SERVICE="${PAYMENT_SERVICE}" \
  WORKER_SERVICE="${WORKER_SERVICE}" \
  BILLING_SERVICE="${BILLING_SERVICE}" \
  CATALOG_SERVICE="${CATALOG_SERVICE}" \
  GATEWAY_HOST="${GATEWAY_HOST}" \
  PAYMENT_HOST="${PAYMENT_HOST}" \
  WORKER_HOST="${WORKER_HOST}" \
  BILLING_HOST="${BILLING_HOST}" \
  CATALOG_HOST="${CATALOG_HOST}" \
  NAMESPACE_NAME="${NAMESPACE_NAME}" \
  ENVIRONMENT="${ENVIRONMENT}" \
  TRACE_CHECKOUT="${TRACE_CHECKOUT}" \
  TRACE_ERROR="${TRACE_ERROR}" \
  TRACE_CATALOG_OK="${TRACE_CATALOG_OK}" \
  TRACE_CATALOG_ERR="${TRACE_CATALOG_ERR}" \
  SPAN_GATEWAY_ROOT="${SPAN_GATEWAY_ROOT}" \
  SPAN_CLIENT_PAYMENT="${SPAN_CLIENT_PAYMENT}" \
  SPAN_PAYMENT_SERVER="${SPAN_PAYMENT_SERVER}" \
  SPAN_PAYMENT_INTERNAL="${SPAN_PAYMENT_INTERNAL}" \
  SPAN_CLIENT_EXTERNAL="${SPAN_CLIENT_EXTERNAL}" \
  SPAN_PAYMENT_PRODUCER="${SPAN_PAYMENT_PRODUCER}" \
  SPAN_WORKER_CONSUMER="${SPAN_WORKER_CONSUMER}" \
  SPAN_GATEWAY_UNSPECIFIED="${SPAN_GATEWAY_UNSPECIFIED}" \
  SPAN_BILLING_ROOT="${SPAN_BILLING_ROOT}" \
  SPAN_BILLING_INTERNAL="${SPAN_BILLING_INTERNAL}" \
  SPAN_CATALOG_OK="${SPAN_CATALOG_OK}" \
  SPAN_CATALOG_ERR="${SPAN_CATALOG_ERR}" \
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

def bval(value):
    return {"boolValue": bool(value)}

def kv(key, value):
    return {"key": key, "value": value}

def attrs(pairs):
    return [kv(key, value) for key, value in pairs]

def resource(service, host):
    return {
        "attributes": attrs([
            ("service.name", sval(service)),
            ("deployment.environment", sval(os.environ["ENVIRONMENT"])),
            ("host.name", sval(host)),
            ("k8s.namespace.name", sval(os.environ["NAMESPACE_NAME"])),
            ("telemetry.sdk.language", sval("python")),
            ("telemetry.sdk.version", sval("1.30.0")),
        ])
    }

def span(trace_id, span_id, parent_span_id, name, kind, start_ms, end_ms, span_attrs, status_code=1, status_message="", events=None, links=None):
    return {
        "traceId": trace_id,
        "spanId": span_id,
        "parentSpanId": parent_span_id,
        "name": name,
        "kind": kind,
        "startTimeUnixNano": ts(start_ms),
        "endTimeUnixNano": ts(end_ms),
        "attributes": span_attrs,
        "status": {"code": status_code, "message": status_message},
        "events": events or [],
        "links": links or [],
    }

gateway_spans = [
    span(
        os.environ["TRACE_CHECKOUT"],
        os.environ["SPAN_GATEWAY_ROOT"],
        "",
        "GET /checkout",
        2,
        0,
        180,
        attrs([
            ("http.method", sval("GET")),
            ("http.route", sval("/checkout")),
            ("http.url", sval("https://gateway.internal/checkout")),
            ("http.host", sval("gateway.internal")),
            ("http.status_code", ival(200)),
        ]),
        events=[{
            "timeUnixNano": ts(15),
            "name": "gateway.request.received",
            "attributes": attrs([("component", sval("gateway"))]),
        }],
    ),
    span(
        os.environ["TRACE_CHECKOUT"],
        os.environ["SPAN_CLIENT_PAYMENT"],
        os.environ["SPAN_GATEWAY_ROOT"],
        "payment.charge",
        3,
        20,
        120,
        attrs([
            ("http.method", sval("POST")),
            ("http.url", sval("http://payment.internal/charge")),
            ("http.host", sval("payment.internal")),
            ("http.status_code", ival(200)),
        ]),
    ),
    span(
        os.environ["TRACE_CHECKOUT"],
        os.environ["SPAN_CLIENT_EXTERNAL"],
        os.environ["SPAN_GATEWAY_ROOT"],
        "inventory.lookup",
        3,
        35,
        70,
        attrs([
            ("http.method", sval("GET")),
            ("http.url", sval("https://inventory.example.com/stock")),
            ("http.host", sval("inventory.example.com")),
            ("http.status_code", ival(200)),
        ]),
    ),
    span(
        os.environ["TRACE_CHECKOUT"],
        os.environ["SPAN_GATEWAY_UNSPECIFIED"],
        os.environ["SPAN_GATEWAY_ROOT"],
        "gateway.unspecified",
        0,
        125,
        140,
        attrs([("component", sval("gateway"))]),
    ),
]

payment_spans = [
    span(
        os.environ["TRACE_CHECKOUT"],
        os.environ["SPAN_PAYMENT_SERVER"],
        os.environ["SPAN_CLIENT_PAYMENT"],
        "payment.charge",
        2,
        25,
        95,
        attrs([
            ("http.request.method", sval("POST")),
            ("http.route", sval("/charge")),
            ("http.response.status_code", ival(200)),
            ("url.full", sval("http://payment.internal/charge")),
        ]),
    ),
    span(
        os.environ["TRACE_CHECKOUT"],
        os.environ["SPAN_PAYMENT_INTERNAL"],
        os.environ["SPAN_PAYMENT_SERVER"],
        "validate-card",
        1,
        35,
        80,
        attrs([
            ("db.name", sval("orders")),
            ("db.operation", sval("SELECT")),
        ]),
    ),
    span(
        os.environ["TRACE_CHECKOUT"],
        os.environ["SPAN_PAYMENT_PRODUCER"],
        os.environ["SPAN_PAYMENT_SERVER"],
        "publish.receipt",
        4,
        82,
        110,
        attrs([
            ("messaging.system", sval("kafka")),
            ("messaging.destination.name", sval("payment.receipts")),
        ]),
    ),
]

worker_spans = [
    span(
        os.environ["TRACE_CHECKOUT"],
        os.environ["SPAN_WORKER_CONSUMER"],
        os.environ["SPAN_PAYMENT_PRODUCER"],
        "publish.receipt",
        5,
        90,
        135,
        attrs([
            ("messaging.system", sval("kafka")),
            ("messaging.destination.name", sval("payment.receipts")),
        ]),
        links=[{
            "traceId": os.environ["TRACE_CHECKOUT"],
            "spanId": os.environ["SPAN_PAYMENT_SERVER"],
            "attributes": attrs([("link.type", sval("causal"))]),
        }],
    )
]

billing_spans = [
    span(
        os.environ["TRACE_ERROR"],
        os.environ["SPAN_BILLING_ROOT"],
        "",
        "POST /charge",
        2,
        250,
        430,
        attrs([
            ("http.method", sval("POST")),
            ("http.route", sval("/charge")),
            ("http.url", sval("https://billing.internal/charge")),
            ("http.status_code", ival(500)),
        ]),
        status_code=2,
        status_message="charge failed",
    ),
    span(
        os.environ["TRACE_ERROR"],
        os.environ["SPAN_BILLING_INTERNAL"],
        os.environ["SPAN_BILLING_ROOT"],
        "charge-card",
        1,
        270,
        390,
        attrs([
            ("exception.type", sval("PaymentFailedException")),
            ("exception.message", sval("card declined")),
            ("exception.stacktrace", sval("PaymentFailedException: card declined")),
            ("exception.escaped", bval(True)),
        ]),
        status_code=2,
        status_message="card declined",
        events=[{
            "timeUnixNano": ts(300),
            "name": "exception",
            "attributes": attrs([
                ("exception.type", sval("PaymentFailedException")),
                ("exception.message", sval("card declined")),
            ]),
        }],
    ),
]

catalog_spans = [
    span(
        os.environ["TRACE_CATALOG_OK"],
        os.environ["SPAN_CATALOG_OK"],
        "",
        "GET /catalog",
        2,
        500,
        610,
        attrs([
            ("http.method", sval("GET")),
            ("http.route", sval("/catalog")),
            ("http.status_code", ival(200)),
        ]),
    ),
    span(
        os.environ["TRACE_CATALOG_ERR"],
        os.environ["SPAN_CATALOG_ERR"],
        "",
        "GET /catalog",
        2,
        650,
        820,
        attrs([
            ("http.method", sval("GET")),
            ("http.route", sval("/catalog")),
            ("http.status_code", ival(503)),
        ]),
        status_code=2,
        status_message="catalog unavailable",
    ),
]

payload = {
    "resourceSpans": [
        {
            "resource": resource(os.environ["GATEWAY_SERVICE"], os.environ["GATEWAY_HOST"]),
            "scopeSpans": [{"scope": {"name": "codex.traces.gateway", "version": "1.0.0"}, "spans": gateway_spans}],
        },
        {
            "resource": resource(os.environ["PAYMENT_SERVICE"], os.environ["PAYMENT_HOST"]),
            "scopeSpans": [{"scope": {"name": "codex.traces.payment", "version": "1.0.0"}, "spans": payment_spans}],
        },
        {
            "resource": resource(os.environ["WORKER_SERVICE"], os.environ["WORKER_HOST"]),
            "scopeSpans": [{"scope": {"name": "codex.traces.worker", "version": "1.0.0"}, "spans": worker_spans}],
        },
        {
            "resource": resource(os.environ["BILLING_SERVICE"], os.environ["BILLING_HOST"]),
            "scopeSpans": [{"scope": {"name": "codex.traces.billing", "version": "1.0.0"}, "spans": billing_spans}],
        },
        {
            "resource": resource(os.environ["CATALOG_SERVICE"], os.environ["CATALOG_HOST"]),
            "scopeSpans": [{"scope": {"name": "codex.traces.catalog", "version": "1.0.0"}, "spans": catalog_spans}],
        },
    ]
}

print(json.dumps(payload))
PY
}

verify_spans_ingestion() {
  print_section "Spans Ingestion Verification"

  if ! clickhouse_ready; then
    echo "Skipping ClickHouse verification because ${CLICKHOUSE_CONTAINER} is not reachable."
    return
  fi

  echo "Span kinds inserted:"
  clickhouse_query "
    SELECT kind_string, count()
    FROM observability.spans
    WHERE team_id = '${TEAM_ID}'
      AND trace_id IN ('${TRACE_CHECKOUT}', '${TRACE_ERROR}', '${TRACE_CATALOG_OK}', '${TRACE_CATALOG_ERR}')
    GROUP BY kind_string
    ORDER BY kind_string
    FORMAT Pretty
  "

  echo ""
  echo "Error and exception coverage:"
  clickhouse_query "
    SELECT
      countIf(has_error = 1) AS error_spans,
      countIf(exception_type != '') AS exception_spans,
      groupUniqArray(name) AS operations
    FROM observability.spans
    WHERE team_id = '${TEAM_ID}'
      AND trace_id IN ('${TRACE_CHECKOUT}', '${TRACE_ERROR}', '${TRACE_CATALOG_OK}', '${TRACE_CATALOG_ERR}')
    FORMAT Pretty
  "

  echo ""
  echo "Recent spans:"
  clickhouse_query "
    SELECT trace_id, span_id, kind_string, name, has_error, response_status_code, timestamp
    FROM observability.spans
    WHERE team_id = '${TEAM_ID}'
      AND trace_id IN ('${TRACE_CHECKOUT}', '${TRACE_ERROR}', '${TRACE_CATALOG_OK}', '${TRACE_CATALOG_ERR}')
    ORDER BY timestamp DESC
    LIMIT 20
    FORMAT Pretty
  "
}

run_spans_api_suite() {
  print_section "Spans API Suite"
  api_suite_reset

  local q="?start=${START_MS}&end=${END_MS}"
  local gateway_q="?start=${START_MS}&end=${END_MS}&serviceName=${GATEWAY_SERVICE}"
  local billing_q="?start=${START_MS}&end=${END_MS}&serviceName=${BILLING_SERVICE}"
  local latency_q="?start=${START_MS}&end=${END_MS}&serviceName=${GATEWAY_SERVICE}&operationName=GET%20/checkout"
  local client_server_q="?start=${START_MS}&end=${END_MS}&operationName=payment.charge"

  local checks=(
    "Trace search|/traces${q}&limit=50|non-empty"
    "Trace spans|/traces/${TRACE_CHECKOUT}/spans|non-empty"
    "Span tree|/spans/${SPAN_CLIENT_PAYMENT}/tree|non-empty"
    "Service dependencies|/services/dependencies${q}|non-empty"
    "Service errors|/services/${BILLING_SERVICE}/errors${q}|non-empty"
    "Latency histogram|/latency/histogram${latency_q}|non-empty"
    "Latency heatmap|/latency/heatmap${gateway_q}|non-empty"
    "Errors groups|/errors/groups${billing_q}|non-empty"
    "Errors timeseries|/errors/timeseries${billing_q}|non-empty"
    "Top slow operations|/spans/top-slow-operations${q}|non-empty"
    "Top error operations|/spans/top-error-operations${q}|non-empty"
    "HTTP status distribution|/spans/http-status-distribution${q}|non-empty"
    "Service scorecard|/spans/service-scorecard${q}|non-empty"
    "Apdex|/spans/apdex${q}|non-empty"
    "Exception rate by type|/spans/exception-rate-by-type${billing_q}|non-empty"
    "Error hotspot|/spans/error-hotspot${q}|non-empty"
    "HTTP 5xx by route|/spans/http-5xx-by-route${billing_q}|non-empty"
    "Trace span events|/traces/${TRACE_ERROR}/span-events|non-empty"
    "Trace span kind breakdown|/traces/${TRACE_CHECKOUT}/span-kind-breakdown|non-empty"
    "Trace critical path|/traces/${TRACE_CHECKOUT}/critical-path|non-empty"
    "Trace self times|/traces/${TRACE_CHECKOUT}/span-self-times|non-empty"
    "Trace error path|/traces/${TRACE_ERROR}/error-path|non-empty"
    "Overview request rate|/overview/request-rate${q}|non-empty"
    "Overview error rate|/overview/error-rate${q}|non-empty"
    "Overview p95 latency|/overview/p95-latency${q}|non-empty"
    "Overview services|/overview/services${q}|non-empty"
    "Overview top endpoints|/overview/top-endpoints${gateway_q}|non-empty"
    "Overview endpoints metrics|/overview/endpoints/metrics${gateway_q}|non-empty"
    "Overview endpoints timeseries|/overview/endpoints/timeseries${gateway_q}|non-empty"
    "Endpoints metrics alias|/endpoints/metrics${gateway_q}|non-empty"
    "Endpoints timeseries alias|/endpoints/timeseries${gateway_q}|non-empty"
    "Overview service error rate|/overview/errors/service-error-rate${billing_q}|non-empty"
    "Overview error volume|/overview/errors/error-volume${billing_q}|non-empty"
    "Overview latency during error windows|/overview/errors/latency-during-error-windows${billing_q}|non-empty"
    "Overview error groups|/overview/errors/groups${billing_q}|non-empty"
    "Overview SLO|/overview/slo${gateway_q}|non-empty"
    "Services total summary|/services/summary/total${q}|non-empty"
    "Services healthy summary|/services/summary/healthy${q}|non-empty"
    "Services degraded summary|/services/summary/degraded${q}|non-empty"
    "Services unhealthy summary|/services/summary/unhealthy${q}|non-empty"
    "Services metrics|/services/metrics${q}|non-empty"
    "Services timeseries|/services/timeseries${q}|non-empty"
    "Service endpoints|/services/${GATEWAY_SERVICE}/endpoints${q}|non-empty"
    "Metrics timeseries alias|/metrics/timeseries${q}|non-empty"
    "Services topology|/services/topology${q}|non-empty"
    "Upstream downstream|/services/${GATEWAY_SERVICE}/upstream-downstream${q}|non-empty"
    "External dependencies|/services/external-dependencies${q}|non-empty"
    "Client server latency|/spans/client-server-latency${client_server_q}|non-empty"
    "Infrastructure nodes|/infrastructure/nodes${q}|non-empty"
    "Infrastructure nodes summary|/infrastructure/nodes/summary${q}|non-empty"
    "Infrastructure node services|/infrastructure/nodes/${GATEWAY_HOST}/services${q}|non-empty"
  )

  local entry
  for entry in "${checks[@]}"; do
    IFS='|' read -r label path expectation <<<"${entry}"
    run_api_check "${label}" "${path}" "${expectation}"
  done

  print_api_summary "Spans API Results"
}

main() {
  print_section "OTLP Spans Comprehensive Test"
  bootstrap_test_identity "spans"
  init_span_context

  echo "Sending comprehensive OTLP spans payload..."
  otlp_post_json "/v1/traces" "$(build_spans_payload)"
  if [[ "${HTTP_STATUS}" != "200" ]]; then
    echo "Spans ingestion request failed"
    echo "HTTP status: ${HTTP_STATUS}"
    echo "${HTTP_BODY}"
    exit 1
  fi
  echo "Spans payload accepted"

  wait_for_async_ingest
  verify_spans_ingestion
  run_spans_api_suite

  if [[ "${API_FAIL}" -gt 0 ]]; then
    exit 1
  fi
}

main "$@"
