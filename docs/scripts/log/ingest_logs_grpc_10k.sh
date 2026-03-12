#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
High-load OTLP gRPC log ingester using grpcurl.

Usage:
  ingest_logs_grpc_10k.sh [options]

Options:
  --otlp-grpc <host:port>      OTLP gRPC endpoint (default: localhost:4317)
  --otlp-api-key <key>         Required x-api-key header value
  --target-lps <int>           Target logs per second (default: 10000)
  --duration-seconds <int>     Total run duration in seconds (default: 1800)
  --workers <int>              Number of sender workers (default: 10)
  --logs-per-request <int>     Log records per grpc Export request (default: 200)
  --dry-run                    Build one payload, validate, and exit (no network send)
  --help                       Show this help

Environment variable equivalents:
  OTLP_GRPC, OTLP_API_KEY, TARGET_LOGS_PER_SEC, DURATION_SECONDS, WORKERS, LOGS_PER_REQUEST
EOF
}

is_positive_int() {
  [[ "${1:-}" =~ ^[0-9]+$ ]] && (( 10#$1 > 0 ))
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

warn() {
  echo "WARN: $*" >&2
}

OTLP_GRPC="${OTLP_GRPC:-localhost:4317}"
OTLP_API_KEY="${OTLP_API_KEY:-}"
TARGET_LOGS_PER_SEC="${TARGET_LOGS_PER_SEC:-10000}"
DURATION_SECONDS="${DURATION_SECONDS:-1800}"
WORKERS="${WORKERS:-10}"
LOGS_PER_REQUEST="${LOGS_PER_REQUEST:-200}"
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --otlp-grpc)
      [[ $# -ge 2 ]] || die "--otlp-grpc requires a value"
      OTLP_GRPC="$2"
      shift 2
      ;;
    --otlp-api-key)
      [[ $# -ge 2 ]] || die "--otlp-api-key requires a value"
      OTLP_API_KEY="$2"
      shift 2
      ;;
    --target-lps)
      [[ $# -ge 2 ]] || die "--target-lps requires a value"
      TARGET_LOGS_PER_SEC="$2"
      shift 2
      ;;
    --duration-seconds)
      [[ $# -ge 2 ]] || die "--duration-seconds requires a value"
      DURATION_SECONDS="$2"
      shift 2
      ;;
    --workers)
      [[ $# -ge 2 ]] || die "--workers requires a value"
      WORKERS="$2"
      shift 2
      ;;
    --logs-per-request)
      [[ $# -ge 2 ]] || die "--logs-per-request requires a value"
      LOGS_PER_REQUEST="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

is_positive_int "$TARGET_LOGS_PER_SEC" || die "--target-lps must be a positive integer"
is_positive_int "$DURATION_SECONDS" || die "--duration-seconds must be a positive integer"
is_positive_int "$WORKERS" || die "--workers must be a positive integer"
is_positive_int "$LOGS_PER_REQUEST" || die "--logs-per-request must be a positive integer"

if (( LOGS_PER_REQUEST < 6 )); then
  die "--logs-per-request must be >= 6 to include TRACE, DEBUG, INFO, WARN, ERROR, and FATAL in every batch"
fi

if [[ -z "$OTLP_API_KEY" ]]; then
  die "OTLP API key is required. Set --otlp-api-key or OTLP_API_KEY."
fi

if (( DRY_RUN == 0 )); then
  command -v grpcurl >/dev/null 2>&1 || die "grpcurl is required in PATH"
fi

RPS_TOTAL=$(( TARGET_LOGS_PER_SEC / LOGS_PER_REQUEST ))
if (( RPS_TOTAL < 1 )); then
  die "target-lps/logs-per-request produced 0 requests/sec. Reduce --logs-per-request or increase --target-lps."
fi

ACTUAL_TARGET_LPS=$(( RPS_TOTAL * LOGS_PER_REQUEST ))
if (( ACTUAL_TARGET_LPS != TARGET_LOGS_PER_SEC )); then
  warn "target-lps (${TARGET_LOGS_PER_SEC}) is not divisible by logs-per-request (${LOGS_PER_REQUEST}); actual attempted load will be ${ACTUAL_TARGET_LPS} logs/sec"
fi

SERVICES=(
  "orders-service"
  "payments-service"
  "inventory-service"
  "checkout-service"
  "gateway-service"
  "auth-service"
)
HTTP_METHODS=("GET" "POST" "PUT" "PATCH" "DELETE")
HTTP_ROUTES=(
  "/api/v1/orders"
  "/api/v1/payments/authorize"
  "/api/v1/inventory/reserve"
  "/api/v1/checkout"
  "/api/v1/gateway/proxy"
  "/api/v1/auth/token"
)
COMPONENTS=(
  "order-controller"
  "payment-gateway-client"
  "inventory-client"
  "checkout-orchestrator"
  "edge-gateway"
  "auth-handler"
)
LOGGER_NAMES=(
  "com.example.orders.OrderController"
  "com.example.payments.PaymentGatewayClient"
  "com.example.inventory.InventoryClient"
  "com.example.checkout.CheckoutService"
  "com.example.gateway.EdgeGateway"
  "com.example.auth.TokenService"
)
SEVERITY_TEXTS=("TRACE" "DEBUG" "INFO" "WARN" "ERROR" "FATAL")
SEVERITY_NUMBERS=(1 5 9 13 17 21)
SEVERITY_HTTP_STATUS=(200 200 201 429 500 503)

SERVICE_COUNT=${#SERVICES[@]}
HTTP_METHOD_COUNT=${#HTTP_METHODS[@]}
HTTP_ROUTE_COUNT=${#HTTP_ROUTES[@]}
COMPONENT_COUNT=${#COMPONENTS[@]}
LOGGER_COUNT=${#LOGGER_NAMES[@]}
SEVERITY_COUNT=${#SEVERITY_TEXTS[@]}

LAST_HOUR_NS=3600000000000
OBSERVED_JITTER_NS_MAX=5000000
TRACE_POOL_SIZE=256
SPAN_POOL_SIZE=256

TRACE_ID_POOL=()
SPAN_ID_POOL=()

random_b64_bytes() {
  local n="$1"
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -base64 "$n" | tr -d '\n'
  else
    head -c "$n" /dev/urandom | base64 | tr -d '\n'
  fi
}

seed_id_pools() {
  local i
  for ((i = 0; i < TRACE_POOL_SIZE; i++)); do
    TRACE_ID_POOL[i]="$(random_b64_bytes 16)"
  done
  for ((i = 0; i < SPAN_POOL_SIZE; i++)); do
    SPAN_ID_POOL[i]="$(random_b64_bytes 8)"
  done
}

rand_u45() {
  echo $(( (RANDOM << 30) ^ (RANDOM << 15) ^ RANDOM ))
}

build_payload() {
  local worker_id="$1"
  local request_seq="$2"
  local out_file="$3"

  local now_s now_ns service_idx service host pod_name
  now_s="$(date +%s)"
  now_ns=$(( now_s * 1000000000 ))
  service_idx=$(( (worker_id + request_seq) % SERVICE_COUNT ))
  service="${SERVICES[$service_idx]}"
  host="${service%%-*}-node-$(( (worker_id % 5) + 1 ))"
  pod_name="${service}-w${worker_id}-r${request_seq}"

  {
    cat <<EOF
{
  "resourceLogs": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "${service}" } },
          { "key": "deployment.environment", "value": { "stringValue": "production" } },
          { "key": "host.name", "value": { "stringValue": "${host}" } },
          { "key": "k8s.namespace.name", "value": { "stringValue": "payments" } },
          { "key": "k8s.pod.name", "value": { "stringValue": "${pod_name}" } },
          { "key": "container.name", "value": { "stringValue": "${service}" } },
          { "key": "telemetry.sdk.language", "value": { "stringValue": "java" } }
        ]
      },
      "scopeLogs": [
        {
          "scope": {
            "name": "io.opentelemetry.javaagent.logback-mdc-1.0",
            "version": "2.11.0"
          },
          "logRecords": [
EOF

    local i severity_idx severity_text severity_number status_code
    local method route component logger_name
    local ts_offset ts_ns observed_jitter_ns observed_ns
    local trace_id_b64 span_id_b64
    local message

    for ((i = 0; i < LOGS_PER_REQUEST; i++)); do
      severity_idx=$(( i % SEVERITY_COUNT ))
      severity_text="${SEVERITY_TEXTS[$severity_idx]}"
      severity_number="${SEVERITY_NUMBERS[$severity_idx]}"
      status_code="${SEVERITY_HTTP_STATUS[$severity_idx]}"
      method="${HTTP_METHODS[$(( (i + worker_id + request_seq) % HTTP_METHOD_COUNT ))]}"
      route="${HTTP_ROUTES[$(( (i + request_seq) % HTTP_ROUTE_COUNT ))]}"
      component="${COMPONENTS[$(( (request_seq + i) % COMPONENT_COUNT ))]}"
      logger_name="${LOGGER_NAMES[$(( (i + worker_id) % LOGGER_COUNT ))]}"

      ts_offset=$(( $(rand_u45) % LAST_HOUR_NS ))
      ts_ns=$(( now_ns - ts_offset ))
      observed_jitter_ns=$(( $(rand_u45) % OBSERVED_JITTER_NS_MAX ))
      observed_ns=$(( ts_ns + observed_jitter_ns ))

      trace_id_b64="${TRACE_ID_POOL[$(( (worker_id * 131 + request_seq * 17 + i) % TRACE_POOL_SIZE ))]}"
      span_id_b64="${SPAN_ID_POOL[$(( (worker_id * 97 + request_seq * 19 + i) % SPAN_POOL_SIZE ))]}"

      case "$severity_text" in
        TRACE)
          message="Trace marker service=${service} route=${route} request=${request_seq} record=${i}"
          ;;
        DEBUG)
          message="Debug snapshot service=${service} component=${component}"
          ;;
        INFO)
          message="Business flow completed service=${service} status=${status_code}"
          ;;
        WARN)
          message="Latency threshold crossed service=${service} route=${route}"
          ;;
        ERROR)
          message="Dependency call failed service=${service} component=${component}"
          ;;
        FATAL)
          message="Circuit breaker opened service=${service} after repeated failures"
          ;;
      esac

      if (( i > 0 )); then
        printf ',\n'
      fi

      printf '            {\n'
      printf '              "timeUnixNano": "%s",\n' "$ts_ns"
      printf '              "observedTimeUnixNano": "%s",\n' "$observed_ns"
      printf '              "severityNumber": %s,\n' "$severity_number"
      printf '              "severityText": "%s",\n' "$severity_text"
      printf '              "body": { "stringValue": "%s" },\n' "$message"
      printf '              "attributes": [\n'
      printf '                { "key": "http.method", "value": { "stringValue": "%s" } },\n' "$method"
      printf '                { "key": "http.route", "value": { "stringValue": "%s" } },\n' "$route"
      printf '                { "key": "http.status_code", "value": { "intValue": "%s" } },\n' "$status_code"
      printf '                { "key": "logger.name", "value": { "stringValue": "%s" } },\n' "$logger_name"
      printf '                { "key": "component", "value": { "stringValue": "%s" } }' "$component"

      if [[ "$severity_text" == "ERROR" || "$severity_text" == "FATAL" ]]; then
        printf ',\n'
        printf '                { "key": "exception.type", "value": { "stringValue": "java.net.SocketTimeoutException" } },\n'
        printf '                { "key": "exception.message", "value": { "stringValue": "Read timed out" } },\n'
        printf '                { "key": "exception.stacktrace", "value": { "stringValue": "java.net.SocketTimeoutException: Read timed out at com.example.%s.Client.call(Client.java:45)" } }' "$component"
      fi

      printf '\n'
      printf '              ],\n'
      printf '              "traceId": "%s",\n' "$trace_id_b64"
      printf '              "spanId": "%s"\n' "$span_id_b64"
      printf '            }'
    done

    cat <<'EOF'

          ]
        }
      ]
    }
  ]
}
EOF
  } >"$out_file"
}

validate_payload() {
  local payload_file="$1"
  local sev
  for sev in "${SEVERITY_TEXTS[@]}"; do
    if ! grep -q "\"severityText\": \"${sev}\"" "$payload_file"; then
      die "dry-run validation failed: severity ${sev} missing from payload"
    fi
  done

  local now_ns min_ns out_of_range_count
  now_ns=$(( $(date +%s) * 1000000000 ))
  min_ns=$(( now_ns - LAST_HOUR_NS ))
  out_of_range_count="$(
    awk -v min_ns="$min_ns" -v max_ns="$now_ns" '
      /"timeUnixNano"/ {
        gsub(/[^0-9]/, "", $0)
        if ($0 != "") {
          ts = $0 + 0
          if (ts < min_ns || ts > max_ns) bad++
        }
      }
      END { print bad + 0 }
    ' "$payload_file"
  )"

  if (( out_of_range_count > 0 )); then
    die "dry-run validation failed: found ${out_of_range_count} timeUnixNano values outside [now-1h, now]"
  fi
}

write_stats() {
  local file="$1"
  local attempted="$2"
  local success="$3"
  local failed="$4"
  local tmp_file="${file}.tmp"
  printf '%s %s %s\n' "$attempted" "$success" "$failed" >"$tmp_file"
  mv "$tmp_file" "$file"
}

send_request() {
  local payload_file="$1"
  if (( DRY_RUN == 1 )); then
    return 0
  fi
  grpcurl -plaintext \
    -H "x-api-key: ${OTLP_API_KEY}" \
    -d @ \
    "${OTLP_GRPC}" \
    opentelemetry.proto.collector.logs.v1.LogsService/Export <"$payload_file" >/dev/null 2>&1
}

worker_loop() {
  local worker_id="$1"
  local worker_rps="$2"
  local stats_file="$3"
  local stop_file="$4"
  local payload_file="$5"
  local request_seq=0
  local attempted=0
  local success=0
  local failed=0

  write_stats "$stats_file" "$attempted" "$success" "$failed"

  if (( worker_rps <= 0 )); then
    while [[ ! -f "$stop_file" ]]; do
      sleep 0.2
    done
    return 0
  fi

  local interval
  interval="$(awk -v r="$worker_rps" 'BEGIN { printf "%.6f", 1.0 / r }')"

  while [[ ! -f "$stop_file" ]]; do
    build_payload "$worker_id" "$request_seq" "$payload_file"
    if send_request "$payload_file"; then
      success=$(( success + 1 ))
    else
      failed=$(( failed + 1 ))
    fi
    attempted=$(( attempted + LOGS_PER_REQUEST ))
    request_seq=$(( request_seq + 1 ))
    write_stats "$stats_file" "$attempted" "$success" "$failed"
    sleep "$interval"
  done
}

sum_stats() {
  local dir="$1"
  local total_attempted=0
  local total_success=0
  local total_failed=0
  local f a s b
  for f in "$dir"/worker_*.stats; do
    [[ -f "$f" ]] || continue
    if read -r a s b <"$f"; then
      total_attempted=$(( total_attempted + a ))
      total_success=$(( total_success + s ))
      total_failed=$(( total_failed + b ))
    fi
  done
  echo "${total_attempted} ${total_success} ${total_failed}"
}

echo "Config:"
echo "  otlp_grpc=${OTLP_GRPC}"
echo "  target_logs_per_sec=${TARGET_LOGS_PER_SEC}"
echo "  actual_attempted_logs_per_sec=${ACTUAL_TARGET_LPS}"
echo "  duration_seconds=${DURATION_SECONDS}"
echo "  workers=${WORKERS}"
echo "  logs_per_request=${LOGS_PER_REQUEST}"
echo "  dry_run=${DRY_RUN}"

seed_id_pools

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/otlp-log-load.XXXXXX")"
STOP_FILE="${TMP_DIR}/stop.flag"

if (( DRY_RUN == 1 )); then
  PAYLOAD_FILE="${TMP_DIR}/dry_run_payload.json"
  build_payload 0 0 "$PAYLOAD_FILE"
  validate_payload "$PAYLOAD_FILE"
  echo
  echo "Dry run success."
  echo "Generated payload: ${PAYLOAD_FILE}"
  echo "Validated:"
  echo "  - Contains all severities: TRACE DEBUG INFO WARN ERROR FATAL"
  echo "  - timeUnixNano values are in [now-1h, now]"
  echo
  echo "Preview (first 60 lines):"
  sed -n '1,60p' "$PAYLOAD_FILE"
  exit 0
fi

BASE_RPS_PER_WORKER=$(( RPS_TOTAL / WORKERS ))
EXTRA_RPS_WORKERS=$(( RPS_TOTAL % WORKERS ))

echo "Request rate:"
echo "  total_requests_per_sec=${RPS_TOTAL}"
echo "  base_rps_per_worker=${BASE_RPS_PER_WORKER}"
echo "  remainder_workers_get_plus_one=${EXTRA_RPS_WORKERS}"
echo

on_signal() {
  touch "$STOP_FILE"
}
trap on_signal INT TERM

WORKER_PIDS=()
for ((w = 0; w < WORKERS; w++)); do
  worker_rps="$BASE_RPS_PER_WORKER"
  if (( w < EXTRA_RPS_WORKERS )); then
    worker_rps=$(( worker_rps + 1 ))
  fi
  stats_file="${TMP_DIR}/worker_${w}.stats"
  payload_file="${TMP_DIR}/worker_${w}.payload.json"
  worker_loop "$w" "$worker_rps" "$stats_file" "$STOP_FILE" "$payload_file" &
  WORKER_PIDS+=("$!")
done

START_EPOCH="$(date +%s)"
prev_attempted=0
prev_success=0
prev_failed=0

while true; do
  sleep 1
  read -r total_attempted total_success total_failed < <(sum_stats "$TMP_DIR")
  delta_attempted=$(( total_attempted - prev_attempted ))
  delta_success=$(( total_success - prev_success ))
  delta_failed=$(( total_failed - prev_failed ))

  printf '[%s] attempted_logs_per_sec=%d success_req_per_sec=%d failed_req_per_sec=%d total_logs=%d\n' \
    "$(date '+%H:%M:%S')" \
    "$delta_attempted" \
    "$delta_success" \
    "$delta_failed" \
    "$total_attempted"

  prev_attempted="$total_attempted"
  prev_success="$total_success"
  prev_failed="$total_failed"

  now_epoch="$(date +%s)"
  elapsed=$(( now_epoch - START_EPOCH ))
  if (( elapsed >= DURATION_SECONDS )); then
    echo "Reached duration (${DURATION_SECONDS}s). Stopping workers..."
    touch "$STOP_FILE"
  fi

  if [[ -f "$STOP_FILE" ]]; then
    break
  fi
done

for pid in "${WORKER_PIDS[@]}"; do
  wait "$pid" || true
done

read -r final_attempted final_success final_failed < <(sum_stats "$TMP_DIR")
echo
echo "Final summary:"
echo "  total_attempted_logs=${final_attempted}"
echo "  total_successful_requests=${final_success}"
echo "  total_failed_requests=${final_failed}"
echo "  temp_dir=${TMP_DIR}"
