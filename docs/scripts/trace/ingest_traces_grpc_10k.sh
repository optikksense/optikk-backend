#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
High-load OTLP gRPC trace ingester using grpcurl.

Usage:
  ingest_traces_grpc_10k.sh [options]

Options:
  --otlp-grpc <host:port>         OTLP gRPC endpoint (default: localhost:4317)
  --otlp-api-key <key>            Required x-api-key header value
  --target-sps <int>              Target spans per second (default: 10000)
  --duration-seconds <int>        Total run duration in seconds (default: 1800)
  --workers <int>                 Number of sender workers (default: 10)
  --spans-per-request <int>       Spans per TraceService Export request (default: 200)
  --min-spans-per-trace <int>     Minimum spans in a generated trace (default: 2)
  --max-spans-per-trace <int>     Maximum spans in a generated trace (default: 6)
  --error-rate-percent <0-100>    Percent of traces marked as error (default: 10)
  --dry-run                       Build/validate one payload and exit (no network send)
  --help                          Show this help

Environment variable equivalents:
  OTLP_GRPC, OTLP_API_KEY, TARGET_SPANS_PER_SEC, DURATION_SECONDS, WORKERS,
  SPANS_PER_REQUEST, MIN_SPANS_PER_TRACE, MAX_SPANS_PER_TRACE, ERROR_RATE_PERCENT
EOF
}

is_positive_int() {
  [[ "${1:-}" =~ ^[0-9]+$ ]] && (( 10#$1 > 0 ))
}

is_percent() {
  [[ "${1:-}" =~ ^[0-9]+$ ]] && (( 10#$1 >= 0 && 10#$1 <= 100 ))
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
TARGET_SPANS_PER_SEC="${TARGET_SPANS_PER_SEC:-10000}"
DURATION_SECONDS="${DURATION_SECONDS:-1800}"
WORKERS="${WORKERS:-10}"
SPANS_PER_REQUEST="${SPANS_PER_REQUEST:-200}"
MIN_SPANS_PER_TRACE="${MIN_SPANS_PER_TRACE:-2}"
MAX_SPANS_PER_TRACE="${MAX_SPANS_PER_TRACE:-6}"
ERROR_RATE_PERCENT="${ERROR_RATE_PERCENT:-10}"
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
    --target-sps)
      [[ $# -ge 2 ]] || die "--target-sps requires a value"
      TARGET_SPANS_PER_SEC="$2"
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
    --spans-per-request)
      [[ $# -ge 2 ]] || die "--spans-per-request requires a value"
      SPANS_PER_REQUEST="$2"
      shift 2
      ;;
    --min-spans-per-trace)
      [[ $# -ge 2 ]] || die "--min-spans-per-trace requires a value"
      MIN_SPANS_PER_TRACE="$2"
      shift 2
      ;;
    --max-spans-per-trace)
      [[ $# -ge 2 ]] || die "--max-spans-per-trace requires a value"
      MAX_SPANS_PER_TRACE="$2"
      shift 2
      ;;
    --error-rate-percent)
      [[ $# -ge 2 ]] || die "--error-rate-percent requires a value"
      ERROR_RATE_PERCENT="$2"
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

is_positive_int "$TARGET_SPANS_PER_SEC" || die "--target-sps must be a positive integer"
is_positive_int "$DURATION_SECONDS" || die "--duration-seconds must be a positive integer"
is_positive_int "$WORKERS" || die "--workers must be a positive integer"
is_positive_int "$SPANS_PER_REQUEST" || die "--spans-per-request must be a positive integer"
is_positive_int "$MIN_SPANS_PER_TRACE" || die "--min-spans-per-trace must be a positive integer"
is_positive_int "$MAX_SPANS_PER_TRACE" || die "--max-spans-per-trace must be a positive integer"
is_percent "$ERROR_RATE_PERCENT" || die "--error-rate-percent must be an integer in [0,100]"

if (( MIN_SPANS_PER_TRACE > MAX_SPANS_PER_TRACE )); then
  die "--min-spans-per-trace cannot be greater than --max-spans-per-trace"
fi

if (( SPANS_PER_REQUEST < MIN_SPANS_PER_TRACE )); then
  die "--spans-per-request must be >= --min-spans-per-trace"
fi

if [[ -z "$OTLP_API_KEY" ]]; then
  die "OTLP API key is required. Set --otlp-api-key or OTLP_API_KEY."
fi

if (( DRY_RUN == 0 )); then
  command -v grpcurl >/dev/null 2>&1 || die "grpcurl is required in PATH"
fi

RPS_TOTAL=$(( TARGET_SPANS_PER_SEC / SPANS_PER_REQUEST ))
if (( RPS_TOTAL < 1 )); then
  die "target-sps/spans-per-request produced 0 requests/sec. Reduce --spans-per-request or increase --target-sps."
fi

ACTUAL_TARGET_SPS=$(( RPS_TOTAL * SPANS_PER_REQUEST ))
if (( ACTUAL_TARGET_SPS != TARGET_SPANS_PER_SEC )); then
  warn "target-sps (${TARGET_SPANS_PER_SEC}) is not divisible by spans-per-request (${SPANS_PER_REQUEST}); actual attempted load will be ${ACTUAL_TARGET_SPS} spans/sec"
fi

SERVICES=(
  "checkout-service"
  "orders-service"
  "payment-service"
  "inventory-service"
  "gateway-service"
  "auth-service"
)
HTTP_METHODS=("GET" "POST" "PUT" "PATCH" "DELETE")
HTTP_ROUTES=(
  "/api/v1/orders"
  "/api/v1/checkout"
  "/api/v1/payments/authorize"
  "/api/v1/inventory/reserve"
  "/api/v1/gateway/proxy"
  "/api/v1/auth/token"
)
COMPONENTS=(
  "checkout-orchestrator"
  "order-controller"
  "payment-gateway-client"
  "inventory-client"
  "edge-gateway"
  "auth-handler"
)

SERVICE_COUNT=${#SERVICES[@]}
HTTP_METHOD_COUNT=${#HTTP_METHODS[@]}
HTTP_ROUTE_COUNT=${#HTTP_ROUTES[@]}
COMPONENT_COUNT=${#COMPONENTS[@]}

LAST_HOUR_NS=3600000000000
MAX_TRACE_DURATION_NS=900000000
MIN_ROOT_DURATION_NS=80000000
MAX_ROOT_DURATION_NS=500000000

TRACE_POOL_SIZE=512
SPAN_POOL_SIZE=2048
TRACE_ID_POOL=()
SPAN_ID_POOL=()

BUILD_META_TRACE_COUNT=0
BUILD_META_ERROR_TRACE_COUNT=0
BUILD_META_TRACE_SPAN_COUNTS=()

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

rand_between() {
  local min="$1"
  local max="$2"
  if (( max <= min )); then
    echo "$min"
    return
  fi
  local span=$(( max - min + 1 ))
  echo $(( min + ($(rand_u45) % span) ))
}

emit_db_event() {
  local event_ns="$1"
  cat <<EOF
                {
                  "timeUnixNano": "${event_ns}",
                  "name": "inventory.reservation.persisted",
                  "attributes": [
                    { "key": "db.system", "value": { "stringValue": "postgresql" } },
                    { "key": "db.operation", "value": { "stringValue": "INSERT" } },
                    { "key": "db.sql.table", "value": { "stringValue": "reservations" } }
                  ]
                }
EOF
}

emit_exception_event() {
  local event_ns="$1"
  cat <<EOF
                {
                  "timeUnixNano": "${event_ns}",
                  "name": "exception",
                  "attributes": [
                    { "key": "exception.type", "value": { "stringValue": "java.net.SocketTimeoutException" } },
                    { "key": "exception.message", "value": { "stringValue": "Read timed out" } },
                    { "key": "exception.stacktrace", "value": { "stringValue": "java.net.SocketTimeoutException: Read timed out at com.example.TraceClient.call(TraceClient.java:45)" } }
                  ]
                }
EOF
}

build_payload() {
  local worker_id="$1"
  local request_seq="$2"
  local out_file="$3"

  local now_s now_ns
  now_s="$(date +%s)"
  now_ns=$(( now_s * 1000000000 ))

  BUILD_META_TRACE_COUNT=0
  BUILD_META_ERROR_TRACE_COUNT=0
  BUILD_META_TRACE_SPAN_COUNTS=()

  local remaining="$SPANS_PER_REQUEST"
  local trace_seq=0
  local first_resource=1

  {
    printf '{\n'
    printf '  "resourceSpans": [\n'

    while (( remaining > 0 )); do
      local spans_this_trace leftover
      if (( remaining <= MAX_SPANS_PER_TRACE )); then
        spans_this_trace="$remaining"
      else
        spans_this_trace="$(rand_between "$MIN_SPANS_PER_TRACE" "$MAX_SPANS_PER_TRACE")"
        leftover=$(( remaining - spans_this_trace ))
        if (( leftover > 0 && leftover < MIN_SPANS_PER_TRACE )); then
          spans_this_trace=$(( spans_this_trace - (MIN_SPANS_PER_TRACE - leftover) ))
        fi
      fi

      (( spans_this_trace < 1 )) && spans_this_trace=1
      remaining=$(( remaining - spans_this_trace ))

      BUILD_META_TRACE_COUNT=$(( BUILD_META_TRACE_COUNT + 1 ))
      BUILD_META_TRACE_SPAN_COUNTS+=("$spans_this_trace")

      local trace_is_error=0
      if (( ($(rand_u45) % 100) < ERROR_RATE_PERCENT )); then
        trace_is_error=1
      fi
      if (( trace_is_error == 1 )); then
        BUILD_META_ERROR_TRACE_COUNT=$(( BUILD_META_ERROR_TRACE_COUNT + 1 ))
      fi

      local error_span_idx=-1
      if (( trace_is_error == 1 )); then
        error_span_idx=$(( (trace_seq + worker_id + request_seq) % spans_this_trace ))
      fi

      local root_service_idx=$(( (worker_id + request_seq + trace_seq) % SERVICE_COUNT ))
      local downstream_service_idx=$(( (root_service_idx + 1) % SERVICE_COUNT ))
      local root_service="${SERVICES[$root_service_idx]}"
      local downstream_service="${SERVICES[$downstream_service_idx]}"
      local host="${root_service%%-*}-node-$(( (worker_id % 5) + 1 ))"
      local pod_name="${root_service}-trace-w${worker_id}-r${request_seq}-t${trace_seq}"

      local method="${HTTP_METHODS[$(( (request_seq + trace_seq) % HTTP_METHOD_COUNT ))]}"
      local route="${HTTP_ROUTES[$(( (trace_seq + worker_id) % HTTP_ROUTE_COUNT ))]}"
      local component="${COMPONENTS[$(( (trace_seq + request_seq) % COMPONENT_COUNT ))]}"

      local offset_range=$(( LAST_HOUR_NS - MAX_TRACE_DURATION_NS ))
      local trace_offset=$(( MAX_TRACE_DURATION_NS + ($(rand_u45) % offset_range) ))
      local trace_start_ns=$(( now_ns - trace_offset ))

      local root_dur_ns
      root_dur_ns="$(rand_between "$MIN_ROOT_DURATION_NS" "$MAX_ROOT_DURATION_NS")"
      local root_end_ns=$(( trace_start_ns + root_dur_ns ))

      local trace_pool_idx=$(( (worker_id * 131 + request_seq * 17 + trace_seq * 11) % TRACE_POOL_SIZE ))
      local trace_id_b64="${TRACE_ID_POOL[$trace_pool_idx]}"

      local span_ids=()
      local si
      for ((si = 0; si < spans_this_trace; si++)); do
        local span_pool_idx=$(( (worker_id * 97 + request_seq * 19 + trace_seq * 23 + si) % SPAN_POOL_SIZE ))
        span_ids+=("${SPAN_ID_POOL[$span_pool_idx]}")
      done

      if (( first_resource == 0 )); then
        printf ',\n'
      fi
      first_resource=0

      printf '    {\n'
      printf '      "resource": {\n'
      printf '        "attributes": [\n'
      printf '          { "key": "service.name", "value": { "stringValue": "%s" } },\n' "$root_service"
      printf '          { "key": "deployment.environment", "value": { "stringValue": "production" } },\n'
      printf '          { "key": "host.name", "value": { "stringValue": "%s" } },\n' "$host"
      printf '          { "key": "k8s.namespace.name", "value": { "stringValue": "payments" } },\n'
      printf '          { "key": "k8s.pod.name", "value": { "stringValue": "%s" } }\n' "$pod_name"
      printf '        ]\n'
      printf '      },\n'
      printf '      "scopeSpans": [\n'
      printf '        {\n'
      printf '          "scope": {\n'
      printf '            "name": "io.opentelemetry.spring-webmvc-6.0",\n'
      printf '            "version": "2.11.0"\n'
      printf '          },\n'
      printf '          "spans": [\n'

      local first_span=1
      for ((si = 0; si < spans_this_trace; si++)); do
        local span_id="${span_ids[$si]}"
        local parent_span_id=""
        local span_kind=1
        local span_name=""
        local start_ns="$trace_start_ns"
        local end_ns="$root_end_ns"
        local http_status_code="200"

        if (( si == 0 )); then
          span_kind=2
          span_name="${method} ${route}"
          start_ns="$trace_start_ns"
          end_ns="$root_end_ns"
          http_status_code="201"
        elif (( si == 1 )); then
          parent_span_id="${span_ids[0]}"
          span_kind=3
          span_name="POST ${downstream_service}/internal/reservations"
          start_ns=$(( trace_start_ns + root_dur_ns / 6 ))
          end_ns=$(( trace_start_ns + (root_dur_ns * 3 / 4) ))
          http_status_code="200"
        elif (( si == 2 )); then
          parent_span_id="${span_ids[1]}"
          span_kind=2
          span_name="POST /internal/reservations"
          start_ns=$(( trace_start_ns + (root_dur_ns * 2 / 5) ))
          end_ns=$(( trace_start_ns + (root_dur_ns * 2 / 3) ))
          http_status_code="200"
        else
          parent_span_id="${span_ids[0]}"
          span_kind=1
          span_name="internal.${component}.step.${si}"
          local internal_start_min=$(( trace_start_ns + root_dur_ns / 5 ))
          local internal_start_max=$(( root_end_ns - 3000000 ))
          start_ns="$(rand_between "$internal_start_min" "$internal_start_max")"
          local internal_dur_ns
          internal_dur_ns="$(rand_between 2000000 60000000)"
          end_ns=$(( start_ns + internal_dur_ns ))
          local end_ceiling=$(( root_end_ns - 1000000 ))
          if (( end_ns > end_ceiling )); then
            end_ns="$end_ceiling"
          fi
          if (( end_ns <= start_ns )); then
            end_ns=$(( start_ns + 1000000 ))
          fi
          http_status_code="200"
        fi

        if (( end_ns <= start_ns )); then
          end_ns=$(( start_ns + 1000000 ))
        fi

        local is_error_span=0
        if (( trace_is_error == 1 && si == error_span_idx )); then
          is_error_span=1
          http_status_code="500"
        fi

        if (( first_span == 0 )); then
          printf ',\n'
        fi
        first_span=0

        printf '            {\n'
        printf '              "traceId": "%s",\n' "$trace_id_b64"
        printf '              "spanId": "%s",\n' "$span_id"
        if [[ -n "$parent_span_id" ]]; then
          printf '              "parentSpanId": "%s",\n' "$parent_span_id"
        fi
        printf '              "name": "%s",\n' "$span_name"
        printf '              "kind": %s,\n' "$span_kind"
        printf '              "startTimeUnixNano": "%s",\n' "$start_ns"
        printf '              "endTimeUnixNano": "%s",\n' "$end_ns"
        printf '              "attributes": [\n'

        local attrs=()
        if (( si == 0 )); then
          attrs+=("{ \"key\": \"http.method\", \"value\": { \"stringValue\": \"${method}\" } }")
          attrs+=("{ \"key\": \"http.route\", \"value\": { \"stringValue\": \"${route}\" } }")
          attrs+=("{ \"key\": \"http.url\", \"value\": { \"stringValue\": \"https://${root_service}.example.internal${route}\" } }")
          attrs+=("{ \"key\": \"http.status_code\", \"value\": { \"intValue\": \"${http_status_code}\" } }")
        elif (( si == 1 )); then
          attrs+=("{ \"key\": \"peer.service\", \"value\": { \"stringValue\": \"${downstream_service}\" } }")
          attrs+=("{ \"key\": \"http.method\", \"value\": { \"stringValue\": \"POST\" } }")
          attrs+=("{ \"key\": \"http.url\", \"value\": { \"stringValue\": \"http://${downstream_service}/internal/reservations\" } }")
          attrs+=("{ \"key\": \"http.status_code\", \"value\": { \"intValue\": \"${http_status_code}\" } }")
        elif (( si == 2 )); then
          attrs+=("{ \"key\": \"http.method\", \"value\": { \"stringValue\": \"POST\" } }")
          attrs+=("{ \"key\": \"http.route\", \"value\": { \"stringValue\": \"/internal/reservations\" } }")
          attrs+=("{ \"key\": \"http.status_code\", \"value\": { \"intValue\": \"${http_status_code}\" } }")
        else
          attrs+=("{ \"key\": \"component\", \"value\": { \"stringValue\": \"${component}\" } }")
          attrs+=("{ \"key\": \"operation.name\", \"value\": { \"stringValue\": \"${span_name}\" } }")
          attrs+=("{ \"key\": \"http.status_code\", \"value\": { \"intValue\": \"${http_status_code}\" } }")
        fi

        if (( is_error_span == 1 )); then
          attrs+=("{ \"key\": \"exception.type\", \"value\": { \"stringValue\": \"java.net.SocketTimeoutException\" } }")
          attrs+=("{ \"key\": \"exception.message\", \"value\": { \"stringValue\": \"Read timed out\" } }")
          attrs+=("{ \"key\": \"exception.stacktrace\", \"value\": { \"stringValue\": \"java.net.SocketTimeoutException: Read timed out at com.example.TraceClient.call(TraceClient.java:45)\" } }")
        fi

        local ai
        for ai in "${!attrs[@]}"; do
          if (( ai > 0 )); then
            printf ',\n'
          fi
          printf '                %s' "${attrs[$ai]}"
        done
        printf '\n'
        printf '              ]'

        local has_db_event=0
        local has_exception_event=0
        if (( si == 2 )); then
          has_db_event=1
        fi
        if (( is_error_span == 1 )); then
          has_exception_event=1
        fi

        if (( has_db_event == 1 || has_exception_event == 1 )); then
          printf ',\n'
          printf '              "events": [\n'
          local emitted_event=0
          if (( has_db_event == 1 )); then
            emit_db_event $(( end_ns - 1000000 ))
            emitted_event=1
          fi
          if (( has_exception_event == 1 )); then
            if (( emitted_event == 1 )); then
              printf ',\n'
            fi
            emit_exception_event $(( end_ns - 500000 ))
          fi
          printf '\n'
          printf '              ]'
        fi

        if (( is_error_span == 1 )); then
          printf ',\n'
          printf '              "status": {\n'
          printf '                "code": 2,\n'
          printf '                "message": "java.net.SocketTimeoutException: Read timed out"\n'
          printf '              }\n'
        else
          printf ',\n'
          printf '              "status": { "code": 1 }\n'
        fi
        printf '            }'
      done

      printf '\n'
      printf '          ]\n'
      printf '        }\n'
      printf '      ]\n'
      printf '    }'

      trace_seq=$(( trace_seq + 1 ))
    done

    printf '\n'
    printf '  ]\n'
    printf '}\n'
  } >"$out_file"
}

validate_payload() {
  local payload_file="$1"

  grep -q '"resourceSpans"' "$payload_file" || die "dry-run validation failed: missing resourceSpans"
  grep -q '"scopeSpans"' "$payload_file" || die "dry-run validation failed: missing scopeSpans"
  grep -q '"spans"' "$payload_file" || die "dry-run validation failed: missing spans"

  local span_count
  span_count="$(grep -c '"spanId": "' "$payload_file")"
  if (( span_count != SPANS_PER_REQUEST )); then
    die "dry-run validation failed: expected ${SPANS_PER_REQUEST} spans, got ${span_count}"
  fi

  grep -Eq '"traceId": "[A-Za-z0-9+/]{22}=="' "$payload_file" || die "dry-run validation failed: traceId base64 pattern not found"
  grep -Eq '"spanId": "[A-Za-z0-9+/]{11}="' "$payload_file" || die "dry-run validation failed: spanId base64 pattern not found"

  local now_ns min_ns bad_range bad_order
  now_ns=$(( $(date +%s) * 1000000000 ))
  min_ns=$(( now_ns - LAST_HOUR_NS ))
  read -r bad_range bad_order < <(
    awk -v min_ns="$min_ns" -v max_ns="$now_ns" '
      /"startTimeUnixNano"/ {
        gsub(/[^0-9]/, "", $0)
        if ($0 != "") {
          s = $0 + 0
          if (s < min_ns || s > max_ns) bad_range++
          last_start = s
          have_start = 1
        }
      }
      /"endTimeUnixNano"/ {
        gsub(/[^0-9]/, "", $0)
        if ($0 != "") {
          e = $0 + 0
          if (e < min_ns || e > max_ns) bad_range++
          if (have_start == 1 && e < last_start) bad_order++
          have_start = 0
        }
      }
      /"timeUnixNano"/ {
        gsub(/[^0-9]/, "", $0)
        if ($0 != "") {
          t = $0 + 0
          if (t < min_ns || t > max_ns) bad_range++
        }
      }
      END { print bad_range + 0, bad_order + 0 }
    ' "$payload_file"
  )

  if (( bad_range > 0 )); then
    die "dry-run validation failed: found ${bad_range} timestamps outside [now-1h, now]"
  fi
  if (( bad_order > 0 )); then
    die "dry-run validation failed: found ${bad_order} spans where endTime < startTime"
  fi

  local c
  for c in "${BUILD_META_TRACE_SPAN_COUNTS[@]}"; do
    if (( c < MIN_SPANS_PER_TRACE || c > MAX_SPANS_PER_TRACE )); then
      die "dry-run validation failed: generated trace with ${c} spans (outside ${MIN_SPANS_PER_TRACE}..${MAX_SPANS_PER_TRACE})"
    fi
  done

  if (( ERROR_RATE_PERCENT > 0 )); then
    if (( BUILD_META_ERROR_TRACE_COUNT == 0 )); then
      die "dry-run validation failed: expected error traces for error-rate-percent=${ERROR_RATE_PERCENT}, got none"
    fi
    grep -q '"code": 2' "$payload_file" || die "dry-run validation failed: missing status code 2 in payload"
    grep -q '"exception.type"' "$payload_file" || die "dry-run validation failed: missing exception fields in payload"
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
  local err_file="$2"

  if (( DRY_RUN == 1 )); then
    : >"$err_file"
    return 0
  fi

  local out
  if out="$(
    grpcurl -plaintext \
      -H "x-api-key: ${OTLP_API_KEY}" \
      -d @ \
      "${OTLP_GRPC}" \
      opentelemetry.proto.collector.trace.v1.TraceService/Export <"$payload_file" 2>&1
  )"; then
    : >"$err_file"
    return 0
  fi

  out="$(printf '%s\n' "$out" | sed -n '1p' | tr -d '\r')"
  if [[ -z "$out" ]]; then
    out="grpcurl request failed (no error message)"
  fi
  printf '%s|%s\n' "$(date +%s)" "$out" >"$err_file"
  return 1
}

worker_loop() {
  local worker_id="$1"
  local worker_rps="$2"
  local stats_file="$3"
  local stop_file="$4"
  local payload_file="$5"
  local err_file="$6"

  local request_seq=0
  local attempted=0
  local success=0
  local failed=0

  write_stats "$stats_file" "$attempted" "$success" "$failed"
  : >"$err_file"

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
    if send_request "$payload_file" "$err_file"; then
      success=$(( success + 1 ))
    else
      failed=$(( failed + 1 ))
    fi
    attempted=$(( attempted + SPANS_PER_REQUEST ))
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

collect_error_samples() {
  local dir="$1"
  local max_samples="${2:-3}"
  local count=0
  local f line msg
  for f in "$dir"/worker_*.err; do
    [[ -s "$f" ]] || continue
    if ! IFS= read -r line <"$f"; then
      continue
    fi
    msg="${line#*|}"
    [[ -n "$msg" ]] || continue
    printf '%s\n' "$msg"
    count=$(( count + 1 ))
    if (( count >= max_samples )); then
      break
    fi
  done
}

echo "Config:"
echo "  otlp_grpc=${OTLP_GRPC}"
echo "  target_spans_per_sec=${TARGET_SPANS_PER_SEC}"
echo "  actual_attempted_spans_per_sec=${ACTUAL_TARGET_SPS}"
echo "  duration_seconds=${DURATION_SECONDS}"
echo "  workers=${WORKERS}"
echo "  spans_per_request=${SPANS_PER_REQUEST}"
echo "  min_spans_per_trace=${MIN_SPANS_PER_TRACE}"
echo "  max_spans_per_trace=${MAX_SPANS_PER_TRACE}"
echo "  error_rate_percent=${ERROR_RATE_PERCENT}"
echo "  dry_run=${DRY_RUN}"

seed_id_pools

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/otlp-trace-load.XXXXXX")"
STOP_FILE="${TMP_DIR}/stop.flag"

if (( DRY_RUN == 1 )); then
  PAYLOAD_FILE="${TMP_DIR}/dry_run_payload.json"
  build_payload 0 0 "$PAYLOAD_FILE"
  validate_payload "$PAYLOAD_FILE"

  local_error_pct=0
  if (( BUILD_META_TRACE_COUNT > 0 )); then
    local_error_pct=$(( BUILD_META_ERROR_TRACE_COUNT * 100 / BUILD_META_TRACE_COUNT ))
  fi

  echo
  echo "Dry run success."
  echo "Generated payload: ${PAYLOAD_FILE}"
  echo "Validated:"
  echo "  - resourceSpans/scopeSpans/spans payload structure"
  echo "  - base64 traceId/spanId presence"
  echo "  - timestamps in [now-1h, now] and end >= start"
  echo "  - trace span counts in ${MIN_SPANS_PER_TRACE}..${MAX_SPANS_PER_TRACE}"
  echo "  - error traces in payload: ${BUILD_META_ERROR_TRACE_COUNT}/${BUILD_META_TRACE_COUNT} (${local_error_pct}%)"
  echo
  echo "Preview (first 80 lines):"
  sed -n '1,80p' "$PAYLOAD_FILE"
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
  err_file="${TMP_DIR}/worker_${w}.err"
  worker_loop "$w" "$worker_rps" "$stats_file" "$STOP_FILE" "$payload_file" "$err_file" &
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

  printf '[%s] attempted_spans_per_sec=%d success_req_per_sec=%d failed_req_per_sec=%d total_spans=%d\n' \
    "$(date '+%H:%M:%S')" \
    "$delta_attempted" \
    "$delta_success" \
    "$delta_failed" \
    "$total_attempted"

  if (( delta_failed > 0 )); then
    sample_errors="$(collect_error_samples "$TMP_DIR" 3 | paste -sd '; ' -)"
    if [[ -n "$sample_errors" ]]; then
      echo "  failure_samples=${sample_errors}"
    fi
  fi

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
echo "  total_attempted_spans=${final_attempted}"
echo "  total_successful_requests=${final_success}"
echo "  total_failed_requests=${final_failed}"
echo "  temp_dir=${TMP_DIR}"
