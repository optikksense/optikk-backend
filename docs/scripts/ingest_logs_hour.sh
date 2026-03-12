#!/bin/bash

##############################################################################
# Log Ingestion Script: Generates realistic logs over 1 hour
# - Multiple services and log levels (DEBUG, INFO, WARN, ERROR)
# - Realistic scenarios: orders, payments, inventory, auth, caching, db
# - Uses OTLP gRPC to send logs to the observability backend
##############################################################################

set -o pipefail

# ============================================================================
# Configuration
# ============================================================================

API_BASE="${API_BASE:-http://localhost:9090}"
OTLP_GRPC="${OTLP_GRPC:-localhost:4317}"
OTLP_API_KEY="${OTLP_API_KEY:-test-api-key}"
JWT_TOKEN="${JWT_TOKEN:-test-jwt-token}"

# Services to generate logs for
SERVICES=(
  "orders-service"
  "payments-service"
  "inventory-service"
  "auth-service"
  "cache-service"
  "database-service"
)

# ============================================================================
# Helper Functions
# ============================================================================

# Generate a random trace ID (hex, 32 chars = 16 bytes)
generate_trace_id() {
  openssl rand -hex 16
}

# Generate a random span ID (hex, 16 chars = 8 bytes)
generate_span_id() {
  openssl rand -hex 8
}

# Convert hex to base64 for gRPC
hex_to_b64() {
  echo -n "$1" | xxd -r -p | base64
}

# Get timestamp in nanoseconds for a given time offset (seconds from now)
get_ts_ns() {
  local offset_sec=${1:-0}
  local now_sec=$(date +%s)
  local ts_sec=$((now_sec + offset_sec))
  echo $((ts_sec * 1000000000))
}

# ============================================================================
# Log Generators - Each returns a complete logRecord JSON object
# ============================================================================

generate_debug_log() {
  local service=$1
  local ts_ns=$2
  local trace_id_b64=$3
  local span_id_b64=$4

  cat <<EOF
{
  "timeUnixNano": "$ts_ns",
  "observedTimeUnixNano": "$ts_ns",
  "severityNumber": 5,
  "severityText": "DEBUG",
  "body": { "stringValue": "Executing database query for service=$service, execution_plan=optimized" },
  "attributes": [
    { "key": "logger.name", "value": { "stringValue": "com.example.$service.QueryExecutor" } },
    { "key": "db.operation", "value": { "stringValue": "SELECT" } },
    { "key": "db.statement", "value": { "stringValue": "SELECT * FROM orders WHERE customer_id = ?" } },
    { "key": "request.id", "value": { "stringValue": "req-$(date +%s%N | md5sum | cut -c1-8)" } }
  ],
  "traceId": "$trace_id_b64",
  "spanId": "$span_id_b64"
}
EOF
}

generate_info_log() {
  local service=$1
  local ts_ns=$2
  local trace_id_b64=$3
  local span_id_b64=$4
  local msg=$5

  cat <<EOF
{
  "timeUnixNano": "$ts_ns",
  "observedTimeUnixNano": "$ts_ns",
  "severityNumber": 9,
  "severityText": "INFO",
  "body": { "stringValue": "$msg" },
  "attributes": [
    { "key": "logger.name", "value": { "stringValue": "com.example.$service.Handler" } },
    { "key": "http.method", "value": { "stringValue": "POST" } },
    { "key": "http.status_code", "value": { "intValue": "200" } },
    { "key": "duration_ms", "value": { "intValue": "$((RANDOM % 500))" } }
  ],
  "traceId": "$trace_id_b64",
  "spanId": "$span_id_b64"
}
EOF
}

generate_warn_log() {
  local service=$1
  local ts_ns=$2
  local trace_id_b64=$3
  local span_id_b64=$4
  local msg=$5

  cat <<EOF
{
  "timeUnixNano": "$ts_ns",
  "observedTimeUnixNano": "$ts_ns",
  "severityNumber": 13,
  "severityText": "WARN",
  "body": { "stringValue": "$msg" },
  "attributes": [
    { "key": "logger.name", "value": { "stringValue": "com.example.$service.Monitor" } },
    { "key": "component", "value": { "stringValue": "external-api" } },
    { "key": "retry.count", "value": { "intValue": "$((RANDOM % 5))" } },
    { "key": "latency_ms", "value": { "intValue": "$((200 + RANDOM % 300))" } }
  ],
  "traceId": "$trace_id_b64",
  "spanId": "$span_id_b64"
}
EOF
}

generate_error_log() {
  local service=$1
  local ts_ns=$2
  local trace_id_b64=$3
  local span_id_b64=$4
  local msg=$5
  local exc_type=$6

  cat <<EOF
{
  "timeUnixNano": "$ts_ns",
  "observedTimeUnixNano": "$ts_ns",
  "severityNumber": 17,
  "severityText": "ERROR",
  "body": { "stringValue": "$msg" },
  "attributes": [
    { "key": "logger.name", "value": { "stringValue": "com.example.$service.ErrorHandler" } },
    { "key": "exception.type", "value": { "stringValue": "$exc_type" } },
    { "key": "exception.message", "value": { "stringValue": "$msg" } },
    { "key": "exception.stacktrace", "value": { "stringValue": "at com.example.$service.process(Service.java:$((RANDOM % 500)))\nat com.example.$service.handle(Service.java:$((RANDOM % 500)))" } },
    { "key": "http.status_code", "value": { "intValue": "500" } },
    { "key": "request.id", "value": { "stringValue": "req-$(date +%s%N | md5sum | cut -c1-8)" } }
  ],
  "traceId": "$trace_id_b64",
  "spanId": "$span_id_b64"
}
EOF
}

# ============================================================================
# Send Logs via OTLP gRPC
# ============================================================================

send_logs() {
  local service=$1
  shift
  local log_records=("$@")

  # Build the complete request
  local request=$(cat <<EOF
{
  "resourceLogs": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "$service" } },
          { "key": "deployment.environment", "value": { "stringValue": "production" } },
          { "key": "telemetry.sdk.language", "value": { "stringValue": "java" } },
          { "key": "telemetry.sdk.name", "value": { "stringValue": "opentelemetry" } }
        ]
      },
      "scopeLogs": [
        {
          "scope": {
            "name": "io.opentelemetry.javaagent.logback-mdc-1.0",
            "version": "2.11.0"
          },
          "logRecords": [
            $(IFS=,; echo "${log_records[*]}")
          ]
        }
      ]
    }
  ]
}
EOF
)

  # Send via grpcurl
  if ! echo "$request" | grpcurl -plaintext \
    -H "x-api-key: ${OTLP_API_KEY}" \
    -d @ \
    "${OTLP_GRPC}" \
    opentelemetry.proto.collector.logs.v1.LogsService/Export > /dev/null 2>&1; then
    echo "⚠ Warning: Failed to send logs to ${OTLP_GRPC}" >&2
  fi
}

# ============================================================================
# Main Loop - Generate logs every 5 seconds for ~1 hour
# ============================================================================

echo "Starting log ingestion loop..."
echo "Target: ${OTLP_GRPC}"
echo "API Key: ${OTLP_API_KEY}"
echo "Duration: ~1 hour (logs every 5 seconds)"
echo ""

iteration=0
max_iterations=720  # 720 * 5 seconds = 1 hour

while [ $iteration -lt $max_iterations ]; do
  iteration=$((iteration + 1))

  # Pick a random service
  service=${SERVICES[$RANDOM % ${#SERVICES[@]}]}

  # Generate trace and span IDs
  trace_id_hex=$(generate_trace_id)
  trace_id_b64=$(hex_to_b64 "$trace_id_hex")
  span_id_hex=$(generate_span_id)
  span_id_b64=$(hex_to_b64 "$span_id_hex")

  # Timestamp for this batch (current time + random offset within this 5-second window)
  offset=$((RANDOM % 5))
  ts_ns=$(get_ts_ns "$((iteration * 5 + offset))")

  # Generate random logs for this iteration
  declare -a logs

  case $((RANDOM % 20)) in
    0|1)
      # ERROR logs (10% chance)
      logs[0]=$(generate_error_log "$service" "$ts_ns" "$trace_id_b64" "$span_id_b64" \
        "Connection timeout to external API" "java.net.SocketTimeoutException")
      logs[1]=$(generate_error_log "$service" "$ts_ns" "$trace_id_b64" "$span_id_b64" \
        "Database connection pool exhausted" "java.sql.SQLException")
      ;;
    2|3|4)
      # WARN logs (15% chance)
      logs[0]=$(generate_warn_log "$service" "$ts_ns" "$trace_id_b64" "$span_id_b64" \
        "High latency detected on inventory-service call: 450ms")
      logs[1]=$(generate_info_log "$service" "$ts_ns" "$trace_id_b64" "$span_id_b64" \
        "Cache miss rate exceeds threshold: 35%")
      logs[2]=$(generate_warn_log "$service" "$ts_ns" "$trace_id_b64" "$span_id_b64" \
        "Memory usage approaching limit: 85% of heap")
      ;;
    5|6|7|8|9)
      # INFO logs (25% chance)
      logs[0]=$(generate_info_log "$service" "$ts_ns" "$trace_id_b64" "$span_id_b64" \
        "Order #$((RANDOM + 10000)) created successfully")
      logs[1]=$(generate_info_log "$service" "$ts_ns" "$trace_id_b64" "$span_id_b64" \
        "Payment processed for customer $((RANDOM + 1000))")
      logs[2]=$(generate_info_log "$service" "$ts_ns" "$trace_id_b64" "$span_id_b64" \
        "Inventory updated: sku-$((RANDOM)), qty=$((RANDOM % 100))")
      ;;
    *)
      # DEBUG logs (50% chance)
      logs[0]=$(generate_debug_log "$service" "$ts_ns" "$trace_id_b64" "$span_id_b64")
      logs[1]=$(generate_debug_log "$service" "$ts_ns" "$trace_id_b64" "$span_id_b64")
      ;;
  esac

  # Send the batch
  send_logs "$service" "${logs[@]}"

  # Progress indicator
  percent=$((iteration * 100 / max_iterations))
  printf "\r[%-50s] %3d%% (%d/%d iterations)" \
    "$(printf '#%.0s' $(seq 1 $((percent / 2))))" \
    "$percent" "$iteration" "$max_iterations"

  # Wait 5 seconds before next batch
  sleep 5
done

echo ""
echo ""
echo "✓ Log ingestion complete!"
echo "  Generated logs from $((max_iterations)) iterations across ${#SERVICES[@]} services"
echo "  Approximate duration: 1 hour"
echo ""
echo "Verify logs in dashboard:"
echo "  - Logs search: ${API_BASE}/api/v1/logs"
echo "  - Logs histogram: ${API_BASE}/api/v1/logs/histogram"
echo "  - Logs stats: ${API_BASE}/api/v1/logs/stats"
