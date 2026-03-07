# Logs Endpoint

- OTLP log ingestion uses gRPC on port `4317` with `x-api-key`.
- Logs dashboard APIs use `Authorization: Bearer <JWT_TOKEN>`.
- The OTLP gRPC payload uses base64 for `traceId` and `spanId`, but dashboard query params use the normal lower-case hex form.

## 1. Shared Variables

```bash
API_BASE="http://localhost:9090"
OTLP_GRPC="localhost:4317"

TOKEN="<JWT_TOKEN>"
TEAM_ID="<TEAM_ID>"
OTLP_API_KEY="<YOUR_API_KEY>"

AUTH_HEADER="Authorization: Bearer ${TOKEN}"
TEAM_HEADER="X-Team-Id: ${TEAM_ID}"

SERVICE_NAME="orders-service"
TRACE_ID_HEX="665f7e3a9c4b1d82f3a0c6e1b7d9425f"
ROOT_SPAN_HEX="a3f19c7b5d8e2f41"

TRACE_ID_B64="Zl9+OpxLHYLzoMbht9lCXw=="
ROOT_SPAN_B64="o/Gce12OL0E="

END_MS=$(($(date +%s) * 1000))
START_MS=$((END_MS - 3600000))
NOW_NS=$((END_MS * 1000000))

LOG_TS_NS=$((NOW_NS - 12 * 60 * 1000000000))
LOG_OBSERVED_NS=$((LOG_TS_NS + 1000000))
LOG_WARN_NS=$((LOG_TS_NS + 10000000))
LOG_ERROR_NS=$((LOG_TS_NS + 20000000))
LOG_TS_MS=$((LOG_TS_NS / 1000000))
```

## 2. OTLP gRPC Ingestion Example For Logs

```bash
grpcurl -plaintext \
  -H "x-api-key: ${OTLP_API_KEY}" \
  -d @ \
  "${OTLP_GRPC}" \
  opentelemetry.proto.collector.logs.v1.LogsService/Export <<EOF
{
  "resourceLogs": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "${SERVICE_NAME}" } },
          { "key": "deployment.environment", "value": { "stringValue": "production" } },
          { "key": "host.name", "value": { "stringValue": "orders-node-01" } },
          { "key": "k8s.namespace.name", "value": { "stringValue": "payments" } },
          { "key": "k8s.pod.name", "value": { "stringValue": "orders-service-7f6d8b9d6d-2xw4m" } },
          { "key": "container.name", "value": { "stringValue": "orders-service" } },
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
            {
              "timeUnixNano": "${LOG_TS_NS}",
              "observedTimeUnixNano": "${LOG_OBSERVED_NS}",
              "severityNumber": 9,
              "severityText": "INFO",
              "body": { "stringValue": "Order created successfully for customer 1042" },
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "POST" } },
                { "key": "http.route", "value": { "stringValue": "/api/v1/orders" } },
                { "key": "http.status_code", "value": { "intValue": "201" } },
                { "key": "logger.name", "value": { "stringValue": "com.example.orders.OrderController" } }
              ],
              "traceId": "${TRACE_ID_B64}",
              "spanId": "${ROOT_SPAN_B64}"
            },
            {
              "timeUnixNano": "${LOG_WARN_NS}",
              "observedTimeUnixNano": "${LOG_WARN_NS}",
              "severityNumber": 13,
              "severityText": "WARN",
              "body": { "stringValue": "Inventory reservation latency crossed 250ms" },
              "attributes": [
                { "key": "component", "value": { "stringValue": "inventory-client" } },
                { "key": "http.route", "value": { "stringValue": "/internal/reservations" } }
              ],
              "traceId": "${TRACE_ID_B64}",
              "spanId": "${ROOT_SPAN_B64}"
            },
            {
              "timeUnixNano": "${LOG_ERROR_NS}",
              "observedTimeUnixNano": "${LOG_ERROR_NS}",
              "severityNumber": 17,
              "severityText": "ERROR",
              "body": { "stringValue": "Payment authorization retry exhausted" },
              "attributes": [
                { "key": "logger.name", "value": { "stringValue": "com.example.orders.PaymentGatewayClient" } },
                { "key": "retry.count", "value": { "intValue": "3" } }
              ],
              "traceId": "${TRACE_ID_B64}",
              "spanId": "${ROOT_SPAN_B64}"
            }
          ]
        }
      ]
    }
  ]
}
EOF
```

## 3. Dashboard API Curls For Logs

### 3.1 Logs Histogram

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  --data-urlencode "services=${SERVICE_NAME}" \
  --data-urlencode "step=1m" \
  "${API_BASE}/api/v1/logs/histogram"
```

### 3.2 Logs Search

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  --data-urlencode "services=${SERVICE_NAME}" \
  --data-urlencode "search=Order" \
  --data-urlencode "limit=50" \
  "${API_BASE}/api/v1/logs"
```

### 3.3 Logs Stats

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  --data-urlencode "services=${SERVICE_NAME}" \
  "${API_BASE}/api/v1/logs/stats"
```

### 3.4 Logs Facets

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  --data-urlencode "services=${SERVICE_NAME}" \
  "${API_BASE}/api/v1/logs/facets"
```

### 3.5 Trace-Scoped Logs

```bash
curl -sS \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  "${API_BASE}/api/v1/traces/${TRACE_ID_HEX}/logs"
```

### 3.6 Log Detail Around A Span

`timestamp` here is milliseconds, even though the stored log timestamp is nanoseconds.

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "traceId=${TRACE_ID_HEX}" \
  --data-urlencode "spanId=${ROOT_SPAN_HEX}" \
  --data-urlencode "timestamp=${LOG_TS_MS}" \
  --data-urlencode "contextWindow=30" \
  "${API_BASE}/api/v1/logs/detail"
```
