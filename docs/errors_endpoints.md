# Errors Endpoints

- Error dashboard data comes from root spans and span error attributes.
- OTLP ingestion uses gRPC on `4317`.
- Dashboard APIs use `Authorization: Bearer <JWT_TOKEN>`.

## 1. Shared Variables

```bash
API_BASE="http://localhost:9090"
OTLP_GRPC="localhost:4317"

TOKEN="<JWT_TOKEN>"
TEAM_ID="<TEAM_ID>"
OTLP_API_KEY="<YOUR_API_KEY>"

AUTH_HEADER="Authorization: Bearer ${TOKEN}"
TEAM_HEADER="X-Team-Id: ${TEAM_ID}"

ERROR_TRACE_HEX="8f4c2d1a9b7e6f5031425364758697a8"
ERROR_TRACE_B64="j0wtGpt+b1AxQlNkdYaXqA=="
ERROR_SPAN_B64="vC1Ob4CRorM="

END_MS=$(($(date +%s) * 1000))
START_MS=$((END_MS - 3600000))
NOW_NS=$((END_MS * 1000000))

ERROR_START_NS=$((NOW_NS - 11 * 60 * 1000000000))
ERROR_END_NS=$((ERROR_START_NS + 260000000))
ERROR_EVENT_NS=$((ERROR_END_NS - 2000000))
```

## 2. OTLP gRPC Ingestion Example For Error Data

`exception.type` and `exception.message` are added as span attributes because the error-tracking endpoints read them from the stored span row.

```bash
grpcurl -plaintext \
  -H "x-api-key: ${OTLP_API_KEY}" \
  -d @ \
  "${OTLP_GRPC}" \
  opentelemetry.proto.collector.trace.v1.TraceService/Export <<EOF
{
  "resourceSpans": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "payments-service" } },
          { "key": "deployment.environment", "value": { "stringValue": "production" } }
        ]
      },
      "scopeSpans": [
        {
          "scope": { "name": "io.opentelemetry.spring-webmvc-6.0", "version": "2.11.0" },
          "spans": [
            {
              "traceId": "${ERROR_TRACE_B64}",
              "spanId": "${ERROR_SPAN_B64}",
              "name": "POST /api/v1/payments/authorize",
              "kind": 2,
              "startTimeUnixNano": "${ERROR_START_NS}",
              "endTimeUnixNano": "${ERROR_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "POST" } },
                { "key": "http.route", "value": { "stringValue": "/api/v1/payments/authorize" } },
                { "key": "http.status_code", "value": { "intValue": "503" } },
                { "key": "exception.type", "value": { "stringValue": "java.net.SocketTimeoutException" } },
                { "key": "exception.message", "value": { "stringValue": "Read timed out from upstream gateway" } },
                { "key": "exception.stacktrace", "value": { "stringValue": "java.net.SocketTimeoutException: Read timed out" } }
              ],
              "events": [
                {
                  "timeUnixNano": "${ERROR_EVENT_NS}",
                  "name": "exception",
                  "attributes": [
                    { "key": "exception.type", "value": { "stringValue": "java.net.SocketTimeoutException" } }
                  ]
                }
              ],
              "status": { "code": 2, "message": "upstream gateway timeout" }
            }
          ]
        }
      ]
    }
  ]
}
EOF
```

## 3. Dashboard API Curls

### 3.1 Service Error Rate

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/overview/errors/service-error-rate"
```

### 3.2 Error Volume

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/overview/errors/error-volume"
```

### 3.3 Latency During Error Windows

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/overview/errors/latency-during-error-windows"
```

### 3.4 Error Groups

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" \
  --data-urlencode "limit=50" \
  "${API_BASE}/api/v1/errors/groups"
```

### 3.5 Exception Rate By Type

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/spans/exception-rate-by-type"
```

### 3.6 Error Hotspot

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/spans/error-hotspot"
```

### 3.7 HTTP 5xx By Route

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/spans/http-5xx-by-route"
```
