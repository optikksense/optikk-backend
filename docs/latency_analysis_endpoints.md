# Latency Analysis Endpoints

- Covers `internal/defaultconfig/pages/metrics/tabs/latency-analysis.json`.
- OTLP ingestion uses gRPC on `4317`.
- Dashboard APIs use `Authorization: Bearer <JWT_TOKEN>` and `X-Team-Id`.

## 1. Shared Variables

```bash
API_BASE="http://localhost:9090"
OTLP_GRPC="localhost:4317"

TOKEN="<JWT_TOKEN>"
TEAM_ID="<TEAM_ID>"
OTLP_API_KEY="<YOUR_API_KEY>"

AUTH_HEADER="Authorization: Bearer ${TOKEN}"
TEAM_HEADER="X-Team-Id: ${TEAM_ID}"

TRACE_ID_B64="Zl9+OpxLHYLzoMbht9lCXw=="
ROOT_SPAN_B64="o/Gce12OL0E="
SLOW_TRACE_B64="j0wtGpt+b1AxQlNkdYaXqA=="
SLOW_SPAN_B64="vC1Ob4CRorM="

END_MS=$(($(date +%s) * 1000))
START_MS=$((END_MS - 3600000))
NOW_NS=$((END_MS * 1000000))

FAST_START_NS=$((NOW_NS - 21 * 60 * 1000000000))
FAST_END_NS=$((FAST_START_NS + 85000000))
SLOW_START_NS=$((NOW_NS - 9 * 60 * 1000000000))
SLOW_END_NS=$((SLOW_START_NS + 880000000))
```

## 2. OTLP gRPC Ingestion Example For Latency Views

Expected response: gRPC status `OK` with an empty JSON body like `{}`.

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
          { "key": "service.name", "value": { "stringValue": "checkout-service" } },
          { "key": "deployment.environment", "value": { "stringValue": "production" } }
        ]
      },
      "scopeSpans": [
        {
          "scope": { "name": "io.opentelemetry.spring-webmvc-6.0", "version": "2.11.0" },
          "spans": [
            {
              "traceId": "${TRACE_ID_B64}",
              "spanId": "${ROOT_SPAN_B64}",
              "name": "POST /api/v1/orders",
              "kind": 2,
              "startTimeUnixNano": "${FAST_START_NS}",
              "endTimeUnixNano": "${FAST_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "POST" } },
                { "key": "http.route", "value": { "stringValue": "/api/v1/orders" } },
                { "key": "http.status_code", "value": { "intValue": "201" } }
              ],
              "status": { "code": 1 }
            },
            {
              "traceId": "${SLOW_TRACE_B64}",
              "spanId": "${SLOW_SPAN_B64}",
              "name": "POST /api/v1/orders",
              "kind": 2,
              "startTimeUnixNano": "${SLOW_START_NS}",
              "endTimeUnixNano": "${SLOW_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "POST" } },
                { "key": "http.route", "value": { "stringValue": "/api/v1/orders" } },
                { "key": "http.status_code", "value": { "intValue": "503" } }
              ],
              "status": { "code": 2, "message": "inventory lock timeout" }
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

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" --data-urlencode "serviceName=checkout-service" --data-urlencode "operationName=POST /api/v1/orders" "${API_BASE}/api/v1/latency/histogram"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" --data-urlencode "interval=5m" --data-urlencode "serviceName=checkout-service" "${API_BASE}/api/v1/latency/heatmap"
```
