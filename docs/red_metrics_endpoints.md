# RED Metrics Endpoints

- Covers `internal/defaultconfig/pages/metrics/tabs/red-metrics.json`.
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

CHECKOUT_TRACE_B64="Zl9+OpxLHYLzoMbht9lCXw=="
CHECKOUT_ROOT_B64="o/Gce12OL0E="
PAYMENTS_TRACE_B64="j0wtGpt+b1AxQlNkdYaXqA=="
PAYMENTS_ROOT_B64="vC1Ob4CRorM="
INVENTORY_TRACE_B64="bo8QKThKW2x9jp+gscLT5A=="
INVENTORY_ROOT_B64="HS4/QFFic4Q="

END_MS=$(($(date +%s) * 1000))
START_MS=$((END_MS - 3600000))
NOW_NS=$((END_MS * 1000000))

CHECKOUT_START_NS=$((NOW_NS - 28 * 60 * 1000000000))
CHECKOUT_END_NS=$((CHECKOUT_START_NS + 95000000))
PAYMENTS_START_NS=$((NOW_NS - 19 * 60 * 1000000000))
PAYMENTS_END_NS=$((PAYMENTS_START_NS + 420000000))
INVENTORY_START_NS=$((NOW_NS - 11 * 60 * 1000000000))
INVENTORY_END_NS=$((INVENTORY_START_NS + 55000000))
```

## 2. OTLP gRPC Ingestion Example For RED Metrics

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
              "traceId": "${CHECKOUT_TRACE_B64}",
              "spanId": "${CHECKOUT_ROOT_B64}",
              "name": "POST /api/v1/orders",
              "kind": 2,
              "startTimeUnixNano": "${CHECKOUT_START_NS}",
              "endTimeUnixNano": "${CHECKOUT_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "POST" } },
                { "key": "http.route", "value": { "stringValue": "/api/v1/orders" } },
                { "key": "http.status_code", "value": { "intValue": "201" } }
              ],
              "status": { "code": 1 }
            }
          ]
        }
      ]
    },
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
              "traceId": "${PAYMENTS_TRACE_B64}",
              "spanId": "${PAYMENTS_ROOT_B64}",
              "name": "POST /api/v1/payments/authorize",
              "kind": 2,
              "startTimeUnixNano": "${PAYMENTS_START_NS}",
              "endTimeUnixNano": "${PAYMENTS_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "POST" } },
                { "key": "http.route", "value": { "stringValue": "/api/v1/payments/authorize" } },
                { "key": "http.status_code", "value": { "intValue": "503" } },
                { "key": "exception.type", "value": { "stringValue": "java.net.SocketTimeoutException" } }
              ],
              "status": { "code": 2, "message": "gateway timeout" }
            }
          ]
        }
      ]
    },
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "inventory-service" } },
          { "key": "deployment.environment", "value": { "stringValue": "production" } }
        ]
      },
      "scopeSpans": [
        {
          "scope": { "name": "io.opentelemetry.spring-webmvc-6.0", "version": "2.11.0" },
          "spans": [
            {
              "traceId": "${INVENTORY_TRACE_B64}",
              "spanId": "${INVENTORY_ROOT_B64}",
              "name": "GET /internal/inventory/{sku}",
              "kind": 2,
              "startTimeUnixNano": "${INVENTORY_START_NS}",
              "endTimeUnixNano": "${INVENTORY_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "GET" } },
                { "key": "http.route", "value": { "stringValue": "/internal/inventory/{sku}" } },
                { "key": "http.status_code", "value": { "intValue": "200" } }
              ],
              "status": { "code": 1 }
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
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/spans/service-scorecard"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" --data-urlencode "interval=5m" "${API_BASE}/api/v1/services/timeseries"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" --data-urlencode "satisfied_ms=300" --data-urlencode "tolerating_ms=1200" "${API_BASE}/api/v1/spans/apdex"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/spans/http-status-distribution"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" --data-urlencode "serviceName=checkout-service" --data-urlencode "operationName=POST /api/v1/orders" "${API_BASE}/api/v1/latency/histogram"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" --data-urlencode "limit=20" "${API_BASE}/api/v1/spans/top-slow-operations"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" --data-urlencode "limit=20" "${API_BASE}/api/v1/spans/top-error-operations"
```
