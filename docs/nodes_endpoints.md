# Nodes Endpoints

- Covers `internal/defaultconfig/pages/infrastructure/tabs/nodes.json`.
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

END_MS=$(($(date +%s) * 1000))
START_MS=$((END_MS - 3600000))
NOW_NS=$((END_MS * 1000000))

CHECKOUT_START_NS=$((NOW_NS - 24 * 60 * 1000000000))
CHECKOUT_END_NS=$((CHECKOUT_START_NS + 82000000))
PAYMENTS_START_NS=$((NOW_NS - 10 * 60 * 1000000000))
PAYMENTS_END_NS=$((PAYMENTS_START_NS + 260000000))
```

## 2. OTLP gRPC Ingestion Example For Node Health

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
          { "key": "deployment.environment", "value": { "stringValue": "production" } },
          { "key": "host.name", "value": { "stringValue": "checkout-node-01" } },
          { "key": "k8s.pod.name", "value": { "stringValue": "checkout-service-74486c9d7d-qzq8l" } },
          { "key": "container.name", "value": { "stringValue": "checkout-service" } }
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
          { "key": "deployment.environment", "value": { "stringValue": "production" } },
          { "key": "host.name", "value": { "stringValue": "payments-node-02" } },
          { "key": "k8s.pod.name", "value": { "stringValue": "payments-service-5fd85b7d7f-rv5p2" } },
          { "key": "container.name", "value": { "stringValue": "payments-service" } }
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
                { "key": "http.status_code", "value": { "intValue": "503" } }
              ],
              "status": { "code": 2, "message": "gateway unavailable" }
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
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/nodes/summary"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/nodes"
```

Useful drilldown:

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/nodes/checkout-node-01/services"
```
