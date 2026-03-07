# Services Endpoints

- Covers `internal/defaultconfig/pages/services/tabs/overview.json`, `service-map.json`, and `topology.json`.
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
INVENTORY_CLIENT_B64="ey1Pihw+W2c="
INVENTORY_SERVER_B64="nE4aKz1PWmw="
STRIPE_CLIENT_B64="WnyR0+T2Bxg="
PAYMENTS_TRACE_B64="j0wtGpt+b1AxQlNkdYaXqA=="
PAYMENTS_ROOT_B64="vC1Ob4CRorM="

END_MS=$(($(date +%s) * 1000))
START_MS=$((END_MS - 3600000))
NOW_NS=$((END_MS * 1000000))

CHECKOUT_START_NS=$((NOW_NS - 26 * 60 * 1000000000))
CHECKOUT_END_NS=$((CHECKOUT_START_NS + 210000000))
INVENTORY_CLIENT_START_NS=$((CHECKOUT_START_NS + 25000000))
INVENTORY_CLIENT_END_NS=$((CHECKOUT_START_NS + 95000000))
INVENTORY_SERVER_START_NS=$((CHECKOUT_START_NS + 30000000))
INVENTORY_SERVER_END_NS=$((CHECKOUT_START_NS + 85000000))
STRIPE_CLIENT_START_NS=$((CHECKOUT_START_NS + 110000000))
STRIPE_CLIENT_END_NS=$((CHECKOUT_START_NS + 170000000))
PAYMENTS_START_NS=$((NOW_NS - 12 * 60 * 1000000000))
PAYMENTS_END_NS=$((PAYMENTS_START_NS + 340000000))
```

## 2. OTLP gRPC Ingestion Example For Services, Topology, And Service Map

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
          { "key": "deployment.environment", "value": { "stringValue": "production" } },
          { "key": "host.name", "value": { "stringValue": "checkout-node-01" } }
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
            },
            {
              "traceId": "${CHECKOUT_TRACE_B64}",
              "spanId": "${INVENTORY_CLIENT_B64}",
              "parentSpanId": "${CHECKOUT_ROOT_B64}",
              "name": "POST inventory-service/internal/reservations",
              "kind": 3,
              "startTimeUnixNano": "${INVENTORY_CLIENT_START_NS}",
              "endTimeUnixNano": "${INVENTORY_CLIENT_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "POST" } },
                { "key": "http.url", "value": { "stringValue": "http://inventory-service/internal/reservations" } },
                { "key": "http.host", "value": { "stringValue": "inventory-service" } },
                { "key": "http.status_code", "value": { "intValue": "200" } }
              ],
              "status": { "code": 1 }
            },
            {
              "traceId": "${CHECKOUT_TRACE_B64}",
              "spanId": "${STRIPE_CLIENT_B64}",
              "parentSpanId": "${CHECKOUT_ROOT_B64}",
              "name": "POST api.stripe.com/v1/payment_intents",
              "kind": 3,
              "startTimeUnixNano": "${STRIPE_CLIENT_START_NS}",
              "endTimeUnixNano": "${STRIPE_CLIENT_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "POST" } },
                { "key": "http.url", "value": { "stringValue": "https://api.stripe.com/v1/payment_intents" } },
                { "key": "http.host", "value": { "stringValue": "api.stripe.com" } },
                { "key": "http.status_code", "value": { "intValue": "200" } }
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
          { "key": "service.name", "value": { "stringValue": "inventory-service" } },
          { "key": "deployment.environment", "value": { "stringValue": "production" } },
          { "key": "host.name", "value": { "stringValue": "inventory-node-02" } }
        ]
      },
      "scopeSpans": [
        {
          "scope": { "name": "io.opentelemetry.spring-webmvc-6.0", "version": "2.11.0" },
          "spans": [
            {
              "traceId": "${CHECKOUT_TRACE_B64}",
              "spanId": "${INVENTORY_SERVER_B64}",
              "parentSpanId": "${INVENTORY_CLIENT_B64}",
              "name": "POST /internal/reservations",
              "kind": 2,
              "startTimeUnixNano": "${INVENTORY_SERVER_START_NS}",
              "endTimeUnixNano": "${INVENTORY_SERVER_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "POST" } },
                { "key": "http.route", "value": { "stringValue": "/internal/reservations" } },
                { "key": "http.status_code", "value": { "intValue": "200" } }
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
          { "key": "host.name", "value": { "stringValue": "payments-node-02" } }
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
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" --data-urlencode "interval=5m" "${API_BASE}/api/v1/services/timeseries"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/services/topology"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/services/external-dependencies"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" --data-urlencode "operationName=POST /api/v1/orders" "${API_BASE}/api/v1/spans/client-server-latency"
```

Useful related APIs from the same module:

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/services/metrics"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/services/summary/total"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/services/checkout-service/endpoints"
```
