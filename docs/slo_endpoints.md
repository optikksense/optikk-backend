# SLO Endpoints

- SLO uses root spans from the traces store.
- OTLP ingestion uses gRPC on `4317`.
- The dashboard API is `GET /api/v1/overview/slo`.

## 1. Shared Variables

```bash
API_BASE="http://localhost:9090"
OTLP_GRPC="localhost:4317"

TOKEN="<JWT_TOKEN>"
TEAM_ID="<TEAM_ID>"
OTLP_API_KEY="<YOUR_API_KEY>"

AUTH_HEADER="Authorization: Bearer ${TOKEN}"
TEAM_HEADER="X-Team-Id: ${TEAM_ID}"

SUCCESS_TRACE_B64="Zl9+OpxLHYLzoMbht9lCXw=="
SUCCESS_SPAN_B64="o/Gce12OL0E="
ERROR_TRACE_B64="j0wtGpt+b1AxQlNkdYaXqA=="
ERROR_SPAN_B64="vC1Ob4CRorM="

END_MS=$(($(date +%s) * 1000))
START_MS=$((END_MS - 3600000))
NOW_NS=$((END_MS * 1000000))

SUCCESS_START_NS=$((NOW_NS - 32 * 60 * 1000000000))
SUCCESS_END_NS=$((SUCCESS_START_NS + 90000000))
ERROR_START_NS=$((NOW_NS - 9 * 60 * 1000000000))
ERROR_END_NS=$((ERROR_START_NS + 240000000))
```

## 2. OTLP gRPC Ingestion Example For SLO Data

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
              "traceId": "${SUCCESS_TRACE_B64}",
              "spanId": "${SUCCESS_SPAN_B64}",
              "name": "GET /api/v1/orders/{id}",
              "kind": 2,
              "startTimeUnixNano": "${SUCCESS_START_NS}",
              "endTimeUnixNano": "${SUCCESS_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "GET" } },
                { "key": "http.route", "value": { "stringValue": "/api/v1/orders/{id}" } },
                { "key": "http.status_code", "value": { "intValue": "200" } }
              ],
              "status": { "code": 1 }
            },
            {
              "traceId": "${ERROR_TRACE_B64}",
              "spanId": "${ERROR_SPAN_B64}",
              "name": "POST /api/v1/orders",
              "kind": 2,
              "startTimeUnixNano": "${ERROR_START_NS}",
              "endTimeUnixNano": "${ERROR_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "POST" } },
                { "key": "http.route", "value": { "stringValue": "/api/v1/orders" } },
                { "key": "http.status_code", "value": { "intValue": "503" } }
              ],
              "status": { "code": 2, "message": "order repository timeout" }
            }
          ]
        }
      ]
    }
  ]
}
EOF
```

## 3. Dashboard API Curl

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  --data-urlencode "interval=5m" \
  "${API_BASE}/api/v1/overview/slo"
```

To scope the SLO view to one service:

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  --data-urlencode "interval=5m" \
  --data-urlencode "serviceName=checkout-service" \
  "${API_BASE}/api/v1/overview/slo"
```
