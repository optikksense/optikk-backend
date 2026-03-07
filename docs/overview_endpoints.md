# Overview Endpoints

- OTLP ingestion uses gRPC on `4317` with `x-api-key`.
- Overview dashboard APIs use `Authorization: Bearer <JWT_TOKEN>`.
- `start` and `end` query params are Unix milliseconds.

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

SUCCESS_START_NS=$((NOW_NS - 28 * 60 * 1000000000))
SUCCESS_END_NS=$((SUCCESS_START_NS + 85000000))
ERROR_START_NS=$((NOW_NS - 12 * 60 * 1000000000))
ERROR_END_NS=$((ERROR_START_NS + 120000000))
```

## 2. OTLP gRPC Ingestion Example For Overview Data

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
              "traceId": "${ERROR_TRACE_B64}",
              "spanId": "${ERROR_SPAN_B64}",
              "name": "POST /api/v1/payments/authorize",
              "kind": 2,
              "startTimeUnixNano": "${ERROR_START_NS}",
              "endTimeUnixNano": "${ERROR_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "POST" } },
                { "key": "http.route", "value": { "stringValue": "/api/v1/payments/authorize" } },
                { "key": "http.status_code", "value": { "intValue": "503" } }
              ],
              "status": { "code": 2, "message": "payment gateway unavailable" }
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

### 3.1 Request Rate

Expected response: HTTP `200 OK` with the JSON payload used by this dashboard view.

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/overview/request-rate"
```

### 3.2 Error Rate

Expected response: HTTP `200 OK` with the JSON payload used by this dashboard view.

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/overview/error-rate"
```

### 3.3 p95 Latency

Expected response: HTTP `200 OK` with the JSON payload used by this dashboard view.

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/overview/p95-latency"
```

### 3.4 Services Table

Expected response: HTTP `200 OK` with the JSON payload used by this dashboard view.

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/overview/services"
```

### 3.5 Top Endpoints

Expected response: HTTP `200 OK` with the JSON payload used by this dashboard view.

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/overview/top-endpoints"
```

### 3.6 Endpoint Time Series Drilldown

Expected response: HTTP `200 OK` with the JSON payload used by this dashboard view.

```bash
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" \
  --data-urlencode "serviceName=checkout-service" \
  "${API_BASE}/api/v1/overview/endpoints/timeseries"
```
