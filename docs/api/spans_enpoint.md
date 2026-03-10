# Spans Endpoint

- OTLP span ingestion uses gRPC on port `4317` with `x-api-key`.
- Trace and span APIs use `Authorization: Bearer <JWT_TOKEN>`.
- The OTLP gRPC payload uses base64 for byte fields, but dashboard APIs use lower-case hex IDs.

## 1. Shared Variables

```bash
API_BASE="http://localhost:9090"
OTLP_GRPC="localhost:4317"

TOKEN="<JWT_TOKEN>"
TEAM_ID="<TEAM_ID>"
OTLP_API_KEY="<YOUR_API_KEY>"

AUTH_HEADER="Authorization: Bearer ${TOKEN}"
TEAM_HEADER="X-Team-Id: ${TEAM_ID}"

TRACE_ID_HEX="665f7e3a9c4b1d82f3a0c6e1b7d9425f"
ROOT_SPAN_HEX="a3f19c7b5d8e2f41"
CLIENT_SPAN_HEX="7b2d4f8a1c3e5b67"
SERVER_SPAN_HEX="9c4e1a2b3d4f5a6c"

TRACE_ID_B64="Zl9+OpxLHYLzoMbht9lCXw=="
ROOT_SPAN_B64="o/Gce12OL0E="
CLIENT_SPAN_B64="ey1Pihw+W2c="
SERVER_SPAN_B64="nE4aKz1PWmw="

END_MS=$(($(date +%s) * 1000))
START_MS=$((END_MS - 3600000))
NOW_NS=$((END_MS * 1000000))

TRACE_START_NS=$((NOW_NS - 18 * 60 * 1000000000))
ROOT_END_NS=$((TRACE_START_NS + 180000000))
CLIENT_START_NS=$((TRACE_START_NS + 20000000))
CLIENT_END_NS=$((TRACE_START_NS + 90000000))
SERVER_START_NS=$((TRACE_START_NS + 30000000))
SERVER_END_NS=$((TRACE_START_NS + 80000000))
SERVER_EVENT_NS=$((SERVER_END_NS - 1000000))
```

## 2. OTLP gRPC Ingestion Example For Spans

Expected response: gRPC status `OK` with an empty JSON body like `{}`.

This example creates one root server span, one client span, and one downstream server span so that trace search, span tree, and service dependency endpoints all have useful data.

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
          { "key": "k8s.namespace.name", "value": { "stringValue": "payments" } },
          { "key": "k8s.pod.name", "value": { "stringValue": "checkout-service-74486c9d7d-qzq8l" } }
        ]
      },
      "scopeSpans": [
        {
          "scope": {
            "name": "io.opentelemetry.spring-webmvc-6.0",
            "version": "2.11.0"
          },
          "spans": [
            {
              "traceId": "${TRACE_ID_B64}",
              "spanId": "${ROOT_SPAN_B64}",
              "name": "POST /api/v1/orders",
              "kind": 2,
              "startTimeUnixNano": "${TRACE_START_NS}",
              "endTimeUnixNano": "${ROOT_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "POST" } },
                { "key": "http.route", "value": { "stringValue": "/api/v1/orders" } },
                { "key": "http.url", "value": { "stringValue": "https://checkout.example.internal/api/v1/orders" } },
                { "key": "http.status_code", "value": { "intValue": "201" } }
              ],
              "status": { "code": 1 }
            },
            {
              "traceId": "${TRACE_ID_B64}",
              "spanId": "${CLIENT_SPAN_B64}",
              "parentSpanId": "${ROOT_SPAN_B64}",
              "name": "POST inventory-service/internal/reservations",
              "kind": 3,
              "startTimeUnixNano": "${CLIENT_START_NS}",
              "endTimeUnixNano": "${CLIENT_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "POST" } },
                { "key": "http.url", "value": { "stringValue": "http://inventory-service/internal/reservations" } },
                { "key": "http.host", "value": { "stringValue": "inventory-service" } },
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
          { "key": "host.name", "value": { "stringValue": "inventory-node-02" } },
          { "key": "k8s.namespace.name", "value": { "stringValue": "payments" } },
          { "key": "k8s.pod.name", "value": { "stringValue": "inventory-service-6f7f9d6d9c-k2d3r" } }
        ]
      },
      "scopeSpans": [
        {
          "scope": {
            "name": "io.opentelemetry.spring-webmvc-6.0",
            "version": "2.11.0"
          },
          "spans": [
            {
              "traceId": "${TRACE_ID_B64}",
              "spanId": "${SERVER_SPAN_B64}",
              "parentSpanId": "${CLIENT_SPAN_B64}",
              "name": "POST /internal/reservations",
              "kind": 2,
              "startTimeUnixNano": "${SERVER_START_NS}",
              "endTimeUnixNano": "${SERVER_END_NS}",
              "attributes": [
                { "key": "http.method", "value": { "stringValue": "POST" } },
                { "key": "http.route", "value": { "stringValue": "/internal/reservations" } },
                { "key": "http.status_code", "value": { "intValue": "200" } }
              ],
              "events": [
                {
                  "timeUnixNano": "${SERVER_EVENT_NS}",
                  "name": "inventory.reservation.persisted",
                  "attributes": [
                    { "key": "db.system", "value": { "stringValue": "postgresql" } },
                    { "key": "db.operation", "value": { "stringValue": "INSERT" } }
                  ]
                }
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

## 3. Dashboard API Curls For Traces And Spans

### 3.1 Traces Search

Expected response: HTTP `200 OK` with the JSON payload used by this dashboard view.

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  --data-urlencode "services=checkout-service" \
  --data-urlencode "limit=20" \
  "${API_BASE}/api/v1/traces"
```

### 3.2 Traces Page Service Time Series

Expected response: HTTP `200 OK` with the JSON payload used by this dashboard view.

This powers the request rate, error rate, and p95 latency charts on the traces landing page.

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  --data-urlencode "interval=5m" \
  "${API_BASE}/api/v1/metrics/timeseries"
```

### 3.3 Trace Spans

Expected response: HTTP `200 OK` with the JSON payload used by this dashboard view.

```bash
curl -sS \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  "${API_BASE}/api/v1/traces/${TRACE_ID_HEX}/spans"
```

### 3.4 Span Tree By Root Span ID

Expected response: HTTP `200 OK` with the JSON payload used by this dashboard view.

```bash
curl -sS \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  "${API_BASE}/api/v1/spans/${ROOT_SPAN_HEX}/tree"
```

### 3.5 Latency Histogram

Expected response: HTTP `200 OK` with the JSON payload used by this dashboard view.

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  --data-urlencode "serviceName=checkout-service" \
  --data-urlencode "operationName=POST /api/v1/orders" \
  "${API_BASE}/api/v1/latency/histogram"
```

### 3.6 Service Dependencies

Expected response: HTTP `200 OK` with the JSON payload used by this dashboard view.

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/services/dependencies"
```

### 3.7 Trace Span Events

Expected response: HTTP `200 OK` with the JSON payload used by this dashboard view.

```bash
curl -sS \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  "${API_BASE}/api/v1/traces/${TRACE_ID_HEX}/span-events"
```

### 3.8 Trace Critical Path

Expected response: HTTP `200 OK` with the JSON payload used by this dashboard view.

```bash
curl -sS \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  "${API_BASE}/api/v1/traces/${TRACE_ID_HEX}/critical-path"
```

### 3.9 Span Kind Breakdown

Expected response: HTTP `200 OK` with duration breakdown by span kind.

```bash
curl -sS \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  "${API_BASE}/api/v1/traces/${TRACE_ID_HEX}/span-kind-breakdown"
```

### 3.10 Span Self Times

Expected response: HTTP `200 OK` with self-time per span (excluding child span duration).

```bash
curl -sS \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  "${API_BASE}/api/v1/traces/${TRACE_ID_HEX}/span-self-times"
```

### 3.11 Error Path

Expected response: HTTP `200 OK` with ERROR span chain from root to leaf.

```bash
curl -sS \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  "${API_BASE}/api/v1/traces/${TRACE_ID_HEX}/error-path"
```
