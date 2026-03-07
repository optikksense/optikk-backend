# HTTP Endpoints

- Covers `internal/defaultconfig/pages/metrics/tabs/http.json`.
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

END_MS=$(($(date +%s) * 1000))
START_MS=$((END_MS - 3600000))
NOW_NS=$((END_MS * 1000000))

HTTP_HIST_START_NS=$((NOW_NS - 25 * 60 * 1000000000))
HTTP_HIST_END_NS=$((HTTP_HIST_START_NS + 18000000))
HTTP_GAUGE_NS=$((NOW_NS - 8 * 60 * 1000000000))
```

## 2. OTLP gRPC Ingestion Example For HTTP Metrics

```bash
grpcurl -plaintext \
  -H "x-api-key: ${OTLP_API_KEY}" \
  -d @ \
  "${OTLP_GRPC}" \
  opentelemetry.proto.collector.metrics.v1.MetricsService/Export <<EOF
{
  "resourceMetrics": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "checkout-service" } },
          { "key": "deployment.environment", "value": { "stringValue": "production" } }
        ]
      },
      "scopeMetrics": [
        {
          "scope": { "name": "io.opentelemetry.http", "version": "2.11.0" },
          "metrics": [
            {
              "name": "http.server.request.duration",
              "unit": "ms",
              "histogram": {
                "aggregationTemporality": 2,
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "http.request.method", "value": { "stringValue": "POST" } },
                      { "key": "http.route", "value": { "stringValue": "/api/v1/orders" } },
                      { "key": "http.response.status_code", "value": { "stringValue": "201" } }
                    ],
                    "startTimeUnixNano": "${HTTP_HIST_START_NS}",
                    "timeUnixNano": "${HTTP_HIST_END_NS}",
                    "count": 42,
                    "sum": 1680.0,
                    "bucketCounts": [9, 21, 12],
                    "explicitBounds": [25, 60]
                  },
                  {
                    "attributes": [
                      { "key": "http.request.method", "value": { "stringValue": "POST" } },
                      { "key": "http.route", "value": { "stringValue": "/api/v1/orders" } },
                      { "key": "http.response.status_code", "value": { "stringValue": "503" } }
                    ],
                    "startTimeUnixNano": "${HTTP_HIST_START_NS}",
                    "timeUnixNano": "${HTTP_HIST_END_NS}",
                    "count": 4,
                    "sum": 920.0,
                    "bucketCounts": [0, 1, 3],
                    "explicitBounds": [80, 180]
                  }
                ]
              }
            },
            {
              "name": "http.server.active_requests",
              "unit": "{request}",
              "gauge": {
                "dataPoints": [
                  {
                    "timeUnixNano": "${HTTP_GAUGE_NS}",
                    "asDouble": 6
                  }
                ]
              }
            },
            {
              "name": "http.client.request.duration",
              "unit": "ms",
              "histogram": {
                "aggregationTemporality": 2,
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "http.request.method", "value": { "stringValue": "POST" } },
                      { "key": "http.route", "value": { "stringValue": "/internal/reservations" } },
                      { "key": "http.response.status_code", "value": { "stringValue": "200" } }
                    ],
                    "startTimeUnixNano": "${HTTP_HIST_START_NS}",
                    "timeUnixNano": "${HTTP_HIST_END_NS}",
                    "count": 18,
                    "sum": 540.0,
                    "bucketCounts": [4, 9, 5],
                    "explicitBounds": [15, 40]
                  }
                ]
              }
            },
            {
              "name": "dns.lookup.duration",
              "unit": "ms",
              "histogram": {
                "aggregationTemporality": 2,
                "dataPoints": [
                  {
                    "startTimeUnixNano": "${HTTP_HIST_START_NS}",
                    "timeUnixNano": "${HTTP_HIST_END_NS}",
                    "count": 10,
                    "sum": 31.0,
                    "bucketCounts": [5, 4, 1],
                    "explicitBounds": [2, 5]
                  }
                ]
              }
            },
            {
              "name": "tls.connect.duration",
              "unit": "ms",
              "histogram": {
                "aggregationTemporality": 2,
                "dataPoints": [
                  {
                    "startTimeUnixNano": "${HTTP_HIST_START_NS}",
                    "timeUnixNano": "${HTTP_HIST_END_NS}",
                    "count": 10,
                    "sum": 84.0,
                    "bucketCounts": [3, 5, 2],
                    "explicitBounds": [5, 12]
                  }
                ]
              }
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
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/http/request-rate"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/http/request-duration"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/http/active-requests"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/http/client-duration"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/http/dns-duration"
```
