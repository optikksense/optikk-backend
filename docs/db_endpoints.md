# Database Endpoints

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

DB_HIST_START_NS=$((NOW_NS - 26 * 60 * 1000000000))
DB_HIST_END_NS=$((DB_HIST_START_NS + 18000000))
DB_CONN_NS=$((NOW_NS - 16 * 60 * 1000000000))
```

## 2. OTLP gRPC Ingestion Example For Database Metrics

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
          "scope": { "name": "io.opentelemetry.jdbc", "version": "2.11.0" },
          "metrics": [
            {
              "name": "mongodb.driver.commands",
              "unit": "ms",
              "histogram": {
                "aggregationTemporality": 2,
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "db.system", "value": { "stringValue": "mongodb" } },
                      { "key": "db.name", "value": { "stringValue": "orders" } },
                      { "key": "db.mongodb.collection", "value": { "stringValue": "orders" } },
                      { "key": "db.operation", "value": { "stringValue": "find" } }
                    ],
                    "startTimeUnixNano": "${DB_HIST_START_NS}",
                    "timeUnixNano": "${DB_HIST_END_NS}",
                    "count": 24,
                    "sum": 48.0,
                    "bucketCounts": [8, 10, 6],
                    "explicitBounds": [1, 3]
                  }
                ]
              }
            },
            {
              "name": "hikaricp.connections.usage",
              "unit": "ms",
              "histogram": {
                "aggregationTemporality": 2,
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "db.system", "value": { "stringValue": "postgresql" } },
                      { "key": "db.sql.table", "value": { "stringValue": "orders" } },
                      { "key": "pool", "value": { "stringValue": "orders-hikari" } }
                    ],
                    "startTimeUnixNano": "${DB_HIST_START_NS}",
                    "timeUnixNano": "${DB_HIST_END_NS}",
                    "count": 16,
                    "sum": 22.4,
                    "bucketCounts": [5, 7, 4],
                    "explicitBounds": [1, 2]
                  }
                ]
              }
            },
            {
              "name": "db.cache.hits",
              "unit": "{hit}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [{ "timeUnixNano": "${DB_CONN_NS}", "asDouble": 340 }]
              }
            },
            {
              "name": "db.cache.misses",
              "unit": "{miss}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [{ "timeUnixNano": "${DB_CONN_NS}", "asDouble": 12 }]
              }
            },
            {
              "name": "db.replication.lag.ms",
              "unit": "ms",
              "gauge": { "dataPoints": [{ "timeUnixNano": "${DB_CONN_NS}", "asDouble": 3.2 }] }
            },
            {
              "name": "db.client.errors",
              "unit": "{error}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [{ "timeUnixNano": "${DB_CONN_NS}", "asDouble": 1 }]
              }
            },
            {
              "name": "db.client.connection.count",
              "unit": "{connection}",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "db.client.connection.state", "value": { "stringValue": "idle" } }
                    ],
                    "timeUnixNano": "${DB_CONN_NS}",
                    "asDouble": 12
                  }
                ]
              }
            },
            {
              "name": "db.client.connection.pending_requests",
              "unit": "{request}",
              "gauge": { "dataPoints": [{ "timeUnixNano": "${DB_CONN_NS}", "asDouble": 1 }] }
            },
            {
              "name": "db.client.connection.timeouts",
              "unit": "{timeout}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [{ "timeUnixNano": "${DB_CONN_NS}", "asDouble": 1 }]
              }
            },
            {
              "name": "db.client.operation.duration",
              "unit": "ms",
              "histogram": {
                "aggregationTemporality": 2,
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "db.system", "value": { "stringValue": "postgresql" } },
                      { "key": "db.operation.name", "value": { "stringValue": "SELECT" } }
                    ],
                    "startTimeUnixNano": "${DB_HIST_START_NS}",
                    "timeUnixNano": "${DB_HIST_END_NS}",
                    "count": 20,
                    "sum": 36.0,
                    "bucketCounts": [6, 9, 5],
                    "explicitBounds": [1, 2]
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
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/database/latency-summary"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/database/avg-latency"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/database/systems"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/database/connection-count"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/database/connection-pending"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/database/connection-timeouts"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/database/query-duration"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/database/top-tables"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/database/query-by-table"
```
