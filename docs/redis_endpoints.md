# Redis Endpoints

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

REDIS_NS=$((NOW_NS - 13 * 60 * 1000000000))
```

## 2. OTLP gRPC Ingestion Example For Redis Metrics

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
          { "key": "service.name", "value": { "stringValue": "redis-cache" } },
          { "key": "deployment.environment", "value": { "stringValue": "production" } }
        ]
      },
      "scopeMetrics": [
        {
          "scope": { "name": "redis-otel", "version": "1.0.0" },
          "metrics": [
            {
              "name": "redis.keyspace.hits",
              "unit": "{hit}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [{ "timeUnixNano": "${REDIS_NS}", "asDouble": 520 }]
              }
            },
            {
              "name": "redis.keyspace.misses",
              "unit": "{miss}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [{ "timeUnixNano": "${REDIS_NS}", "asDouble": 18 }]
              }
            },
            {
              "name": "redis.clients.connected",
              "unit": "{client}",
              "gauge": { "dataPoints": [{ "timeUnixNano": "${REDIS_NS}", "asDouble": 42 }] }
            },
            {
              "name": "redis.memory.used",
              "unit": "By",
              "gauge": { "dataPoints": [{ "timeUnixNano": "${REDIS_NS}", "asDouble": 125829120 }] }
            },
            {
              "name": "redis.memory.fragmentation_ratio",
              "unit": "1",
              "gauge": { "dataPoints": [{ "timeUnixNano": "${REDIS_NS}", "asDouble": 1.18 }] }
            },
            {
              "name": "redis.commands.processed",
              "unit": "{command}",
              "gauge": { "dataPoints": [{ "timeUnixNano": "${REDIS_NS}", "asDouble": 8200 }] }
            },
            {
              "name": "redis.keys.evicted",
              "unit": "{key}",
              "gauge": { "dataPoints": [{ "timeUnixNano": "${REDIS_NS}", "asDouble": 2 }] }
            },
            {
              "name": "redis.db.keys",
              "unit": "{key}",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "redis.db", "value": { "stringValue": "0" } }
                    ],
                    "timeUnixNano": "${REDIS_NS}",
                    "asDouble": 34000
                  }
                ]
              }
            },
            {
              "name": "redis.db.expires",
              "unit": "{key}",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "redis.db", "value": { "stringValue": "0" } }
                    ],
                    "timeUnixNano": "${REDIS_NS}",
                    "asDouble": 1250
                  }
                ]
              }
            },
            {
              "name": "redis.replication.offset",
              "unit": "{offset}",
              "gauge": { "dataPoints": [{ "timeUnixNano": "${REDIS_NS}", "asDouble": 9723401 }] }
            },
            {
              "name": "redis.replication.backlog_first_byte_offset",
              "unit": "{offset}",
              "gauge": { "dataPoints": [{ "timeUnixNano": "${REDIS_NS}", "asDouble": 9721000 }] }
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
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/redis/cache-hit-rate"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/redis/replication-lag"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/redis/clients"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/redis/memory"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/redis/memory-fragmentation"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/redis/commands"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/redis/evictions"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/redis/keyspace"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/redis/key-expiries"
```
