# APM Endpoints

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

RPC_START_NS=$((NOW_NS - 24 * 60 * 1000000000))
RPC_END_NS=$((RPC_START_NS + 15000000))
MSG_PUB_NS=$((NOW_NS - 18 * 60 * 1000000000))
PROCESS_NS=$((NOW_NS - 12 * 60 * 1000000000))
```

## 2. OTLP gRPC Ingestion Example For APM Metrics

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
          "scope": { "name": "io.opentelemetry.javaagent", "version": "2.11.0" },
          "metrics": [
            {
              "name": "rpc.server.duration",
              "unit": "ms",
              "histogram": {
                "aggregationTemporality": 2,
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "rpc.system", "value": { "stringValue": "grpc" } },
                      { "key": "rpc.service", "value": { "stringValue": "checkout.OrderService" } },
                      { "key": "rpc.method", "value": { "stringValue": "CreateOrder" } },
                      { "key": "rpc.grpc.status_code", "value": { "stringValue": "0" } }
                    ],
                    "startTimeUnixNano": "${RPC_START_NS}",
                    "timeUnixNano": "${RPC_END_NS}",
                    "count": 24,
                    "sum": 360.0,
                    "bucketCounts": [6, 11, 7],
                    "explicitBounds": [10, 25]
                  }
                ]
              }
            },
            {
              "name": "messaging.client.operation.duration",
              "unit": "ms",
              "histogram": {
                "aggregationTemporality": 2,
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "messaging.operation.name", "value": { "stringValue": "publish" } }
                    ],
                    "startTimeUnixNano": "${MSG_PUB_NS}",
                    "timeUnixNano": "${MSG_PUB_NS}",
                    "count": 12,
                    "sum": 144.0,
                    "bucketCounts": [3, 5, 4],
                    "explicitBounds": [5, 15]
                  }
                ]
              }
            },
            {
              "name": "process.cpu.time",
              "unit": "s",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "process.cpu.state", "value": { "stringValue": "user" } }
                    ],
                    "timeUnixNano": "${PROCESS_NS}",
                    "asDouble": 1.83
                  },
                  {
                    "attributes": [
                      { "key": "process.cpu.state", "value": { "stringValue": "system" } }
                    ],
                    "timeUnixNano": "${PROCESS_NS}",
                    "asDouble": 0.44
                  }
                ]
              }
            },
            {
              "name": "process.memory.usage",
              "unit": "By",
              "gauge": { "dataPoints": [{ "timeUnixNano": "${PROCESS_NS}", "asDouble": 402653184 }] }
            },
            {
              "name": "process.memory.virtual",
              "unit": "By",
              "gauge": { "dataPoints": [{ "timeUnixNano": "${PROCESS_NS}", "asDouble": 17179869184 }] }
            },
            {
              "name": "process.open_file_descriptor.count",
              "unit": "{fd}",
              "gauge": { "dataPoints": [{ "timeUnixNano": "${PROCESS_NS}", "asDouble": 192 }] }
            },
            {
              "name": "process.uptime",
              "unit": "s",
              "gauge": { "dataPoints": [{ "timeUnixNano": "${PROCESS_NS}", "asDouble": 14400 }] }
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
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/apm/rpc-duration"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/apm/rpc-request-rate"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/apm/messaging-publish-duration"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/apm/process-cpu"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/apm/process-memory"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/apm/open-fds"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/apm/uptime"
```
