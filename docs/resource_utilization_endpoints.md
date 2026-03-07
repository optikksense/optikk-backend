# Resource Utilization Endpoints

- Covers `internal/defaultconfig/pages/infrastructure/tabs/resource-utilization.json`.
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

RES_NS=$((NOW_NS - 12 * 60 * 1000000000))
SUM_START_NS=$((NOW_NS - 26 * 60 * 1000000000))
SUM_END_NS=$((SUM_START_NS + 24000000))
```

## 2. OTLP gRPC Ingestion Example For Resource Utilization

Expected response: gRPC status `OK` with an empty JSON body like `{}`.

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
          { "key": "deployment.environment", "value": { "stringValue": "production" } },
          { "key": "host.name", "value": { "stringValue": "checkout-node-01" } },
          { "key": "k8s.pod.name", "value": { "stringValue": "checkout-service-74486c9d7d-qzq8l" } },
          { "key": "container.name", "value": { "stringValue": "checkout-service" } }
        ]
      },
      "scopeMetrics": [
        {
          "scope": { "name": "otelcol/hostmetricsreceiver", "version": "0.103.0" },
          "metrics": [
            { "name": "system.cpu.utilization", "unit": "1", "gauge": { "dataPoints": [{ "timeUnixNano": "${RES_NS}", "asDouble": 0.41 }] } },
            { "name": "system.memory.utilization", "unit": "1", "gauge": { "dataPoints": [{ "timeUnixNano": "${RES_NS}", "asDouble": 0.68 }] } },
            { "name": "system.network.utilization", "unit": "1", "gauge": { "dataPoints": [{ "timeUnixNano": "${RES_NS}", "asDouble": 0.29 }] } },
            { "name": "db.connection.pool.utilization", "unit": "1", "gauge": { "dataPoints": [{ "timeUnixNano": "${RES_NS}", "asDouble": 0.56 }] } },
            { "name": "hikaricp.connections.active", "unit": "{connection}", "gauge": { "dataPoints": [{ "timeUnixNano": "${RES_NS}", "asDouble": 14 }] } },
            { "name": "hikaricp.connections.max", "unit": "{connection}", "gauge": { "dataPoints": [{ "timeUnixNano": "${RES_NS}", "asDouble": 32 }] } },
            {
              "name": "system.cpu.time",
              "unit": "s",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [
                  { "attributes": [{ "key": "system.cpu.state", "value": { "stringValue": "user" } }], "timeUnixNano": "${RES_NS}", "asDouble": 183.4 },
                  { "attributes": [{ "key": "system.cpu.state", "value": { "stringValue": "system" } }], "timeUnixNano": "${RES_NS}", "asDouble": 52.1 }
                ]
              }
            },
            {
              "name": "system.memory.usage",
              "unit": "By",
              "gauge": {
                "dataPoints": [
                  { "attributes": [{ "key": "system.memory.state", "value": { "stringValue": "used" } }], "timeUnixNano": "${RES_NS}", "asDouble": 8589934592 },
                  { "attributes": [{ "key": "system.memory.state", "value": { "stringValue": "free" } }], "timeUnixNano": "${RES_NS}", "asDouble": 4294967296 }
                ]
              }
            },
            {
              "name": "system.paging.usage",
              "unit": "By",
              "gauge": {
                "dataPoints": [
                  { "attributes": [{ "key": "system.memory.state", "value": { "stringValue": "used" } }], "timeUnixNano": "${RES_NS}", "asDouble": 536870912 },
                  { "attributes": [{ "key": "system.memory.state", "value": { "stringValue": "free" } }], "timeUnixNano": "${RES_NS}", "asDouble": 1610612736 }
                ]
              }
            },
            {
              "name": "system.disk.io",
              "unit": "By",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [
                  { "attributes": [{ "key": "system.disk.direction", "value": { "stringValue": "read" } }], "timeUnixNano": "${RES_NS}", "asDouble": 7340032 },
                  { "attributes": [{ "key": "system.disk.direction", "value": { "stringValue": "write" } }], "timeUnixNano": "${RES_NS}", "asDouble": 9437184 }
                ]
              }
            },
            {
              "name": "system.disk.operations",
              "unit": "{operation}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [
                  { "attributes": [{ "key": "system.disk.direction", "value": { "stringValue": "read" } }], "timeUnixNano": "${RES_NS}", "asDouble": 122 },
                  { "attributes": [{ "key": "system.disk.direction", "value": { "stringValue": "write" } }], "timeUnixNano": "${RES_NS}", "asDouble": 184 }
                ]
              }
            },
            { "name": "system.disk.io_time", "unit": "s", "sum": { "aggregationTemporality": 2, "isMonotonic": true, "dataPoints": [{ "timeUnixNano": "${RES_NS}", "asDouble": 12.3 }] } },
            {
              "name": "system.filesystem.usage",
              "unit": "By",
              "gauge": { "dataPoints": [{ "attributes": [{ "key": "system.filesystem.mountpoint", "value": { "stringValue": "/" } }], "timeUnixNano": "${RES_NS}", "asDouble": 25769803776 }] }
            },
            {
              "name": "system.filesystem.utilization",
              "unit": "1",
              "gauge": { "dataPoints": [{ "attributes": [{ "key": "system.filesystem.mountpoint", "value": { "stringValue": "/" } }], "timeUnixNano": "${RES_NS}", "asDouble": 0.61 }] }
            },
            {
              "name": "system.network.io",
              "unit": "By",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [
                  { "attributes": [{ "key": "system.network.io.direction", "value": { "stringValue": "transmit" } }], "timeUnixNano": "${RES_NS}", "asDouble": 3145728 },
                  { "attributes": [{ "key": "system.network.io.direction", "value": { "stringValue": "receive" } }], "timeUnixNano": "${RES_NS}", "asDouble": 4194304 }
                ]
              }
            },
            {
              "name": "system.network.packets",
              "unit": "{packet}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [
                  { "attributes": [{ "key": "system.network.io.direction", "value": { "stringValue": "transmit" } }], "timeUnixNano": "${RES_NS}", "asDouble": 2800 },
                  { "attributes": [{ "key": "system.network.io.direction", "value": { "stringValue": "receive" } }], "timeUnixNano": "${RES_NS}", "asDouble": 3310 }
                ]
              }
            },
            {
              "name": "system.network.errors",
              "unit": "{packet}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [
                  { "attributes": [{ "key": "system.network.io.direction", "value": { "stringValue": "transmit" } }], "timeUnixNano": "${RES_NS}", "asDouble": 1 },
                  { "attributes": [{ "key": "system.network.io.direction", "value": { "stringValue": "receive" } }], "timeUnixNano": "${RES_NS}", "asDouble": 0 }
                ]
              }
            },
            { "name": "system.network.dropped", "unit": "{packet}", "sum": { "aggregationTemporality": 2, "isMonotonic": true, "dataPoints": [{ "timeUnixNano": "${RES_NS}", "asDouble": 2 }] } },
            { "name": "system.cpu.load_average.1m", "unit": "1", "gauge": { "dataPoints": [{ "timeUnixNano": "${RES_NS}", "asDouble": 1.26 }] } },
            { "name": "system.cpu.load_average.5m", "unit": "1", "gauge": { "dataPoints": [{ "timeUnixNano": "${RES_NS}", "asDouble": 1.11 }] } },
            { "name": "system.cpu.load_average.15m", "unit": "1", "gauge": { "dataPoints": [{ "timeUnixNano": "${RES_NS}", "asDouble": 0.94 }] } },
            {
              "name": "system.process.count",
              "unit": "{process}",
              "gauge": {
                "dataPoints": [
                  { "attributes": [{ "key": "process.status", "value": { "stringValue": "running" } }], "timeUnixNano": "${RES_NS}", "asDouble": 146 },
                  { "attributes": [{ "key": "process.status", "value": { "stringValue": "sleeping" } }], "timeUnixNano": "${RES_NS}", "asDouble": 312 }
                ]
              }
            },
            {
              "name": "system.network.connections",
              "unit": "{connection}",
              "gauge": {
                "dataPoints": [
                  { "attributes": [{ "key": "system.network.state", "value": { "stringValue": "established" } }], "timeUnixNano": "${RES_NS}", "asDouble": 84 },
                  { "attributes": [{ "key": "system.network.state", "value": { "stringValue": "listen" } }], "timeUnixNano": "${RES_NS}", "asDouble": 12 }
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
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/resource-utilisation/avg-cpu"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/resource-utilisation/avg-memory"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/resource-utilisation/avg-network"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/resource-utilisation/avg-conn-pool"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/resource-utilisation/cpu-usage-percentage"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/resource-utilisation/memory-usage-percentage"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/resource-utilisation/by-service"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/resource-utilisation/by-instance"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/cpu-time"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/memory-usage"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/swap-usage"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/load-average"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/disk-io"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/disk-operations"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/disk-io-time"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/filesystem-usage"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/filesystem-utilization"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/network-io"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/network-packets"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/network-errors"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/network-dropped"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/process-count"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/network-connections"
```
