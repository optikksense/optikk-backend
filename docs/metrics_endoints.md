# Metrics Endpoints

This document focuses on JVM runtime metrics.

- OTLP ingestion uses gRPC on port `4317` with `x-api-key`.
- Dashboard APIs use `Authorization: Bearer <JWT_TOKEN>`.
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

END_MS=$(($(date +%s) * 1000))
START_MS=$((END_MS - 3600000))
NOW_NS=$((END_MS * 1000000))

GC_START_NS=$((NOW_NS - 22 * 60 * 1000000000))
GC_END_NS=$((GC_START_NS + 12000000))
CPU_SAMPLE_NS=$((NOW_NS - 18 * 60 * 1000000000))
MEM_SAMPLE_NS=$((NOW_NS - 16 * 60 * 1000000000))
THREAD_SAMPLE_NS=$((NOW_NS - 14 * 60 * 1000000000))
CLASS_SAMPLE_NS=$((NOW_NS - 12 * 60 * 1000000000))
BUFFER_SAMPLE_NS=$((NOW_NS - 10 * 60 * 1000000000))
```

## 2. OTLP gRPC Ingestion Example For JVM Metrics

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
          { "key": "service.name", "value": { "stringValue": "orders-service" } },
          { "key": "deployment.environment", "value": { "stringValue": "production" } },
          { "key": "host.name", "value": { "stringValue": "orders-node-01" } },
          { "key": "k8s.namespace.name", "value": { "stringValue": "payments" } },
          { "key": "k8s.pod.name", "value": { "stringValue": "orders-service-7f6d8b9d6d-2xw4m" } },
          { "key": "telemetry.sdk.language", "value": { "stringValue": "java" } },
          { "key": "telemetry.sdk.version", "value": { "stringValue": "1.35.0" } }
        ]
      },
      "scopeMetrics": [
        {
          "scope": {
            "name": "io.micrometer.opentelemetry",
            "version": "1.13.6"
          },
          "metrics": [
            {
              "name": "jvm.gc.duration",
              "unit": "ms",
              "histogram": {
                "aggregationTemporality": 2,
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "jvm.gc.name", "value": { "stringValue": "G1 Young Generation" } },
                      { "key": "jvm.gc.action", "value": { "stringValue": "end of minor GC" } }
                    ],
                    "startTimeUnixNano": "${GC_START_NS}",
                    "timeUnixNano": "${GC_END_NS}",
                    "count": 4,
                    "sum": 18.4,
                    "bucketCounts": [1, 2, 1],
                    "explicitBounds": [2, 5]
                  }
                ]
              }
            },
            {
              "name": "jvm.cpu.time",
              "unit": "ns",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [
                  {
                    "timeUnixNano": "${CPU_SAMPLE_NS}",
                    "asDouble": 2418000000
                  }
                ]
              }
            },
            {
              "name": "jvm.cpu.recent_utilization",
              "unit": "1",
              "gauge": {
                "dataPoints": [
                  {
                    "timeUnixNano": "${CPU_SAMPLE_NS}",
                    "asDouble": 0.37
                  }
                ]
              }
            },
            {
              "name": "jvm.memory.used",
              "unit": "By",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "jvm.memory.pool.name", "value": { "stringValue": "G1 Eden Space" } },
                      { "key": "jvm.memory.type", "value": { "stringValue": "heap" } }
                    ],
                    "timeUnixNano": "${MEM_SAMPLE_NS}",
                    "asDouble": 268435456
                  }
                ]
              }
            },
            {
              "name": "jvm.memory.committed",
              "unit": "By",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "jvm.memory.pool.name", "value": { "stringValue": "G1 Eden Space" } },
                      { "key": "jvm.memory.type", "value": { "stringValue": "heap" } }
                    ],
                    "timeUnixNano": "${MEM_SAMPLE_NS}",
                    "asDouble": 402653184
                  }
                ]
              }
            },
            {
              "name": "jvm.memory.limit",
              "unit": "By",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "jvm.memory.pool.name", "value": { "stringValue": "G1 Eden Space" } },
                      { "key": "jvm.memory.type", "value": { "stringValue": "heap" } }
                    ],
                    "timeUnixNano": "${MEM_SAMPLE_NS}",
                    "asDouble": 536870912
                  }
                ]
              }
            },
            {
              "name": "jvm.thread.count",
              "unit": "{thread}",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "jvm.thread.daemon", "value": { "stringValue": "false" } }
                    ],
                    "timeUnixNano": "${THREAD_SAMPLE_NS}",
                    "asDouble": 48
                  }
                ]
              }
            },
            {
              "name": "jvm.class.loaded",
              "unit": "{class}",
              "gauge": {
                "dataPoints": [
                  {
                    "timeUnixNano": "${CLASS_SAMPLE_NS}",
                    "asDouble": 12420
                  }
                ]
              }
            },
            {
              "name": "jvm.class.count",
              "unit": "{class}",
              "gauge": {
                "dataPoints": [
                  {
                    "timeUnixNano": "${CLASS_SAMPLE_NS}",
                    "asDouble": 12600
                  }
                ]
              }
            },
            {
              "name": "jvm.buffer.memory.usage",
              "unit": "By",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "jvm.buffer.pool.name", "value": { "stringValue": "direct" } }
                    ],
                    "timeUnixNano": "${BUFFER_SAMPLE_NS}",
                    "asDouble": 8388608
                  }
                ]
              }
            },
            {
              "name": "jvm.buffer.count",
              "unit": "{buffer}",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "jvm.buffer.pool.name", "value": { "stringValue": "direct" } }
                    ],
                    "timeUnixNano": "${BUFFER_SAMPLE_NS}",
                    "asDouble": 128
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

## 3. Dashboard API Curls For JVM Metrics

These are the endpoints used by the JVM runtime dashboard tab.

### 3.1 GC Duration

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/infrastructure/jvm/gc-duration"
```

### 3.2 JVM CPU

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/infrastructure/jvm/cpu"
```

### 3.3 JVM Memory

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/infrastructure/jvm/memory"
```

### 3.4 JVM GC Collections

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/infrastructure/jvm/gc-collections"
```

### 3.5 JVM Threads

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/infrastructure/jvm/threads"
```

### 3.6 JVM Buffers

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/infrastructure/jvm/buffers"
```

### 3.7 JVM Classes

```bash
curl -sS -G \
  -H "${AUTH_HEADER}" \
  -H "${TEAM_HEADER}" \
  --data-urlencode "start=${START_MS}" \
  --data-urlencode "end=${END_MS}" \
  "${API_BASE}/api/v1/infrastructure/jvm/classes"
```
