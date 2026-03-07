# Kafka Endpoints

This covers the queue / Kafka saturation dashboard.

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

QUEUE_NS=$((NOW_NS - 14 * 60 * 1000000000))
QUEUE_HIST_START_NS=$((NOW_NS - 20 * 60 * 1000000000))
QUEUE_HIST_END_NS=$((QUEUE_HIST_START_NS + 22000000))
```

## 2. OTLP gRPC Ingestion Example For Queue Metrics

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
          { "key": "service.name", "value": { "stringValue": "checkout-worker" } },
          { "key": "deployment.environment", "value": { "stringValue": "production" } }
        ]
      },
      "scopeMetrics": [
        {
          "scope": { "name": "io.opentelemetry.kafka-clients-2.6", "version": "2.11.0" },
          "metrics": [
            {
              "name": "kafka.consumer.lag",
              "unit": "{message}",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "messaging.destination.name", "value": { "stringValue": "orders-events" } },
                      { "key": "messaging.kafka.consumer.group", "value": { "stringValue": "checkout-consumer" } },
                      { "key": "messaging.kafka.destination.partition", "value": { "stringValue": "0" } },
                      { "key": "messaging.system", "value": { "stringValue": "kafka" } }
                    ],
                    "timeUnixNano": "${QUEUE_NS}",
                    "asDouble": 37
                  }
                ]
              }
            },
            {
              "name": "queue.depth",
              "unit": "{message}",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "messaging.destination.name", "value": { "stringValue": "orders-events" } },
                      { "key": "messaging.system", "value": { "stringValue": "kafka" } }
                    ],
                    "timeUnixNano": "${QUEUE_NS}",
                    "asDouble": 92
                  }
                ]
              }
            },
            {
              "name": "kafka.producer.message.count",
              "unit": "{message}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "messaging.destination.name", "value": { "stringValue": "orders-events" } }
                    ],
                    "timeUnixNano": "${QUEUE_NS}",
                    "asDouble": 4800
                  }
                ]
              }
            },
            {
              "name": "kafka.consumer.message.count",
              "unit": "{message}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "messaging.destination.name", "value": { "stringValue": "orders-events" } }
                    ],
                    "timeUnixNano": "${QUEUE_NS}",
                    "asDouble": 4710
                  }
                ]
              }
            },
            {
              "name": "messaging.client.published.messages",
              "unit": "{message}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [{ "timeUnixNano": "${QUEUE_NS}", "asDouble": 4800 }]
              }
            },
            {
              "name": "messaging.client.consumed.messages",
              "unit": "{message}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [{ "timeUnixNano": "${QUEUE_NS}", "asDouble": 4710 }]
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
                    "startTimeUnixNano": "${QUEUE_HIST_START_NS}",
                    "timeUnixNano": "${QUEUE_HIST_END_NS}",
                    "count": 120,
                    "sum": 9600.0,
                    "bucketCounts": [36, 54, 30],
                    "explicitBounds": [50, 100]
                  }
                ]
              }
            },
            {
              "name": "messaging.kafka.consumer.offset",
              "unit": "{offset}",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "messaging.destination.name", "value": { "stringValue": "orders-events" } }
                    ],
                    "timeUnixNano": "${QUEUE_NS}",
                    "asDouble": 982341
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
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/queue/operation-duration"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/queue/message-rates"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/queue/consumer-lag"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/queue/topic-lag"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/queue/offset-commit-rate"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/queue/consumer-lag-detail"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/saturation/queue/top-queues"
```
