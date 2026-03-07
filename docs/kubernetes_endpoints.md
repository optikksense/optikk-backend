# Kubernetes Endpoints

- Covers `internal/defaultconfig/pages/infrastructure/tabs/kubernetes.json`.
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

K8S_NS=$((NOW_NS - 9 * 60 * 1000000000))
```

## 2. OTLP gRPC Ingestion Example For Kubernetes Metrics

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
          { "key": "k8s.namespace.name", "value": { "stringValue": "payments" } },
          { "key": "k8s.node.name", "value": { "stringValue": "worker-01" } }
        ]
      },
      "scopeMetrics": [
        {
          "scope": { "name": "otelcol/kubeletstatsreceiver", "version": "0.103.0" },
          "metrics": [
            {
              "name": "container.cpu.time",
              "unit": "s",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [
                  { "attributes": [{ "key": "container.name", "value": { "stringValue": "checkout-service" } }], "timeUnixNano": "${K8S_NS}", "asDouble": 92.3 }
                ]
              }
            },
            {
              "name": "container.cpu.throttling_data.throttled_time",
              "unit": "s",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [
                  { "attributes": [{ "key": "container.name", "value": { "stringValue": "checkout-service" } }], "timeUnixNano": "${K8S_NS}", "asDouble": 3.4 }
                ]
              }
            },
            {
              "name": "container.memory.usage",
              "unit": "By",
              "gauge": {
                "dataPoints": [
                  { "attributes": [{ "key": "container.name", "value": { "stringValue": "checkout-service" } }], "timeUnixNano": "${K8S_NS}", "asDouble": 402653184 }
                ]
              }
            },
            {
              "name": "container.memory.oom_kill_count",
              "unit": "{event}",
              "sum": {
                "aggregationTemporality": 2,
                "isMonotonic": true,
                "dataPoints": [
                  { "attributes": [{ "key": "container.name", "value": { "stringValue": "checkout-service" } }], "timeUnixNano": "${K8S_NS}", "asDouble": 0 }
                ]
              }
            },
            {
              "name": "k8s.container.restarts",
              "unit": "{restart}",
              "gauge": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "k8s.pod.name", "value": { "stringValue": "checkout-service-74486c9d7d-qzq8l" } },
                      { "key": "k8s.namespace.name", "value": { "stringValue": "payments" } }
                    ],
                    "timeUnixNano": "${K8S_NS}",
                    "asDouble": 1
                  }
                ]
              }
            },
            { "name": "k8s.node.allocatable.cpu", "unit": "{cpu}", "gauge": { "dataPoints": [{ "timeUnixNano": "${K8S_NS}", "asDouble": 8 }] } },
            { "name": "k8s.node.allocatable.memory", "unit": "By", "gauge": { "dataPoints": [{ "timeUnixNano": "${K8S_NS}", "asDouble": 34359738368 }] } },
            {
              "name": "k8s.pod.phase",
              "unit": "{pod}",
              "gauge": {
                "dataPoints": [
                  { "attributes": [{ "key": "k8s.pod.phase", "value": { "stringValue": "Running" } }], "timeUnixNano": "${K8S_NS}", "asDouble": 12 },
                  { "attributes": [{ "key": "k8s.pod.phase", "value": { "stringValue": "Pending" } }], "timeUnixNano": "${K8S_NS}", "asDouble": 1 }
                ]
              }
            },
            {
              "name": "k8s.replicaset.desired",
              "unit": "{replica}",
              "gauge": {
                "dataPoints": [
                  { "attributes": [{ "key": "k8s.replicaset.name", "value": { "stringValue": "checkout-service-74486c9d7d" } }], "timeUnixNano": "${K8S_NS}", "asDouble": 4 }
                ]
              }
            },
            {
              "name": "k8s.replicaset.available",
              "unit": "{replica}",
              "gauge": {
                "dataPoints": [
                  { "attributes": [{ "key": "k8s.replicaset.name", "value": { "stringValue": "checkout-service-74486c9d7d" } }], "timeUnixNano": "${K8S_NS}", "asDouble": 3 }
                ]
              }
            },
            {
              "name": "k8s.volume.capacity",
              "unit": "By",
              "gauge": {
                "dataPoints": [
                  { "attributes": [{ "key": "k8s.volume.name", "value": { "stringValue": "data-orders-pvc" } }], "timeUnixNano": "${K8S_NS}", "asDouble": 107374182400 }
                ]
              }
            },
            {
              "name": "k8s.volume.inodes",
              "unit": "{inode}",
              "gauge": {
                "dataPoints": [
                  { "attributes": [{ "key": "k8s.volume.name", "value": { "stringValue": "data-orders-pvc" } }], "timeUnixNano": "${K8S_NS}", "asDouble": 840000 }
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
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/kubernetes/node-allocatable"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/kubernetes/container-cpu"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/kubernetes/cpu-throttling"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/kubernetes/container-memory"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/kubernetes/oom-kills"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/kubernetes/pod-phases"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/kubernetes/replica-status"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/kubernetes/pod-restarts"
curl -sS -G -H "${AUTH_HEADER}" -H "${TEAM_HEADER}" --data-urlencode "start=${START_MS}" --data-urlencode "end=${END_MS}" "${API_BASE}/api/v1/infrastructure/kubernetes/volume-usage"
```
