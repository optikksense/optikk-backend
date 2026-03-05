# API DOCUMENTATION

### 1. User

#### 1.1 Create Team

```bash
curl -X POST http://localhost:9090/api/v1/teams \
  -H "Content-Type: application/json" \
  -d '{
    "team_name": "My Team",
    "slug": "my-team",
    "description": "My Team",
    "org_name": "My-Organization"
  }'

Note: Only `team_name` and `org_name` are required. `slug`, `description`, and `color` are optional.
```

#### 1.2 Create User

```bash
curl -X POST http://localhost:9090/api/v1/users \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "name": "John Doe",
    "role": "admin",
    "password": "securePassword123",
    "teamIds": [1]
  }'
```


#### 1.3 Login

```bash
curl -X POST http://localhost:9090/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "password": "securePassword123"
  }'
```

---

### 2. OTLP Ingest

> Replace `<YOUR_API_KEY>` with a valid API key for your team.

#### 2.1 Metrics — gRPC (port 4317)

Use [`grpcurl`](https://github.com/fullstorydev/grpcurl) — it transparently transcodes the JSON body → binary protobuf before sending, so the handler receives a fully-populated request.

> **Why not plain curl?** The gRPC endpoint speaks binary protobuf, not JSON. Sending raw JSON to port 4317 results in an empty `ResourceMetrics` slice inside the handler.

```bash
grpcurl -plaintext \
  -H "x-api-key: 3676cb53c5f82057886c8d72bbdd24c1aebe386d5639b0db24c83858b713e2ae" \
  -d '{
    "resourceMetrics": [
      {
        "resource": {
          "attributes": [
            { "key": "service.name",           "value": { "stringValue": "payment-service" } },
            { "key": "deployment.environment", "value": { "stringValue": "production" } },
            { "key": "host.name",              "value": { "stringValue": "prod-host-01" } },
            { "key": "k8s.pod.name",           "value": { "stringValue": "payment-7d9f-xkr2p" } },
            { "key": "k8s.namespace.name",     "value": { "stringValue": "payments" } }
          ]
        },
        "scopeMetrics": [
          {
            "scope": { "name": "payment.instrumentation", "version": "1.0.0" },
            "metrics": [
              {
                "name": "process.cpu.usage",
                "unit": "1",
                "gauge": {
                  "dataPoints": [
                    { "timeUnixNano": "1741132860000000000", "asDouble": 0.72 }
                  ]
                }
              },
              {
                "name": "http.server.request.count",
                "unit": "{request}",
                "sum": {
                  "aggregationTemporality": 2,
                  "isMonotonic": true,
                  "dataPoints": [
                    {
                      "attributes": [
                        { "key": "http.status_code", "value": { "intValue": "200" } }
                      ],
                      "startTimeUnixNano": "1741132800000000000",
                      "timeUnixNano":      "1741132860000000000",
                      "asDouble": 15234
                    }
                  ]
                }
              },
              {
                "name": "http.server.duration",
                "unit": "ms",
                "histogram": {
                  "aggregationTemporality": 2,
                  "dataPoints": [
                    {
                      "attributes": [
                        { "key": "http.route", "value": { "stringValue": "/v1/payments" } }
                      ],
                      "startTimeUnixNano": "1741132800000000000",
                      "timeUnixNano":      "1741132860000000000",
                      "count": 1000,
                      "sum":   95000.0,
                      "min":   1.2,
                      "max":   4300.5,
                      "bucketCounts":  [100, 200, 350, 200, 100, 40, 8, 2],
                      "explicitBounds": [5, 10, 25, 50, 100, 250, 500]
                    }
                  ]
                }
              }
            ]
          }
        ]
      }
    ]
  }' \
  localhost:4317 \
  opentelemetry.proto.collector.metrics.v1.MetricsService/Export
```

---

#### 2.2 Metrics — HTTP/JSON (port 4318)

Plain `curl` works here — the HTTP OTLP endpoint accepts `application/json` directly.

```bash
curl -X POST http://localhost:4318/v1/metrics \
  -H "Content-Type: application/json" \
  -H "x-api-key: <YOUR_API_KEY>" \
  -d '{
    "resourceMetrics": [
      {
        "resource": {
          "attributes": [
            { "key": "service.name",           "value": { "stringValue": "payment-service" } },
            { "key": "deployment.environment", "value": { "stringValue": "production" } },
            { "key": "host.name",              "value": { "stringValue": "prod-host-01" } },
            { "key": "k8s.pod.name",           "value": { "stringValue": "payment-7d9f-xkr2p" } },
            { "key": "k8s.namespace.name",     "value": { "stringValue": "payments" } }
          ]
        },
        "scopeMetrics": [
          {
            "scope": { "name": "payment.instrumentation", "version": "1.0.0" },
            "metrics": [
              {
                "name": "process.cpu.usage",
                "unit": "1",
                "gauge": {
                  "dataPoints": [
                    { "timeUnixNano": "1741132860000000000", "asDouble": 0.72 }
                  ]
                }
              },
              {
                "name": "http.server.request.count",
                "unit": "{request}",
                "sum": {
                  "aggregationTemporality": 2,
                  "isMonotonic": true,
                  "dataPoints": [
                    {
                      "attributes": [
                        { "key": "http.status_code", "value": { "intValue": "200" } }
                      ],
                      "startTimeUnixNano": "1741132800000000000",
                      "timeUnixNano":      "1741132860000000000",
                      "asDouble": 15234
                    }
                  ]
                }
              },
              {
                "name": "http.server.duration",
                "unit": "ms",
                "histogram": {
                  "aggregationTemporality": 2,
                  "dataPoints": [
                    {
                      "attributes": [
                        { "key": "http.route", "value": { "stringValue": "/v1/payments" } }
                      ],
                      "startTimeUnixNano": "1741132800000000000",
                      "timeUnixNano":      "1741132860000000000",
                      "count": 1000,
                      "sum":   95000.0,
                      "min":   1.2,
                      "max":   4300.5,
                      "bucketCounts":  [100, 200, 350, 200, 100, 40, 8, 2],
                      "explicitBounds": [5, 10, 25, 50, 100, 250, 500]
                    }
                  ]
                }
              }
            ]
          }
        ]
      }
    ]
  }'
```
