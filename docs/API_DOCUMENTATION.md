# API DOCUMENTATION

### 1. User

#### 1.1 Create Team

Expected response: HTTP `201 Created` with the new team payload.

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

Expected response: HTTP `201 Created` with the new user payload.

```bash
curl -X POST http://localhost:9090/api/v1/users \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user6@example.com",
    "name": "John Doe",
    "role": "admin",
    "password": "securePassword123",
    "teamIds": [1]
  }'
```


#### 1.3 Login

Expected response: HTTP `200 OK` with a JWT token and the current team context.

```bash
curl -X POST http://localhost:9090/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user6@example.com",
    "password": "securePassword123"
  }'
```

---

### 2. OTLP Ingest

> Replace `<YOUR_API_KEY>` with a valid API key for your team.

#### 2.1 Metrics — gRPC (port 4317)

Expected response: gRPC status `OK` with an empty JSON body like `{}`.

Use [`grpcurl`](https://github.com/fullstorydev/grpcurl) — it transparently transcodes the JSON body → binary protobuf before sending, so the handler receives a fully-populated request.

> **Why not plain curl?** The gRPC endpoint speaks binary protobuf, not JSON. Sending raw JSON to port 4317 results in an empty `ResourceMetrics` slice inside the handler.
>
> **Byte fields in `grpcurl`:** `traceId`, `spanId`, and `parentSpanId` use protobuf JSON encoding here, so they must be base64-encoded.
> The span and log examples below correspond to hex `traceId` `665f7e3a9c4b1d82f3a0c6e1b7d9425f` and hex `spanId` `a3f19c7b5d8e2f41`, which match the lower-case 32/16-char format commonly seen in Spring Boot tracing.

```bash
grpcurl -plaintext \
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
                    { "timeUnixNano": "1772867983920860928", "asDouble": 0.72 }
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
                      "startTimeUnixNano": "1772866243920860928",
                      "timeUnixNano":      "1772868943920860928",
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
                      "startTimeUnixNano": "1772866243920860928",
                      "timeUnixNano":      "1772868943920860928",
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

#### 2.2 Spans — gRPC (port 4317)

Expected response: gRPC status `OK` with an empty JSON body like `{}`.

```bash
grpcurl -plaintext \
  -H "x-api-key: <YOUR_API_KEY>" \
  -d '{
    "resourceSpans": [
      {
        "resource": {
          "attributes": [
            { "key": "service.name",           "value": { "stringValue": "checkout-service" } },
            { "key": "deployment.environment", "value": { "stringValue": "production" } },
            { "key": "host.name",              "value": { "stringValue": "checkout-host-01" } },
            { "key": "k8s.pod.name",           "value": { "stringValue": "checkout-6b7f9d4f5c-abc12" } },
            { "key": "k8s.namespace.name",     "value": { "stringValue": "checkout" } }
          ]
        },
        "scopeSpans": [
          {
            "scope": { "name": "checkout.instrumentation", "version": "1.0.0" },
            "spans": [
              {
                "traceId": "Zl9+OpxLHYLzoMbht9lCXw==",
                "spanId": "o/Gce12OL0E=",
                "name": "POST /api/v1/orders",
                "kind": 2,
                "startTimeUnixNano": "1772868403920860928",
                "endTimeUnixNano": "1772868404370860928",
                "attributes": [
                  { "key": "http.method",      "value": { "stringValue": "POST" } },
                  { "key": "http.route",       "value": { "stringValue": "/api/v1/orders" } },
                  { "key": "http.status_code", "value": { "intValue": "201" } }
                ],
                "status": {
                  "code": 1
                }
              }
            ]
          }
        ]
      }
    ]
  }' \
  localhost:4317 \
  opentelemetry.proto.collector.trace.v1.TraceService/Export
```

---

#### 2.3 Logs — gRPC (port 4317)

Expected response: gRPC status `OK` with an empty JSON body like `{}`.

```bash
grpcurl -plaintext \
  -H "x-api-key: <YOUR_API_KEY>" \
  -d '{
    "resourceLogs": [
      {
        "resource": {
          "attributes": [
            { "key": "service.name",           "value": { "stringValue": "checkout-service" } },
            { "key": "deployment.environment", "value": { "stringValue": "production" } },
            { "key": "host.name",              "value": { "stringValue": "checkout-host-01" } },
            { "key": "k8s.pod.name",           "value": { "stringValue": "checkout-6b7f9d4f5c-abc12" } },
            { "key": "k8s.namespace.name",     "value": { "stringValue": "checkout" } }
          ]
        },
        "scopeLogs": [
          {
            "scope": { "name": "checkout.logger", "version": "1.0.0" },
            "logRecords": [
              {
                "timeUnixNano": "1772868463920860928",
                "observedTimeUnixNano": "1772868463921860928",
                "severityNumber": 9,
                "severityText": "INFO",
                "body": { "stringValue": "order created successfully" },
                "attributes": [
                  { "key": "http.method",      "value": { "stringValue": "POST" } },
                  { "key": "http.route",       "value": { "stringValue": "/api/v1/orders" } },
                  { "key": "http.status_code", "value": { "intValue": "201" } }
                ],
                "traceId": "Zl9+OpxLHYLzoMbht9lCXw==",
                "spanId": "o/Gce12OL0E="
              }
            ]
          }
        ]
      }
    ]
  }' \
  localhost:4317 \
  opentelemetry.proto.collector.logs.v1.LogsService/Export
```
