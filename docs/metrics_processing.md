# Metrics Processing Pipeline

## Overview

The observability backend processes metrics following the OpenTelemetry (OTLP) standard. This document explains the complete journey of a metric from ingestion through storage and querying, with integration details for logs and spans.

---

## Real-World Example

Let's trace a metric named `http.server.request.duration` with attributes:
- **Metric Name**: `http.server.request.duration`
- **Value**: `45.5` (milliseconds)
- **Type**: `histogram`
- **Service**: `payment-service`
- **Attributes**:
  - `http.method=POST`
  - `http.status_code=200`
  - `server.address=api.example.com`
  - `k8s.pod.name=payment-pod-123`
  - `k8s.container.name=payment-container`

---

## Processing Pipeline

### Phase 1: HTTP Ingestion

**Endpoint**: `POST /otlp/v1/metrics`

The metric arrives as part of an OTLP (OpenTelemetry Protocol) payload, either in JSON or Protobuf format:

```json
{
  "resourceMetrics": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "payment-service" } },
          { "key": "host.name", "value": { "stringValue": "node-456" } }
        ]
      },
      "scopeMetrics": [
        {
          "scope": {
            "name": "opentelemetry.instrumentation.http",
            "version": "1.0.0"
          },
          "metrics": [
            {
              "name": "http.server.request.duration",
              "description": "Measures the duration of inbound HTTP requests",
              "unit": "ms",
              "histogram": {
                "dataPoints": [
                  {
                    "attributes": [
                      { "key": "http.request.method", "value": { "stringValue": "POST" } },
                      { "key": "http.response.status_code", "value": { "intValue": "200" } },
                      { "key": "server.address", "value": { "stringValue": "api.example.com" } },
                      { "key": "k8s.pod.name", "value": { "stringValue": "payment-pod-123" } },
                      { "key": "k8s.container.name", "value": { "stringValue": "payment-container" } }
                    ],
                    "timeUnixNano": "1708864200000000000",
                    "count": "150",
                    "sum": 6825.0,
                    "min": 10.5,
                    "max": 95.2,
                    "bucketCounts": ["30", "45", "50", "20", "5"],
                    "explicitBounds": [10, 25, 50, 100, 250]
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
```

### Phase 2: API Key Resolution & Authentication

**Handler**: `Handler.HandleMetrics()` in [metrics_handler.go:14-36](internal/telemetry/metrics_handler.go#L14-L36)

```
1. Resolve API key from request headers
2. Map API key to team_uuid (e.g., "team-abc-123")
3. If invalid/missing, return 401 Unauthorized
```

**Result**: `teamUUID = "team-abc-123"`

### Phase 3: Payload Parsing

**Handler**: `metrics_handler.go:27-36`

- Detect content type (JSON or Protobuf)
- Unmarshal into `OTLPMetricsPayload` struct
- Validate OTLP format

**Struct**: [types.go:40-84](internal/telemetry/types.go#L40-L84)

### Phase 4: Metric Extraction & Normalization

**Handler**: `metrics_handler.go:38-181`

For each metric in the payload:

#### 4a. Extract Resource Attributes
```go
resourceAttrs := otlpAttrMap(rm.Resource.Attributes)
// Result:
// {
//   "service.name": "payment-service",
//   "host.name": "node-456"
// }
```

#### 4b. Determine Metric Category
```go
category := metricCategory(metric.Name)
// "http.server.request.duration" → category = "http"
```

#### 4c. Process by Metric Type

Our example is a **histogram**, so:

```go
case metric.Histogram != nil:
    for _, dp := range metric.Histogram.DataPoints {
        // Extract attributes from datapoint
        dpAttrs := otlpAttrMap(dp.Attributes)
        // {
        //   "http.request.method": "POST",
        //   "http.response.status_code": "200",
        //   "server.address": "api.example.com",
        //   "k8s.pod.name": "payment-pod-123",
        //   "k8s.container.name": "payment-container"
        // }
```

#### 4d. Extract Known Labels
```go
labels := extractDPLabels(dpAttrs, resourceAttrs)
// Results in:
// {
//   httpMethod: "POST",
//   httpStatusCode: 200,
//   host: "api.example.com",
//   pod: "payment-pod-123",
//   container: "payment-container"
// }
```

#### 4e. Calculate Statistical Values for Histogram
```go
count = 150                    // Total requests
sum = 6825.0                   // Total duration
min = 10.5                     // Minimum duration
max = 95.2                     // Maximum duration
avg = 6825.0 / 150 = 45.5    // Average duration
p50 = 45.0                     // 50th percentile
p95 = 81.5                     // 95th percentile (min + (max-min)*0.85)
p99 = 95.2                     // 99th percentile
```

#### 4f. Merge All Attributes
```go
allAttrs := mergeOTLPAttrs(resourceAttrs, dpAttrs)
// Includes all attributes + special bucket data
allAttrs["_bucketCounts"] = ["30", "45", "50", "20", "5"]
allAttrs["_explicitBounds"] = [10, 25, 50, 100, 250]

// Serialize to JSON string for storage
attributesJSON = `{
  "service.name": "payment-service",
  "host.name": "node-456",
  "http.request.method": "POST",
  "http.response.status_code": "200",
  "server.address": "api.example.com",
  "k8s.pod.name": "payment-pod-123",
  "k8s.container.name": "payment-container",
  "_bucketCounts": ["30", "45", "50", "20", "5"],
  "_explicitBounds": [10, 25, 50, 100, 250]
}`
```

#### 4g. Create MetricRecord

**Struct**: [types.go:171-195](internal/telemetry/types.go#L171-L195)

```go
MetricRecord{
    TeamUUID:       "team-abc-123",
    MetricName:     "http.server.request.duration",
    MetricType:     "histogram",
    MetricCategory: "http",
    ServiceName:    "payment-service",
    Timestamp:      time.Unix(1708864200, 0),
    Value:          45.5,      // Average for histograms
    Count:          150,
    Sum:            6825.0,
    Min:            10.5,
    Max:            95.2,
    Avg:            45.5,
    P50:            45.0,
    P95:            81.5,
    P99:            95.2,
    HTTPMethod:     "POST",
    HTTPStatusCode: 200,
    Status:         "",        // Not applicable for metrics
    Host:           "api.example.com",
    Pod:            "payment-pod-123",
    Container:      "payment-container",
    Attributes:     attributesJSON,
}
```

### Phase 5: Ingestion Strategy

**Interface**: [ingester.go](internal/telemetry/ingester.go)

```
Ingester
├── DirectIngester (synchronous ClickHouse writes)
├── KafkaIngester (async Kafka producer)
└── KafkaConsumer (async ClickHouse writes from Kafka)
```

For this example, let's assume **Kafka ingestion**:

```go
if err := h.Ingester.IngestMetrics(c.Request.Context(), metricsToInsert) {
    // Send to Kafka topic: "metrics"
}
```

The handler responds immediately:
```json
{
  "accepted": 1
}
```

### Phase 6: Storage in ClickHouse

**Repository**: [repository.go:69-114](internal/telemetry/repository.go#L69-L114)

The `MetricRecord` is inserted into the `metrics` table:

```sql
INSERT INTO metrics (
    team_id, metric_name, metric_type, metric_category,
    service_name, operation_name, timestamp,
    value, count, sum, min, max, avg,
    p50, p95, p99,
    http_method, http_status_code, status,
    host, pod, container, attributes
) VALUES (
    'team-abc-123',
    'http.server.request.duration',
    'histogram',
    'http',
    'payment-service',
    NULL,  -- operation_name (not set for metrics)
    '2024-02-25 10:30:00',
    45.5,
    150,
    6825.0,
    10.5,
    95.2,
    45.5,
    45.0,
    81.5,
    95.2,
    'POST',
    200,
    NULL,  -- status
    'api.example.com',
    'payment-pod-123',
    'payment-container',
    '{...full attributes JSON...}'
)
```

**Table Structure** (simplified):
```
metrics
├── team_id (String) - Index
├── metric_name (String) - Index
├── metric_type (String) - Enum (gauge, sum, histogram)
├── metric_category (String)
├── service_name (String) - Index
├── timestamp (DateTime) - Index
├── value (Float64)
├── count (Int64)
├── sum (Float64)
├── min (Float64)
├── max (Float64)
├── avg (Float64)
├── p50 (Float64)
├── p95 (Float64)
├── p99 (Float64)
├── http_method (String)
├── http_status_code (Int32)
├── status (String)
├── host (String)
├── pod (String)
├── container (String)
└── attributes (JSON)
```

---

## Querying Metrics

### Query 1: Get Average Response Time by Status Code

```sql
SELECT
    http_status_code,
    avg(value) as avg_duration,
    max(value) as max_duration,
    count() as sample_count
FROM metrics
WHERE
    team_id = 'team-abc-123'
    AND metric_name = 'http.server.request.duration'
    AND timestamp >= now() - INTERVAL 1 HOUR
GROUP BY http_status_code
ORDER BY avg_duration DESC
```

**Result**:
```
http_status_code  avg_duration  max_duration  sample_count
200               45.5          95.2          150
500               120.3         250.5         12
```

### Query 2: Get Percentile Distribution

```sql
SELECT
    p50,
    p95,
    p99,
    count() as samples
FROM metrics
WHERE
    team_id = 'team-abc-123'
    AND metric_name = 'http.server.request.duration'
    AND service_name = 'payment-service'
    AND timestamp >= now() - INTERVAL 1 HOUR
GROUP BY (p50, p95, p99)
```

**Result**:
```
p50   p95   p99   samples
45.0  81.5  95.2  150
```

### Query 3: Get Metrics by Pod

```sql
SELECT
    pod,
    avg(value) as avg_response_time,
    max(p99) as max_99th_percentile
FROM metrics
WHERE
    team_id = 'team-abc-123'
    AND metric_name = 'http.server.request.duration'
    AND timestamp >= now() - INTERVAL 1 HOUR
GROUP BY pod
```

**Result**:
```
pod                avg_response_time  max_99th_percentile
payment-pod-123    45.5               95.2
payment-pod-124    52.1               110.5
payment-pod-125    38.9               85.3
```

---

## Logs Integration

When the `http.server.request.duration` metric is recorded, associated logs are also ingested.

### Log Ingestion

**Endpoint**: `POST /otlp/v1/logs`

A log might arrive with the same trace context:

```json
{
  "resourceLogs": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "payment-service" } },
          { "key": "host.name", "value": { "stringValue": "node-456" } }
        ]
      },
      "scopeLogs": [
        {
          "scope": { "name": "app.logger" },
          "logRecords": [
            {
              "timeUnixNano": "1708864200050000000",
              "severityText": "INFO",
              "body": { "stringValue": "Processing POST /api/payment - Status: 200" },
              "attributes": [
                { "key": "http.request.method", "value": { "stringValue": "POST" } },
                { "key": "http.response.status_code", "value": { "intValue": "200" } },
                { "key": "trace.id", "value": { "stringValue": "abc123def456" } },
                { "key": "span.id", "value": { "stringValue": "xyz789" } }
              ]
            }
          ]
        }
      ]
    }
  ]
}
```

**Processing** ([logs_handler.go:14-102](internal/telemetry/logs_handler.go#L14-L102)):

1. Resolve team from API key
2. Extract attributes (service, host, pod, etc.)
3. Map severity number to text: `2 → INFO`, `8 → WARN`, `10 → ERROR`, etc.
4. Extract message from body
5. Create `LogRecord` struct

```go
LogRecord{
    TeamUUID:   "team-abc-123",
    Timestamp:  time.Unix(1708864200, 50000000),
    Level:      "INFO",
    Service:    "payment-service",
    Logger:     "app.logger",
    Message:    "Processing POST /api/payment - Status: 200",
    TraceID:    "abc123def456",    // Links to traces
    SpanID:     "xyz789",           // Links to spans
    Host:       "node-456",
    Pod:        "payment-pod-123",
    Container:  "payment-container",
    Thread:     "",
    Exception:  "",
    Attributes: "{...JSON...}",
}
```

**Storage** in `logs` table:
```
logs
├── team_id (String) - Index
├── timestamp (DateTime) - Index
├── level (String) - Enum (DEBUG, INFO, WARN, ERROR, FATAL)
├── service_name (String) - Index
├── logger (String)
├── message (String) - Full text indexed
├── trace_id (String) - Links to traces
├── span_id (String) - Links to spans
├── host (String)
├── pod (String)
├── container (String)
├── thread (String)
├── exception (String)
└── attributes (JSON)
```

### Log Queries with Trace Context

```sql
-- Find logs for a specific request (by trace_id)
SELECT
    timestamp,
    level,
    message,
    span_id
FROM logs
WHERE
    team_id = 'team-abc-123'
    AND trace_id = 'abc123def456'
ORDER BY timestamp
```

**Result**:
```
timestamp                level    message                                  span_id
2024-02-25 10:30:00.01  INFO     Starting payment processing              abc123def456
2024-02-25 10:30:00.03  INFO     Calling payment gateway                  def123
2024-02-25 10:30:00.05  INFO     Processing POST /api/payment - 200       xyz789
2024-02-25 10:30:00.08  INFO     Payment completed successfully           xyz789
```

---

## Spans Integration (Distributed Tracing)

The same request also generates spans showing the breakdown of operations.

### Span Ingestion

**Endpoint**: `POST /otlp/v1/traces`

```json
{
  "resourceSpans": [
    {
      "resource": {
        "attributes": [
          { "key": "service.name", "value": { "stringValue": "payment-service" } }
        ]
      },
      "scopeSpans": [
        {
          "scope": { "name": "opentelemetry.instrumentation.http" },
          "spans": [
            {
              "traceId": "abc123def456",
              "spanId": "xyz789",
              "parentSpanId": "abc123def456",
              "name": "POST /api/payment",
              "kind": 2,  // SERVER span
              "startTimeUnixNano": "1708864200000000000",
              "endTimeUnixNano": "1708864200050000000",
              "attributes": [
                { "key": "http.request.method", "value": { "stringValue": "POST" } },
                { "key": "http.response.status_code", "value": { "intValue": "200" } },
                { "key": "url.full", "value": { "stringValue": "/api/payment" } },
                { "key": "server.address", "value": { "stringValue": "api.example.com" } }
              ],
              "status": { "code": 1, "message": "" }  // 1 = OK
            }
          ]
        }
      ]
    }
  ]
}
```

**Processing** ([traces_handler.go:14-123](internal/telemetry/traces_handler.go#L14-L123)):

1. Extract trace ID and span ID
2. Determine if root span (no parent)
3. Calculate duration: `endTime - startTime = 50ms`
4. Extract status (1=OK, 2=ERROR)
5. Create `SpanRecord` struct

```go
SpanRecord{
    TeamUUID:       "team-abc-123",
    TraceID:        "abc123def456",
    SpanID:         "xyz789",
    ParentSpanID:   "abc123def456",
    IsRoot:         0,             // Has parent
    OperationName:  "POST /api/payment",
    ServiceName:    "payment-service",
    SpanKind:       "SERVER",
    StartTime:      time.Unix(1708864200, 0),
    EndTime:        time.Unix(1708864200, 50000000),
    DurationMs:     50,
    Status:         "OK",
    StatusMessage:  "",
    HTTPMethod:     "POST",
    HTTPURL:        "/api/payment",
    HTTPStatusCode: 200,
    Host:           "api.example.com",
    Pod:            "payment-pod-123",
    Container:      "payment-container",
    Attributes:     "{...JSON...}",
}
```

**Storage** in `spans` table:
```
spans
├── team_id (String) - Index
├── trace_id (String) - Index (trace lookup)
├── span_id (String) - Index
├── parent_span_id (String)
├── is_root (Int8)
├── operation_name (String) - Index
├── service_name (String) - Index
├── span_kind (String) - Enum (INTERNAL, SERVER, CLIENT, PRODUCER, CONSUMER)
├── start_time (DateTime)
├── end_time (DateTime)
├── duration_ms (Int64)
├── status (String) - Enum (OK, ERROR)
├── status_message (String)
├── http_method (String)
├── http_url (String)
├── http_status_code (Int32)
├── host (String)
├── pod (String)
├── container (String)
└── attributes (JSON)
```

### Span Queries

```sql
-- Get the trace tree for a request
SELECT
    span_id,
    parent_span_id,
    operation_name,
    duration_ms,
    status
FROM spans
WHERE
    team_id = 'team-abc-123'
    AND trace_id = 'abc123def456'
ORDER BY start_time
```

**Result**:
```
span_id         parent_span_id  operation_name          duration_ms  status
abc123def456    (null)          POST /api/payment       50           OK
xyz789          abc123def456    db.query.execute        15           OK
uvw456          abc123def456    http.call.payment_gw    30           OK
```

---

## Correlation: Metrics → Logs → Spans

The three telemetry types are interconnected via common attributes:

### Timeline of a Single Request

```
Time       Component        Event                           Duration
────────────────────────────────────────────────────────────
00ms       HTTP Handler     Receive POST /api/payment
           ├─ Span Start    "POST /api/payment" (SERVER)
           ├─ Log           "Starting payment processing"
           │
02ms       ├─ DB Service    Execute SELECT query            (15ms)
           │ ├─ Span        "db.query.execute" (CLIENT)
           │ ├─ Log         "Query: SELECT * FROM users"
           │
20ms       ├─ Payment GW    Call external gateway           (30ms)
           │ ├─ Span        "http.call.payment_gw" (CLIENT)
           │ ├─ Log         "Calling payment gateway"
           │ └─ Log         "Gateway response: 200"
           │
50ms       └─ Span End      Duration: 50ms
             └─ Metrics     http.server.request.duration
                ├─ value: 50ms
                ├─ count: 1
                ├─ status: 200
             └─ Log         "Processing POST /api/payment - Status: 200"
```

### Query to Correlate All Three

```sql
-- Metrics + Logs + Spans in single view
SELECT
    m.timestamp as metric_time,
    m.value as response_time_ms,
    COUNT(DISTINCT l.message) as log_entries,
    COUNT(DISTINCT s.span_id) as span_count,
    s.status as trace_status
FROM metrics m
LEFT JOIN logs l ON m.team_id = l.team_id
    AND m.timestamp = l.timestamp
    AND m.service_name = l.service_name
LEFT JOIN spans s ON m.team_id = s.team_id
    AND m.timestamp >= s.start_time
    AND m.timestamp <= s.end_time
WHERE
    m.team_id = 'team-abc-123'
    AND m.metric_name = 'http.server.request.duration'
    AND m.http_status_code = 200
    AND m.timestamp >= now() - INTERVAL 1 HOUR
GROUP BY metric_time, response_time_ms, trace_status
ORDER BY metric_time DESC
LIMIT 100
```

---

## Key Design Patterns

### 1. Attributes as Pre-serialized JSON
- All dynamic attributes stored as JSON in `attributes` column
- Avoids rigid schema for arbitrary tags
- Indexed for common fields like `http.method`, `http.status_code`

### 2. Known Fields Extracted
- Common attributes (host, pod, container, status) extracted to dedicated columns
- Enables efficient filtering and aggregation
- HTTP attributes extracted for quick metric queries

### 3. Metric Type Specific Aggregates
- **Gauge**: Single value snapshot
- **Sum**: Running total with count
- **Histogram**: Full distribution (count, sum, min, max, avg, p50, p95, p99)

### 4. Trace Context Propagation
- Logs and spans linked via `trace_id` and `span_id`
- Enables request tracing across services
- Correlation between metrics, logs, and spans

### 5. Team Multi-tenancy
- Every record partitioned by `team_id`
- API key resolves to team UUID
- Prevents cross-tenant data leakage

---

## Performance Characteristics

| Operation | Time Complexity | Details |
|-----------|-----------------|---------|
| Metric Ingestion | O(1) | Single insert + attributes serialization |
| Log Ingestion | O(1) | Single insert + level normalization |
| Span Ingestion | O(1) | Single insert + duration calculation |
| Query by Metric Name | O(log N) | Indexed on (team_id, metric_name, timestamp) |
| Query by Trace ID | O(log N) | Indexed on (team_id, trace_id) |
| Aggregation | O(N) | ClickHouse engine optimized for OLAP |
| Correlation | O(N²) | JOIN on timestamps (slower, use sparingly) |

---

## Error Handling

### Ingestion Failures
- Individual metric/log/span failures don't block batch
- Failed records logged with reason
- Partial batches accepted (all-or-nothing per record)
- HTTP 200 with accepted count even if some fail

### Validation
- API key validation: 401 if missing/invalid
- OTLP schema validation: 400 if malformed
- Database constraints enforced at storage layer
- Null values handled gracefully

---

## Summary

A metric like `http.server.request.duration` flows through:
1. **Ingestion** (HTTP + Protocol parsing)
2. **Extraction** (Resource + Datapoint attributes)
3. **Normalization** (Category, labels, aggregates)
4. **Storage** (ClickHouse metrics table)
5. **Correlation** (Links with logs via message, spans via trace_id)
6. **Querying** (Efficient OLAP aggregations)

The same request generates interconnected records in all three telemetry types, enabling comprehensive observability.
