# Production Scalability Improvement Plan

## Audit Date: 2026-03-10

This document identifies production scalability bottlenecks found during a full codebase audit and provides a prioritized improvement plan.

---

## Executive Summary

The backend handles moderate load (~1K spans/sec) but breaks down at production scale due to:
1. **Ingest pipeline** - O(n^2) attribute lookups, redundant allocations, inefficient Kafka codec
2. **Query layer** - No PREWHERE optimization, untuned ClickHouse SETTINGS, expensive JOINs
3. **Connection management** - Missing dial/query timeouts, no MySQL query timeout
4. **Schema** - ORDER BY key not optimal for common access patterns, materialized columns underused

---

## 1. INGEST PIPELINE (Critical Path)

### 1.1 Replace Linear Attribute Lookups with Map (P0)

**File**: `internal/platform/otlp/mapper.go`, `internal/platform/otlp/grpc/mapper.go`

**Problem**: `lookupAttr()` does O(n) linear scan per lookup. Each span triggers 10+ lookups = O(n*k) per span.

**Fix**: Build attribute map once per span, use O(1) map lookups.

```go
// Before (current) - called 10+ times per span
func lookupAttr(kvs []KeyValue, key string) string {
    for _, kv := range kvs { // O(n) each time
        if kv.Key == key { return anyValueString(kv.Value) }
    }
    return ""
}

// After - build map once, reuse
func buildAttrMap(kvs []KeyValue) map[string]string {
    m := make(map[string]string, len(kvs))
    for _, kv := range kvs {
        m[kv.Key] = anyValueString(kv.Value)
    }
    return m
}
```

**Impact**: Eliminates ~10,000 comparisons per 1,000-span batch.

### 1.2 Eliminate Redundant Attribute Processing (P0)

**File**: `internal/platform/otlp/mapper.go` lines 257-260

**Problem**: Each span does:
1. Allocate merged slice (resAttrs + spanAttrs)
2. `attrsToJSON()` creates ANOTHER map from the merged slice
3. `json.Marshal()` serializes it

Three separate iterations + two allocations per span.

**Fix**: Merge resource + span attributes into a single map once. Use that map for both lookups AND JSON serialization.

```go
// Single allocation per span
attrMap := mergeAttrMaps(resAttrMap, spanAttrMap) // merge two pre-built maps
attrJSON, _ := json.Marshal(attrMap)              // serialize once
httpMethod := attrMap["http.method"]               // O(1) lookup
```

**Impact**: Cuts per-span allocations from ~6 to ~2.

### 1.3 Fix Gob Codec in Kafka Path (P1)

**File**: `internal/platform/ingest/ingest.go` lines 140-154

**Problem**: Creates new `gob.Encoder` + `bytes.Buffer` per row (1,000 per batch).

**Fix options**:
- Pool encoders/buffers with `sync.Pool`
- Switch to MessagePack or protobuf (faster, smaller)
- Batch-encode all rows into a single Kafka message

### 1.4 Add Request Size Limits (P1)

**File**: `internal/platform/otlp/handler.go`

**Problem**: No max payload size. A 1GB payload causes OOM.

**Fix**: Add `http.MaxBytesReader` before `io.ReadAll`:
```go
c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 50*1024*1024) // 50MB
```

### 1.5 Batch ByteTracker MySQL Updates (P2)

**File**: `internal/platform/ingest/tracker.go`

**Problem**: Sequential UPDATE per team. 100 teams = 100 round-trips.

**Fix**: Single batch UPDATE:
```sql
INSERT INTO team_bytes_staging (team_id, bytes_kb) VALUES (?,?),(?,?),...
ON DUPLICATE KEY UPDATE bytes_kb = bytes_kb + VALUES(bytes_kb)
```

### 1.6 Optimize Queue Drain (P2)

**File**: `internal/platform/ingest/ingest.go` line 318

**Problem**: `drainBatchLocked()` copies rows into a new slice.

**Fix**: Re-slice without copy:
```go
batch := q.buf[:n:n]
q.buf = q.buf[n:]
```

---

## 2. CLICKHOUSE QUERY LAYER

### 2.1 Add PREWHERE to All Queries (P0)

**Files**: Every `repository.go` / `store.go`

**Problem**: All queries use WHERE for `team_id` and `ts_bucket_start`. ClickHouse reads entire granules before filtering.

**Fix**: Use PREWHERE for partition/primary-key columns:
```sql
-- Before
SELECT ... FROM spans s WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ?

-- After
SELECT ... FROM spans s PREWHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ?
WHERE s.service_name = ? AND s.duration_nano > ?
```

**Impact**: Skips granule decompression for non-matching blocks. 2-10x speedup on large tables.

### 2.2 Use Materialized Columns Instead of JSON Extraction (P1)

**Files**: `internal/modules/spans/errortracking/repository.go`, others

**Problem**: Queries use `s.attributes.'http.route'::String` instead of pre-computed `mat_http_route`.

**Fix**: Replace all JSON path accesses with materialized column references where available:
- `attributes.'http.route'` -> `mat_http_route`
- `attributes.'http.status_code'` -> `mat_http_status_code`
- `attributes.'db.name'` -> `mat_db_name`
- `attributes.'net.peer.name'` -> `mat_host_name`

### 2.3 Eliminate toJSONString() Double Conversion (P1)

**File**: `internal/modules/spans/store.go` line 294, `servicemap/repository.go` line 81

**Problem**: `JSONExtractString(toJSONString(s.attributes), 'key')` does JSON->String->JSON->Extract.

**Fix**: Use native JSON path: `s.attributes.'net.peer.name'::String` (or materialized columns).

### 2.4 Add Query SETTINGS for Production (P1)

**File**: `internal/database/clickhouse.go`

Add per-query-type settings:
```sql
-- For aggregation queries
SETTINGS max_rows_to_read=100000000, read_overflow_mode='throw'

-- For time-series queries
SETTINGS optimize_read_in_order=1

-- For GROUP BY on sort key columns
SETTINGS optimize_aggregation_in_order=1
```

### 2.5 Optimize Self-JOIN Queries (P2)

**Files**: `spans/store.go` (GetServiceDependencies), `topology/repository.go`, `servicemap/repository.go`

**Problem**: JOINing spans table to itself on `trace_id` is expensive.

**Fix options**:
- Pre-compute service dependencies into a materialized view:
  ```sql
  CREATE MATERIALIZED VIEW service_dependencies
  ENGINE = AggregatingMergeTree()
  ORDER BY (team_id, ts_bucket_start, caller_service, callee_service)
  AS SELECT ... GROUP BY ...
  ```
- Add `trace_id` to ORDER BY key as a secondary index (skip index)

### 2.6 Add ClickHouse Skip Indexes (P2)

**File**: Schema DDL

```sql
-- Add bloom filter on trace_id for trace detail lookups
ALTER TABLE spans ADD INDEX idx_trace_id trace_id TYPE bloom_filter GRANULARITY 4;

-- Add set index on status_code for error queries
ALTER TABLE spans ADD INDEX idx_status_code status_code TYPE set(3) GRANULARITY 4;
```

---

## 3. CONNECTION MANAGEMENT

### 3.1 Add MySQL Query Timeouts (P0)

**File**: `internal/database/mysql.go`

**Problem**: No per-query timeout. Runaway MySQL queries block connection pool.

**Fix**: Add DSN timeout params:
```go
dsn := fmt.Sprintf("...?timeout=5s&readTimeout=30s&writeTimeout=30s")
```

And use context timeouts in handlers:
```go
ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
defer cancel()
```

### 3.2 Add ClickHouse Dial Timeout (P1)

**File**: `internal/database/clickhouse.go`

**Problem**: No dial timeout. Network issues hang forever.

**Fix**: Add to ClickHouse options:
```go
clickhouse.Open(&clickhouse.Options{
    DialTimeout: 5 * time.Second,
    ReadTimeout: 30 * time.Second,
    ...
})
```

### 3.3 Enable Redis Rate Limiter for Multi-Pod (P1)

**File**: `internal/platform/server/app.go`

**Problem**: In-process rate limiter doesn't enforce global limits across replicas.

**Fix**: Wire up Redis-based rate limiter when `REDIS_ENABLED=true`:
```go
if cfg.RedisEnabled {
    limiter = middleware.NewRedisRateLimiter(redisClient, 1000, 2000, time.Second)
}
```

---

## 4. SCHEMA IMPROVEMENTS

### 4.1 Evaluate ORDER BY Key (P2)

**Current**: `ORDER BY (team_id, ts_bucket_start, service_name, name, timestamp)`

**Consider**: For trace-detail queries (lookup by trace_id), the current ORDER BY provides no benefit. Add a skip index:
```sql
ALTER TABLE spans ADD INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1;
```

### 4.2 Increase max_dynamic_paths on JSON Column (P2)

**Current**: `attributes JSON(max_dynamic_paths=50)`

**Problem**: Spans with >50 unique attribute keys silently drop extras.

**Fix**: Increase to 200+ and monitor:
```sql
ALTER TABLE spans MODIFY COLUMN attributes JSON(max_dynamic_paths=200);
```

### 4.3 Add Pre-Aggregated Materialized Views (P2)

For dashboard queries that always compute the same aggregations:

```sql
-- Service-level RED metrics per minute
CREATE MATERIALIZED VIEW mv_service_red_1m
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(ts_bucket_start)
ORDER BY (team_id, ts_bucket_start, service_name)
AS SELECT
    team_id,
    toStartOfMinute(toDateTime(ts_bucket_start)) as ts_bucket_start,
    service_name,
    countState() as request_count,
    countIfState(status_code = 2) as error_count,
    quantileState(0.95)(duration_nano) as p95_duration
FROM spans
GROUP BY team_id, ts_bucket_start, service_name;
```

### 4.4 Align Production and Local Schema TTL (P3)

**Production**: 30d warm -> 90d cold -> 365d delete (requires `tiered_gcs` storage policy)
**Local**: 365d delete

Ensure deployment documentation covers storage policy setup.

---

## 5. OBSERVABILITY & RESILIENCE

### 5.1 Add Prometheus Metrics (P1)

Export key metrics:
- `ingest_queue_size` (gauge) - current queue depth per queue type
- `ingest_batch_duration_seconds` (histogram) - ClickHouse batch insert time
- `ingest_rows_total` (counter) - rows ingested per queue type
- `clickhouse_query_duration_seconds` (histogram) - query execution time
- `clickhouse_pool_active_connections` (gauge) - connection pool utilization
- `mysql_pool_active_connections` (gauge)
- `circuit_breaker_state` (gauge) - 0=closed, 1=open, 2=half-open

### 5.2 Implement HTTP 429 Backpressure in All Handlers (P1)

**Problem**: Backpressure mechanism exists but some handlers may not propagate it.

**Fix**: Ensure all OTLP ingest handlers check for `ErrBackpressure` and return 429 + `Retry-After` header.

### 5.3 Add Health Check with Dependency Status (P2)

Extend `/health` to report:
- ClickHouse connectivity + pool stats
- MySQL connectivity + pool stats
- Queue utilization (% of max)
- Circuit breaker state

---

## Implementation Priority

| Phase | Items | Expected Impact |
|-------|-------|-----------------|
| **Phase 1 (Week 1)** | 1.1, 1.2, 2.1, 3.1 | 3-5x ingest throughput, 2-10x query speed |
| **Phase 2 (Week 2)** | 1.3, 1.4, 2.2, 2.3, 2.4, 3.2, 3.3 | Stability under load, reduced CPU |
| **Phase 3 (Week 3)** | 2.5, 2.6, 4.1, 4.2, 5.1, 5.2 | Long-term scalability |
| **Phase 4 (Week 4)** | 1.5, 1.6, 4.3, 4.4, 5.3 | Polish + monitoring |

---

## Expected Outcome

After implementing Phase 1-2:
- Ingest capacity: **1K spans/sec -> 15-20K spans/sec**
- Dashboard query latency (p95): **5-10s -> 500ms-2s**
- Memory per batch: **~50% reduction** from eliminating redundant allocations
- Connection stability: Timeouts prevent pool exhaustion

After Phase 3-4:
- Ingest capacity: **20K-50K spans/sec** (with Kafka + materialized views)
- Dashboard queries: **sub-second** with pre-aggregated views
- Full observability into system health
