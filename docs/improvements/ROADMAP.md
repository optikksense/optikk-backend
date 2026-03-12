# Observability Audit — Implementation Roadmap

**Status**: Week 1 ✅ | Week 2-3 📋 | Month 2 📋

---

## Week 2-3 — HIGH Severity (8 items, ~40 hours)

### MG-1: Prometheus Metrics Infrastructure (8 hours)
**Goal**: Add foundational metrics instrumentation across the backend.

**What to add**:
- HTTP request metrics (via Gin middleware):
  - `http_request_duration_seconds` histogram (p50, p95, p99)
  - `http_requests_total` counter per route + status code
- ClickHouse query metrics (Query/QueryRow methods):
  - `clickhouse_query_duration_seconds` histogram
  - `clickhouse_queries_total` counter
- MySQL query metrics:
  - `mysql_query_duration_seconds` histogram
  - `mysql_queries_total` counter
- Ingest pipeline metrics:
  - `ingest_queue_depth` gauge per signal type (spans/logs/metrics)
  - `ingest_rows_flushed_total` counter
  - `ingest_flush_duration_seconds` histogram
  - `ingest_backpressure_total` counter
- Circuit breaker state:
  - `circuit_breaker_state` gauge (0=closed, 1=open, 2=half-open) per breaker name
- Auth metrics:
  - `auth_failures_total` counter per failure type (UNAUTHORIZED, FORBIDDEN_TEAM, etc.)

**Implementation**:
- Add `go.mod` dependency: `github.com/prometheus/client_golang/prometheus`
- New package: `internal/platform/metrics/`
- Middleware for HTTP metrics
- Wrap DB calls with timing + histogram recording
- Wrap ingest queue with counter + histogram
- Wrap circuit breaker with gauge updates

**Files to create**: `internal/platform/metrics/metrics.go`, `internal/platform/metrics/middleware.go`
**Files to modify**: `internal/database/clickhouse.go`, `internal/database/mysql.go`, `internal/platform/ingest/ingest.go`, `internal/platform/circuit_breaker/breaker.go`, `internal/platform/server/app.go`

---

### TG-1: OpenTelemetry Self-Instrumentation (6 hours)
**Goal**: Add distributed tracing to backend's own operations.

**What to add**:
- OTel SDK initialization in `main.go`:
  - Jaeger exporter (or stdout for dev)
  - Batch span processor
  - Global tracer provider
- Spans for:
  - HTTP request handling (middleware)
  - ClickHouse queries (duration, query, team_id as attributes)
  - MySQL queries (duration, query, team_id as attributes)
  - Ingest flush operations (batch size, table, duration)
  - Auth events (login, logout, denial with reason)
  - Circuit breaker state transitions

**Implementation**:
- Add `go.mod`: `go.opentelemetry.io/otel`, `go.opentelemetry.io/otel/exporters/jaeger`, `go.opentelemetry.io/otel/sdk/trace`
- New package: `internal/platform/tracing/`
- Initialize SDK in `main.go` before `app.Start()`
- Middleware to create request spans
- Wrapper functions around DB queries to create child spans
- Span attributes: team_id, user_id, query_text (for SELECT only), duration, error (if any)

**Files to create**: `internal/platform/tracing/tracing.go`
**Files to modify**: `cmd/server/main.go`, `internal/platform/middleware/middleware.go`, `internal/database/clickhouse.go`, `internal/database/mysql.go`, `internal/platform/ingest/ingest.go`

---

### LG-2-5: Structured Logging Expansion (4 hours)
**What we have**: Basic server-side error logging + auth event logging
**What we need**:
- ✅ Server error logging (done)
- ✅ Auth event logging (done)
- ❌ DB query details (query, duration, team_id, rows affected)
- ❌ Queue depth periodic logging (every 30s per queue)
- ❌ Cache hit/miss counters
- ❌ External API call logging (Slack, etc.) — duration, status, retries

**Implementation**:
- Enhance DB wrapper logging to include: table hint, row count, team_id context
- Add periodic ticker in ingest queue: log queue depth every 30s
- Add cache hit/miss counters (atomic.Int64) to JWT cache, API key cache, query cache
- Log external calls with duration, status, retry count

**Files to modify**: `internal/database/clickhouse.go`, `internal/database/mysql.go`, `internal/platform/ingest/ingest.go`, `internal/platform/auth/auth.go`, `internal/platform/otlp/auth/auth.go`, `internal/platform/cache/middleware.go`

---

### SB-3: Span Deduplication via ReplacingMergeTree (4 hours)
**Problem**: Same span retried on client side → duplicate rows in ClickHouse
**Current**: All spans insert as CREATE (no dedup)
**Solution**: Change `observability.spans` table to `ReplacingMergeTree` with dedup key `(team_id, span_id)`

**Implementation**:
- Modify `db/clickhouse/spans.sql` schema:
  - Change `MergeTree` → `ReplacingMergeTree(version_column)`
  - Add `version` column (incremented on each insert, defaults to 1)
  - Dedup key: `(team_id, span_id)`
- ClickHouse automatically deduplicates within merge window
- Migrations: data migration to backfill existing spans (set version=1)

**Files to modify**: `db/clickhouse/spans.sql`, migration script

---

### SB-4: Idempotency Keys on Mutations (4 hours)
**Problem**: POST/PUT without idempotency → double-submit creates duplicates
**Affected endpoints**: POST teams, POST users, PUT dashboard config, etc.

**Implementation**:
- New middleware: `middleware.IdempotencyKey()`
  - Accept `Idempotency-Key` header (UUID or any string)
  - Cache in Redis: `idempotency:{key}` → `{response_json}` with 24h TTL
  - On retry with same key: return cached response
  - Generate unique key from `{method}:{path}:{idempotency_key_header}`
- Apply to all POST/PUT handlers
- If no key provided, generate one (but cache won't work)

**Files to create**: `internal/platform/middleware/idempotency.go`
**Files to modify**: `internal/platform/server/app.go` (apply middleware to v1 group)

---

### SB-5/6: Query Limits + Pagination (2 hours)
**Problem**: Service list aggregation without LIMIT → unbounded results at 10K+ services
**Also**: Team fetch on login returns all teams without pagination

**Implementation**:
- Add `LIMIT 500` to service list queries
- Add cursor-based pagination to team list queries
- Enforce max 100 results per page in API contracts

**Files to modify**: `internal/modules/services/service/repository.go`, `internal/modules/user/auth.go`

---

## Month 2 — MEDIUM Severity + Dashboards (16 items, ~40 hours)

### MC-1 to MC-9: System + Ingest Dashboards (8 hours)
**Platform**: Grafana + Prometheus data source

**System Health Dashboard**:
- API Request Rate by Endpoint (line chart, requests/sec)
- Error Rate % by Endpoint (area chart, >1% alert)
- P95 Latency Heatmap (heatmap, >5s alert)
- DB Connection Pool Utilization (gauge, >80% alert)
- Circuit Breaker State (status panel, open=red)

**Ingest Pipeline Dashboard**:
- Queue Depth by Signal Type (multi-line, spans/logs/metrics)
- Flush Latency P95 (histogram, >10s alert)
- Rows Ingested/sec by Type (stacked area)
- Backpressure Events/min (bar chart, >10/min alert)

**Implementation**: Grafana JSON via API or UI

---

### MC-10-16: Business + Dependency Dashboards (8 hours)

**Product Dashboard**:
- Active Teams by Usage Tier (table with team_id, data_ingested_kb, plan)
- Feature Usage Frequency (bar chart, routes ranked by request count)
- Error Impact — Unique Users Affected (stat, >10 users in 1h alert)

**Dependency Health**:
- ClickHouse Query Duration P95 (histogram, >10s alert)
- MySQL Query Duration P95 (histogram, >2s alert)
- Slack Webhook Success Rate (gauge, <90% alert)
- Redis Availability (status panel, check via PING)

---

### IC-4/IC-11: httpOnly Cookie Migration (Frontend, 4 hours)
**Currently**: Token stored in localStorage (XSS vulnerable)
**Solution**: Use httpOnly cookies instead

**Implementation**:
- Backend login handler already sets `Set-Cookie: token=JWT; HttpOnly; SameSite=Strict`
- Frontend axios config: `withCredentials: true`
- Remove localStorage token storage
- Extract token from cookie (browser handles it automatically)

**Files to modify**: `optic-frontend/src/shared/api/auth/authStorage.ts`, axios interceptors

---

### IC-5/IC-6: Mutation Retry + Cache Invalidation (Frontend, 3 hours)
**Problem**:
- Mutations (POST/PUT) not retried on failure
- Cache invalidation on team switch is fire-and-forget (race condition)

**Implementation**:
- Add idempotency key header to all POST/PUT calls
- Enable retries for idempotent mutations in axios interceptor
- Await cache invalidation before navigation

**Files to modify**: `optic-frontend/src/shared/api/api/interceptors/retryInterceptor.ts`, `optic-frontend/src/shared/store/appStore.ts`

---

## Low Priority — Ongoing

### Frontend Optimizations (SB-11-15)
- Consolidate charting libraries (recharts vs chart.js)
- Add debounce to filter onChange (300ms)
- Fix staleTime: 0 defeats caching
- Proper Zod schemas for validation

### API Consistency (IC-3)
- Standardize pagination responses: `{data, pagination: {hasMore, nextCursor, total}, summary}`
- Console cleanup (IC-9, IC-10)

---

## Metrics Summary

| Phase | Items | Effort | Priority | Status |
|-------|-------|--------|----------|--------|
| **Week 1** | 10 | 30h | CRITICAL | ✅ Complete |
| **Week 2-3** | 8 | 40h | HIGH | 📋 Ready |
| **Month 2** | 16 | 40h | MEDIUM | 📋 Planned |
| **Ongoing** | 8 | TBD | LOW | 📋 Backlog |
| **TOTAL** | 42+ | 110h+ | — | — |

---

## Execution Notes

1. **Week 2-3 critical path**:
   - Start with Prometheus (MG-1) — enables all dashboard work
   - Parallel: OTel setup (TG-1), idempotency (SB-4)
   - Then: query limits, logging expansion

2. **Frontend work** (IC-4, IC-5, IC-6):
   - Can run in parallel with backend Week 2-3
   - Depends on: backend idempotency key support (SB-4)

3. **Month 2** (dashboards):
   - Requires: Prometheus + Grafana instance + datasource
   - Can start after Week 2-3 MG-1 complete

---

See `WEEK_1_COMPLETE.md` for what's already done.
