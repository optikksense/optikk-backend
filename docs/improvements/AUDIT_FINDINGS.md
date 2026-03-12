# Full-Stack Observability Audit — 62 Findings

**Report Date**: 2026-03-12
**Scope**: Backend (Go/Gin) + Frontend (React/TypeScript)
**Total Findings**: 62 | CRITICAL: 4 | HIGH: 16 | MEDIUM: 28 | LOW: 14

---

## DIMENSION 1: SCALE BUGS (15 findings)

### CRITICAL ⚠️

| ID | Severity | Issue | File | Line | Fix |
|----|----------|-------|------|------|-----|
| SB-1 | CRITICAL | Hardcoded ClickHouse credentials in source | `internal/database/clickhouse.go` | 42-51, 78-88 | Externalize to `CLICKHOUSE_CLOUD_HOST`, `CLICKHOUSE_CLOUD_PASSWORD` env vars |
| SB-2 | CRITICAL | Alerting N+1 + no HTTP timeout + type panics | `internal/platform/alerting/engine.go` | 28-103 | Add 5s timeout, bounded 10-worker pool, type-safe assertions, `LIMIT 1000` |
| SB-11 | CRITICAL | Auth tokens in localStorage (XSS) | `optic-frontend/src/shared/api/auth/authStorage.ts` | 44 | Migrate to httpOnly cookie flow |

### HIGH 🔴

| ID | Severity | Issue | File | Line | Fix |
|----|----------|-------|------|------|-----|
| SB-3 | HIGH | No span deduplication on client retry | `internal/platform/otlp/handler.go` | 85-138 | Use `ReplacingMergeTree(version)` with dedup key `(team_id, span_id)` |
| SB-4 | HIGH | RespondError swallows 500 errors | `internal/modules/common/base.go` | 30 | Log errors server-side: `log.Printf("ERROR %s %s: %v", method, path, err)` |
| SB-5 | HIGH | Login errors silently ignored | `internal/modules/user/auth.go` | 64, 67-69 | Log `UpdateUserLastLogin` errors and team fetch failures |
| SB-6 | HIGH | No idempotency on mutations | `internal/modules/defaultconfig/handler.go` | 102-154 | Accept `Idempotency-Key` header, cache responses in Redis 24h |
| SB-7 | HIGH | Type assertion panic on MySQL types | `internal/platform/alerting/engine.go` | 46 | Use `Int64FromAny()`/`StringFromAny()` instead of `.()` |
| SB-10 | HIGH | Auth failures not logged | `internal/platform/middleware/middleware.go` | 169-223 | Log `AUTH_DENIED [method path] code=X ip=Y` on each rejection |

### MEDIUM 🟡

| ID | Severity | Issue | File | Fix |
|----|----------|-------|------|-----|
| SB-8 | MEDIUM | Queue backpressure silent drop | `internal/platform/ingest/ingest.go` | Log on every backpressure with queue name + depth |
| SB-9 | MEDIUM | Service list without LIMIT | `internal/modules/services/service/repository.go` | Add `LIMIT 500` to aggregation queries |
| SB-12 | MEDIUM | Unthrottled filter onChange | `optic-frontend/src/features/metrics/components/MetricsFilterBar.tsx` | Add 300ms debounce |
| SB-13 | MEDIUM | staleTime: 0 defeats caching | `optic-frontend/src/shared/hooks/useTimeRangeQuery.ts` | Set `staleTime: 5_000` |
| SB-14 | MEDIUM | Dual charting libraries (recharts + chart.js) | `optic-frontend/package.json` | Consolidate to one library |
| SB-15 | LOW | Zod z.any() defeats validation | `optic-frontend/src/features/traces/api/tracesApi.ts` | Define proper Zod schema |

---

## DIMENSION 2: OBSERVABILITY GAPS (31 findings)

### LOG GAPS (8 findings)

| ID | Severity | Issue | File | Line | Fix |
|----|----------|-------|------|------|-----|
| LG-1 | CRITICAL | No server-side 500 error logging | `internal/modules/common/base.go` | 30 | Add `log.Printf("ERROR [%s %s] %s: %v", method, path, code, err)` |
| LG-2 | HIGH | No slow query tracking | `internal/database/clickhouse.go`, `mysql.go` | Query/QueryRow | Log if duration >100ms with query text |
| LG-3 | HIGH | No auth event audit trail | `internal/modules/user/auth.go` | 64, 104-111 | Log: login_success, login_update_failed, team_fetch_failed, logout |
| LG-4 | HIGH | Queue depth invisible | `internal/platform/ingest/ingest.go` | — | Log queue depth every 30s + on backpressure |
| LG-5 | HIGH | Circuit breaker transitions silent | `internal/platform/circuit_breaker/breaker.go` | 67-96 | Log state transitions with name + failure count |
| LG-6 | MEDIUM | No cache hit/miss logging | Auth, API key, query caches | — | Periodic counters (every 60s) |
| LG-7 | MEDIUM | No external API call logging | `internal/platform/alerting/engine.go` | 93 | Log Slack webhook duration, status, retry |
| LG-8 | MEDIUM | No flush duration logging | `internal/platform/ingest/ingest.go` | 255 | Log `time.Since(start)` for each flush |

### METRIC GAPS (3 findings)

| ID | Severity | Issue | Impact | Fix |
|----|----------|-------|--------|-----|
| MG-1 | CRITICAL | Zero metrics instrumentation | Platform has no Prometheus/OTel metrics | Add: http_request_duration, ingest_queue_depth, circuit_breaker_state, auth_failures_total |
| MG-2 | HIGH | No ingest throughput metrics | Cannot see rows/sec, bytes/sec, flush latency | Add: ingest_rows_flushed_total, ingest_flush_duration_seconds |
| MG-3 | HIGH | No DB pool utilization | Cannot detect connection exhaustion | Export `sql.DB.Stats()` as gauge metrics |
| MG-4 | MEDIUM | No cache effectiveness | Cannot tune TTLs | Add cache_hits_total, cache_misses_total counters |

### TRACE GAPS (2 findings)

| ID | Severity | Issue | Impact | Fix |
|----|----------|-------|--------|-----|
| TG-1 | CRITICAL | Zero backend self-instrumentation | No visibility into own operations | Setup OTel SDK, add spans: HTTP, DB queries, ingest flushes |
| TG-2 | HIGH | No request correlation IDs | Cannot tie logs/traces/metrics | Add RequestIDMiddleware, propagate in headers |

---

## DIMENSION 3: MISSING CHARTS & DASHBOARDS (16 findings)

### System Health Dashboard (5 charts)

| ID | Chart | Type | Source | Alert |
|----|-------|------|--------|-------|
| MC-1 | API Request Rate by Endpoint | Line | `http_requests_total` counter | None (baseline) |
| MC-2 | Error Rate % by Endpoint | Area | `http_requests_total{status=~"5.."}` | >1% for 5min |
| MC-3 | P95 Latency Heatmap | Heatmap | `http_request_duration_seconds` histogram | P95 >5s for 5min |
| MC-4 | DB Connection Pool Utilization | Gauge | `sql.DB.Stats()` InUse/MaxOpenConnections | >80% for 2min |
| MC-5 | Circuit Breaker State | Status | `circuit_breaker_state` gauge | Any open for >30s |

### Ingest Pipeline Dashboard (4 charts)

| ID | Chart | Type | Source | Alert |
|----|-------|------|--------|-------|
| MC-6 | Queue Depth by Signal Type | Multi-line | `ingest_queue_depth{signal=spans\|logs\|metrics}` | >80% capacity for 2min |
| MC-7 | Flush Latency P95 | Histogram | `ingest_flush_duration_seconds` | P95 >10s |
| MC-8 | Rows Ingested/sec by Type | Stacked area | `ingest_rows_flushed_total` rate | Drops to 0 for >5min |
| MC-9 | Backpressure Events/min | Bar | `ingest_backpressure_total` | >10/min |

### Business / Product Dashboard (3 charts)

| ID | Chart | Type | Purpose |
|----|-------|------|---------|
| MC-10 | Active Teams by Usage Tier | Table | Understand adoption + resource consumption |
| MC-11 | Feature Usage Frequency | Bar | Which features are actually used |
| MC-12 | Error Impact — Unique Users Affected | Stat | User-facing error severity |

### Dependency Health Dashboard (4 charts)

| ID | Chart | Type | Alert |
|----|-------|------|-------|
| MC-13 | ClickHouse Query Duration P95 | Histogram | P95 >10s |
| MC-14 | MySQL Query Duration P95 | Histogram | P95 >2s |
| MC-15 | Slack Webhook Success Rate | Gauge | <90% for 10min |
| MC-16 | Redis Availability | Status | Unavailable for >30s |

---

## DIMENSION 4: FRONTEND ↔ BACKEND INTERCONNECTION (8 findings)

### API Contract Issues (3)

| ID | Severity | Issue | File | Fix |
|----|----------|-------|------|-----|
| IC-1 | MEDIUM | No X-Request-Id on responses | `internal/platform/middleware/middleware.go` | Generate UUID, set header, store in context |
| IC-2 | MEDIUM | Error responses missing requestId | `internal/contracts/response.go` | Add `RequestID` field to `ErrorDetail` |
| IC-3 | LOW | Inconsistent pagination shapes | Spans vs logs handlers | Standardize: `{data, pagination: {hasMore, nextCursor, total}}` |

### Data Fetching Issues (3)

| ID | Severity | Issue | File | Fix |
|----|----------|-------|------|-----|
| IC-4 | HIGH | Token in localStorage (XSS) | `optic-frontend/src/shared/api/auth/authStorage.ts` | Use httpOnly cookies + `withCredentials: true` |
| IC-5 | MEDIUM | No mutation retry | `optic-frontend/src/shared/api/api/interceptors/retryInterceptor.ts` | Add idempotency key, retry idempotent POSTs |
| IC-6 | MEDIUM | Fire-and-forget cache invalidation | `optic-frontend/src/shared/store/appStore.ts` | `await invalidateQueryClientCache()` before nav |

### Security at Boundary (2)

| ID | Severity | Issue | File | Fix |
|----|----------|-------|------|-----|
| IC-7 | HIGH | Auth failures not logged | `internal/platform/middleware/middleware.go` | Log on each rejection with code + IP |
| IC-8 | MEDIUM | Health check missing Redis | `internal/platform/server/app.go` | Add Redis ping to `/health/ready` |
| IC-9 | LOW | console.error in production | `optic-frontend/src/shared/components/ui/feedback/ErrorBoundary.tsx` | Dev-only or send to error tracking |
| IC-10 | LOW | Test file with console.log in repo | `optic-frontend/src/test_zod_schema.ts` | Remove before prod |

---

## Summary by Priority

### By Severity
- **CRITICAL**: 4 findings (SB-1, SB-2, SB-11, LG-1, MG-1, TG-1) → Week 1
- **HIGH**: 16 findings (scale bugs, logging, metrics) → Week 2-3
- **MEDIUM**: 28 findings (optimizations, UX, API) → Month 2
- **LOW**: 14 findings (hygiene, console cleanup) → Ongoing

### By Dimension
- **Scale Bugs**: 15 (4 CRITICAL, 6 HIGH, 5 MEDIUM)
- **Observability Gaps**: 31 (3 CRITICAL, 10 HIGH, 18 MEDIUM)
- **Missing Dashboards**: 16 (0 CRITICAL, 0 HIGH, 16 MEDIUM)
- **Frontend-Backend**: 8 (0 CRITICAL, 6 HIGH, 2 MEDIUM)

### By Layer
- **Backend Scale**: 10 findings
- **Backend Observability**: 24 findings
- **Frontend Scale**: 3 findings
- **Frontend/Backend**: 8 findings
- **Dashboards**: 16 findings (requires metrics first)

---

## Implementation Timeline

| Phase | Items | Effort | Status |
|-------|-------|--------|--------|
| **Week 1** | 10 CRITICAL | 30h | ✅ Complete |
| **Week 2-3** | 8 HIGH | 40h | 📋 Ready |
| **Month 2** | 16 MEDIUM | 40h | 📋 Planned |
| **Ongoing** | 14 LOW | TBD | 📋 Backlog |

See `WEEK_1_COMPLETE.md` for what's done and `ROADMAP.md` for detailed Week 2-3 plan.
