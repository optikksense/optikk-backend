# Optik backend — codebase index

Orientation for **optikk-backend** (Go modular monolith). Read this file and `.cursor/rules/optik-backend.mdc` before substantive work in this repository.

## How assistants should use this document

- **Before** any substantive task: read **`CODEBASE_INDEX.md`** (this file) and **`.cursor/rules/optik-backend.mdc`**. Follow **`.cursor/rules/engineering-workflow.mdc`** for planning and quality bar.
- **Plan before code:** Produce a plan (with options where appropriate) and **do not change code until the user approves** the plan, except for trivial one-line/typo fixes.
- **After** module or contract changes (new HTTP domains, dashboard JSON, ingestion boundaries): **update this file** and **`.cursor/rules/optik-backend.mdc`** in the same change when something durable changed.

## Related repository

The web app lives in the sibling repo **`optic-frontend`** (see that repo’s `CODEBASE_INDEX.md`).

**Hybrid model:** backend-authored dashboards (JSON + default config), frontend-owned explorer routes and feature modules, shared dashboard panel registry and API decode boundary.

---

## Stack and entry

- **Stack:** Go 1.25, Gin, ClickHouse, MySQL, Redis, native WebSocket live tail (`/api/v1/ws/live`), OTLP ingestion.
- **Module:** `github.com/Optikk-Org/optikk-backend`
- **Server entry:** `cmd/server/main.go`

## Composition (where modules are wired)

| File | Purpose |
|------|---------|
| `internal/app/server/modules_manifest.go` | **`configuredModules()`** — single list of all `registry.Module` constructors; add new HTTP/domain modules here |
| `internal/app/server/app.go` | App wiring |
| `internal/app/registry/registry.go` | Shared dependencies (querier, DB, tenant, session, config) |

## Module packages (`internal/modules/`)

Top-level domains (each may contain `handler.go`, `service.go`, `repository.go`, `module.go`, `dto.go`, `models.go`):

| Domain | Packages (representative) |
|--------|---------------------------|
| **Dashboard / default config** | `dashboard` — default-config API and tenant config. Overview page has 6 tabs: summary (includes RED Metrics section), errors, http, latency-analysis, slo, apm |
| **User** | `user/auth`, `user/user`, `user/team` |
| **Logs** | `logs/explorer`, `logs/search`, `logs/detail`, `logs/tracelogs`; explorer analytics lives under `explorer/analytics` |
| **Traces** | `traces/query`, `traces/explorer`, `traces/tracedetail`, `traces/livetail`, `traces/analytics`, `traces/redmetrics`, `traces/errorfingerprint`, `traces/errortracking`, `traces/tracecompare` |
| **Metrics Explorer** | `metricsexplorer` — custom metric queries, tag exploration |
| **Services** | `services/service`, `services/servicemap`, `services/topology` |
| **Overview** | `overview/overview`, `overview/errors`, `overview/slo` |
| **Deployments** | `deployments` — version / deployment impact from spans (`service.version`, `deployment.environment`); `GET /api/v1/deployments/*` (cached) |
| **Database metrics** | `database/collection`, `connections`, `errors`, `latency`, `slowqueries`, `summary`, `system`, `systems`, `volume` |
| **Infrastructure** | `infrastructure/cpu`, `disk`, `jvm`, `kubernetes`, `memory`, `network`, `nodes`, `resourceutil` |
| **Saturation** | `saturation/kafka`, `saturation/redis`, `saturation/database/*` (comprehensive DB health) |
| **AI** | `ai/dashboard`, `ai/runs`, `ai/rundetail`, `ai/traces`, `ai/conversations` |
| **Other** | `apm`, `httpmetrics`, `explorer/analytics`, `explorer/queryparser` |

## Ingestion

- **OTLP Pipeline**: `internal/ingestion/otlp/` — gRPC export maps to `ingest.Row`, then **`XADD`** to Redis Streams (`otlp:ingest:logs|metrics|spans`, field `payload` JSON). **Redis is required** when OTLP is used (publish path).
- **Authentication**: `internal/ingestion/otlp/auth/` — TTL-cached team resolution via API keys.
- **Row types**: `internal/ingestion/otlp/internal/ingest/` — `ingest.Row`, byte tracking, team rate limits.
- **Background consumers**: `internal/ingestion/otlp/streamworkers/` — `BackgroundRunner` with separate consumer groups: **ClickHouse** writers (`ch-logs` / `ch-metrics` / `ch-spans`) and **live-tail bridges** (`livetail-logs` / `livetail-spans` on ingest logs/spans streams) fanning out to `livetail:logs:stream:{teamId}` and `livetail:spans:stream:{teamId}` (field `data`). WebSocket clients `XREVRANGE` snapshot then `XREAD` block.
- **Encoding**: `internal/ingestion/otlp/rowjson/` — marshal/unmarshal rows for stream payloads.

## Internal Infrastructure (`internal/infra/`)

| Package | Purpose |
|---------|---------|
| `circuitbreaker` | Resilience patterns for external/DB calls (`breaker.go`) |
| `timebucket` | Windowed time utilities for rate limiting and aggregation |
| `validation` | Schema-based validation logic |
| `cache` | Query and object caching |
| `database` | **`NativeQuerier`** and ClickHouse/MySQL connection management |
| `livetailws` | Live tail WebSocket (`GET /api/v1/ws/live`); hub fan-out is filtered per client: `subscribe:logs` sends only OTLP `WireLog`, `subscribe:spans` only `WireSpan` (avoids span-shaped payloads in the Logs UI) |
| `livetailredis` | Redis Stream keys for live tail (`livetail:logs:stream:{teamId}`, `livetail:spans:stream:{teamId}`; field `data`) |
| `otlpredis` | Ingest stream names and consumer group ids; `EnsureIngestStreams` (`MKSTREAM` + `XGROUP CREATE`) |

## Module Anatomy (LLD)

Every feature module follows a 6-file pattern under `internal/modules/<domain>/`:

| File | Purpose |
|------|---------|
| `handler.go` | HTTP handlers — param parsing, response writing |
| `service.go` | Interface + concrete impl — business logic, orchestration |
| `repository.go` | Interface + concrete impl — raw ClickHouse/MySQL queries |
| `module.go` | `NewModule()` constructor, `RegisterRoutes()`, wired into `modules_manifest.go` |
| `dto.go` | Request/response DTOs (JSON tags) |
| `models.go` | Domain models, constants |

**Canonical handler method:**

```go
func (h *Handler) ListItems(c *gin.Context) {
    teamID := h.GetTenant(c).TeamID
    startMs, endMs, ok := httputil.ParseRequiredRange(c) // writes 400 if missing
    if !ok { return }

    items, err := h.Service.ListItems(c.Request.Context(), teamID, startMs, endMs)
    if err != nil {
        httputil.RespondErrorWithCause(c, http.StatusInternalServerError,
            errorcode.Internal, "failed to list items", err)
        return
    }
    httputil.RespondOK(c, items)
}
```

**Constructor chain:** `NewModule(nativeQuerier, getTenant)` → `Handler{GetTenant, Service: NewService(NewRepository(nativeQuerier))}` → register in `modules_manifest.go:configuredModules()`

## Request Lifecycle (LLD)

```
Gin request
  → gin.Default (Logger + Recovery)
  → middleware.ErrorRecovery (panic → contracts.Failure)
  → middleware.CORSMiddleware (origin allowlist)
  → /api/v1 group
    → middleware.TenantMiddleware (session auth + team resolution)
    → RateLimiter (2000 req/s; Redis-backed or in-memory)
    → [cache.CacheResponse 30s for registry.Cached modules]
    → Handler → Service → Repository → ClickHouse/MySQL
  → contracts.APIResponse envelope
```

## ClickHouse Query Helpers (`internal/infra/database/`)

| Helper | Signature | Purpose |
|--------|-----------|---------|
| `QueryMaps` | `(querier, sql, args...) → []map[string]any` | Execute query, 10K row limit |
| `QueryMapsLimit` | `(querier, limit, sql, args...) → []map[string]any` | Custom row limit |
| `SqlTime` | `(ms int64) → time.Time` | Unix ms to UTC for ClickHouse params |
| `Int64FromAny` | `(v any) → int64` | Type-safe row extraction |
| `Float64FromAny` | `(v any) → float64` | Type-safe row extraction |
| `StringFromAny` | `(v any) → string` | Type-safe row extraction |
| `NormalizeRows` | `(rows) → rows` | Sanitize NaN/Inf to 0 for JSON |
| `SanitizeError` | `(err) → err` | Strip internal details for client |

**Time bucketing** (`internal/infra/timebucket/`): `timebucket.NewAdaptiveStrategy(startMs, endMs)` auto-picks minute/5min/hour/day; use `.GetBucketExpression()` in SELECT and GROUP BY. Named params: `clickhouse.Named("teamID", teamID)` with `@teamID` in SQL.

**Spans table materialized attributes** (`observability.spans`, see `db/clickhouse_local.sql`): includes `mat_service_version` ← `attributes.\`service.version\``, `mat_deployment_environment` ← `attributes.\`deployment.environment\`` (bloom indexes), for deployment queries without JSON scans.

## API Response Envelope

All endpoints return `contracts.APIResponse`:

```go
type APIResponse struct {
    Success bool        `json:"success"`
    Data    any         `json:"data,omitempty"`
    Error   *ErrorDetail `json:"error,omitempty"`
}
```

- `contracts.Success(data)` / `contracts.Failure(code, msg, path)`
- Error codes in `internal/shared/contracts/errorcode/`: `BadRequest`, `Validation`, `Unauthorized`, `Forbidden`, `NotFound`, `Internal`, `QueryFailed`, `QueryTimeout`, `CircuitOpen`, etc.
- Comparison support: `httputil.WithComparison(c, startMs, endMs, queryFn)` wraps primary + optional comparison range

## Config Structure (`internal/config/config.go`)

`config.yml` → `Config` struct:

| Section | Struct | Key fields |
|---------|--------|------------|
| `server` | `ServerConfig` | `port`, `allowed_origins`, `debug_api_logs` |
| `mysql` | `MySQLConfig` | `host`, `port`, `database`, `user`, `password`, `max_open_conns` |
| `clickhouse` | `ClickHouseConfig` | `host`, `port`, `database`, `user`, `password`, `production`, `cloud_host` |
| `session` | `SessionConfig` | `lifetime_ms`, `idle_timeout_ms`, `cookie_*` |
| `redis` | `RedisConfig` | `enabled`, `host`, `port`, optional `password`, `db` (shared by sessions/scs, go-redis cache, OTLP helpers) |
| `otl_redis_stream` | `OtlRedisStream` | `max_len_approx`, `ch_batch_size`, `ch_flush_interval_ms`, `xread_block_ms`, `xread_count` |
| `otlp` | `OTLPConfig` | `grpc_port`, `grpc_max_recv_msg_size_mb` |
| `retention` | `RetentionConfig` | `default_days` |
| `app` | `AppConfig` | `region`, `dashboard_config_use_defaults` |
| `circuit_breaker` | `CircuitBreakerConfig` | `consecutive_failures`, `reset_timeout_ms` |

## Dashboard JSON (backend-authored pages)

| Path | Purpose |
|------|---------|
| `internal/infra/dashboardcfg/` | Loader, models, panel layout; JSON pages under `internal/infra/dashboardcfg/pages/` |
| `internal/modules/dashboard/` | HTTP/service for default config |
| `internal/infra/dashboardcfg/defaults/service/` | Default **Service** page (`/service`, tab deployments) — requires `?serviceName=` on the client |

**Schema:** `page.schemaVersion` is **2** in embedded defaults (`CurrentSchemaVersion` in `internal/infra/dashboardcfg/models.go`). **1** remains accepted for older stored configs. **v2** adds explicit **`layout.w` and `layout.h`** (grid units, 12-column model); values must match the canonical footprint for `layoutVariant` (`panel_size_policy.go`). If `w`/`h` are omitted (legacy JSON), the loader hydrates them once from `layoutVariant` before validation. The **optic-frontend** reads `panel.layout.w` / `panel.layout.h` for `react-grid-layout`; pixel spacing stays frontend-only (`panelSizePolicy.ts`).

## Extension Interfaces

**`internal/app/registry/registry.go`** defines the core extension points:
- **`Module`**: Standard HTTP/domain module.
- **`GRPCRegistrar`**: For modules exposing gRPC services.
- **`BackgroundRunner`**: For modules with lifecycle-managed workers.
---

## Backend ↔ frontend map (cross-repo)

Use when a change spans API and UI. Frontend paths refer to **`optic-frontend`**.

| Product area | This repo | Frontend (`optic-frontend`) |
|--------------|-----------|----------------------------|
| Registry / route wiring | `modules_manifest.go` | `domainRegistry.ts`, feature `index.ts` |
| Explorer APIs | `internal/modules/.../handler.go` | Feature `api/` or `shared/api` |
| Metrics Explorer | `internal/modules/metricsexplorer` | `src/features/metrics` |
| Dashboard panels | `internal/infra/dashboardcfg/pages/`, panel types | `dashboard/renderers/`, `dashboardPanelRegistry` |
| Auth | `internal/modules/user/auth/` | `shared/api/auth/` |
| Default config | `internal/modules/dashboard/`, `internal/infra/dashboardcfg/` | `defaultConfigService.ts` |
| Live tail (logs/traces) | `internal/infra/livetailws/`, `logs/search/livetail_*.go`, `traces/livetail/livetail_*.go` | `useSocketStream.ts`, `useLiveTailStream.ts` |

---

## Maintenance

When you add a **new feature domain**: new package under `internal/modules/...`, implement `registry.Module`, register in `modules_manifest.go`. Update this index when module lists or contracts change.
