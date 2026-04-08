# Optik backend — codebase index

Orientation for **optikk-backend** (Go modular monolith). Read this file and `.cursor/rules/optik-backend.mdc` before substantive work in this repository.

## How assistants should use this document

- **Before** any substantive task: read **`CODEBASE_INDEX.md`** (this file), **`.cursor/rules/optik-backend.mdc`**, and **`.agent/philosophy/`** for strategic alignment. Follow **`.cursor/rules/engineering-workflow.mdc`** for planning and quality bar.
- **Plan before code:** Produce a plan (with options where appropriate) and **do not change code until the user approves** the plan, except for trivial one-line/typo fixes.
- **Agent Philosophy**: Mandatory reading for staff-level alignment:
  - **ADR-001**: [adr-001-strict-architecture.md](file:///Users/ramantayal/pro/optikk-backend/.agent/philosophy/adr-001-strict-architecture.md)
  - **Vision**: [vision-and-extensibility.md](file:///Users/ramantayal/pro/optikk-backend/.agent/philosophy/vision-and-extensibility.md)
  - **Architecture**: [system-architecture.md](file:///Users/ramantayal/pro/optikk-backend/.agent/philosophy/system-architecture.md)

## Related repository

The web app lives in the sibling repo **`optic-frontend`** (see that repo's `CODEBASE_INDEX.md`).

**Hybrid model:** backend-authored dashboards (JSON + default config), frontend-owned explorer routes and feature modules, shared dashboard panel registry and API decode boundary.

---

## Stack and entry

- **Stack:** Go 1.25, Gin, ClickHouse, MySQL, Redis, native WebSocket live tail (`/api/v1/ws/live`), OTLP ingestion.
- **Module:** `github.com/Optikk-Org/optikk-backend`
- **Server entry:** `cmd/server/main.go`

## Composition (where modules are wired)

| File | Purpose |
|------|---------|
| `internal/app/server/modules_manifest.go` | **`configuredModules()`** — single list of all `registry.Module` constructors (51 total: 47 HTTP + 4 ingestion); add new HTTP/domain modules here |
| `internal/app/server/app.go` | App wiring; builds `platform/runtime.Runtime`, native querier, WebSocket handler, module graph |
| `internal/app/registry/registry.go` | Shared dependency aliases for modules (querier, DB, tenant, config, platform session contract) |

## Runtime ownership

- `internal/platform/` owns cross-cutting capability contracts and provider selection.
- `internal/platform/runtime/` builds the runtime bundle used by the app layer.
- `internal/infra/` owns concrete low-level implementations behind those platform contracts.
- `internal/modules/` and `internal/app/` should not import provider implementations like `internal/infra/session`, `internal/infra/livetail`, `internal/infra/ingestion`, or the old `internal/infra/dashboardcfg`.

## Module packages (`internal/modules/`)

51 registered modules across 13 domains. Every module **must** follow the strict 6-file pattern: `handler.go`, `service.go`, `repository.go`, `module.go`, `dto.go`, `models.go`. All repository implementation methods must reside in the single `repository.go` file.

| Domain | Packages | Route prefix | Cache |
|--------|----------|-------------|-------|
| **AI** (5) | `ai/dashboard`, `ai/runs`, `ai/rundetail`, `ai/conversations`, `ai/traces` | `/ai/*` | V1 |
| **APM** (1) | `apm` | `/apm/*` | Cached |
| **Dashboard config** (1) | `dashboard` | `/default-config/*` | V1 |
| **Deployments** (1) | `deployments` | `/deployments/*` | Cached |
| **Explorer** (1) | `explorer/analytics` (helper: `explorer/queryparser`) | `POST /explorer/:scope/analytics` | V1 |
| **HTTP Metrics** (1) | `httpmetrics` | `/http/*`, `/http/routes/*`, `/http/external/*` | Cached |
| **Infrastructure** (8) | `infrastructure/{cpu,disk,jvm,kubernetes,memory,network,nodes,resourceutil}` (consts: `infraconsts`) | `/infrastructure/*` | Cached |
| **Logs** (2) | `logs/explorer`, `logs/search` (shared: `logs/internal/shared`) | `/logs/*`, `POST /logs/explorer/query` | V1 |
| **Metrics Explorer** (1) | `metricsexplorer` | `/metrics/names`, `/metrics/:metricName/tags`, `POST /metrics/explorer/query` | V1 |
| **Overview** (3) | `overview/overview`, `overview/errors`, `overview/slo` | `/overview/*`, `/errors/groups/*` | Cached |
| **Saturation** (10) | `saturation/database/{collection,connections,errors,latency,slowqueries,summary,system,systems,volume}`, `saturation/kafka` | `/saturation/*` | V1 (db), Cached (kafka/summary) |
| **Traces** (8) | `traces/{query,explorer,tracedetail,redmetrics,errorfingerprint,errortracking,tracecompare,livetail}` (shared: `traces/shared`) | `/traces/*`, `/spans/*`, `/services/*`, `/latency/*`, `/errors/*` | Mixed |
| **User** (3) | `user/auth`, `user/team`, `user/user` (shared: `user/internal`) | `/auth/*`, `/users/*`, `/teams/*`, `/settings/*` | V1 |
| **Ingestion** (4) | via `internal/ingestion/otlp/{streamworkers,spans,logs,metrics}` | gRPC only (no HTTP routes) | — |

### AI module routes

| Submodule | Key endpoints |
|-----------|--------------|
| `ai/dashboard` | `GET /ai/summary`, `/ai/models`, `/ai/performance/{summary,timeseries,latency-histogram}`, `/ai/cost/{summary,timeseries,token-breakdown}`, `/ai/security/{summary,timeseries,pii-categories}` |
| `ai/runs` | `GET /ai/runs`, `/ai/runs/summary`, `/ai/runs/models`, `/ai/runs/operations` |
| `ai/rundetail` | `GET /ai/runs/:spanId`, `/ai/runs/:spanId/messages`, `/ai/runs/:spanId/context` |
| `ai/conversations` | `GET /ai/conversations`, `/ai/conversations/:conversationId` |
| `ai/traces` | `GET /ai/traces/:traceId`, `/ai/traces/:traceId/summary` |

### Overview module routes

| Submodule | Key endpoints |
|-----------|--------------|
| `overview/overview` | `GET /overview/request-rate`, `/overview/error-rate`, `/overview/p95-latency`, `/overview/services`, `/overview/top-endpoints`, `/overview/endpoints/metrics` (alias), `/overview/endpoints/timeseries`, `/overview/summary` |
| `overview/errors` | `GET /overview/errors/{service-error-rate,error-volume,latency-during-error-windows,groups}`, `/errors/groups/:groupId/*` |
| `overview/slo` | `GET /overview/slo`, `/overview/slo/stats`, `/overview/slo/burn-down`, `/overview/slo/burn-rate` |

### Traces module routes

| Submodule | Key endpoints |
|-----------|--------------|
| `traces/query` | `GET /traces`, `/traces/:traceId/spans`, `/spans/:spanId/tree`, `/spans/search`, `/services/:serviceName/errors/*`, `/latency/*`, `/errors/*` |
| `services/topology` | `GET /services/topology` — runtime service map (nodes + directed RED-weighted edges from parent→child span joins); optional `?service=<name>` for 1-hop neighborhood. Cached (30s). |
| `traces/explorer` | `POST /traces/explorer/query` |
| `traces/tracedetail` | `GET /traces/:traceId/{span-events,span-kind-breakdown,critical-path,span-self-times,error-path,flamegraph,logs,related}`, `/traces/:traceId/spans/:spanId/attributes` |
| `traces/redmetrics` | `GET /spans/red/{summary,apdex,top-slow-operations,top-error-operations,request-rate,error-rate,p95-latency,span-kind-breakdown,errors-by-route}`, `/spans/latency-breakdown` |
| `traces/errorfingerprint` | `GET /errors/fingerprints`, `/errors/fingerprints/trend` |
| `traces/errortracking` | `GET /spans/exception-rate-by-type`, `/spans/error-hotspot`, `/spans/http-5xx-by-route` |
| `traces/tracecompare` | `GET /traces/compare` |
| `traces/livetail` | WebSocket-only via `livetail.Hub` (no HTTP routes) |

### Infrastructure module routes

All under `/infrastructure/` prefix, all Cached:

| Submodule | Key endpoints |
|-----------|--------------|
| `cpu` | `cpu/{time,usage-percentage,load-average,process-count}` |
| `disk` | `disk/{io,operations,io-time,filesystem-usage,filesystem-utilization}` |
| `jvm` | `jvm/{memory,gc-duration,gc-collections,threads,classes,cpu,buffers}` |
| `kubernetes` | `kubernetes/{container-cpu,cpu-throttling,container-memory,oom-kills,pod-restarts,node-allocatable,pod-phases,replica-status,volume-usage}` |
| `memory` | `memory/{usage,usage-percentage,swap}` |
| `network` | `network/{io,packets,errors,dropped,connections}` |
| `nodes` | `nodes`, `nodes/summary`, `nodes/:host/services` |
| `resourceutil` | `resource-utilisation/{avg-cpu,avg-memory,avg-network,avg-conn-pool,cpu-usage-percentage,memory-usage-percentage,by-service,by-instance}` |

### Saturation module routes

Database submodules register under `/saturation/` prefix:

| Submodule | Key endpoints |
|-----------|--------------|
| `collection` | `collection/{latency,ops,errors,query-texts,read-vs-write}` |
| `connections` | `connections/{count,utilization,limits,pending,timeout-rate,wait-time}` |
| `errors` | `errors/{by-system,by-operation,by-error-type,by-collection,by-status,ratio}` |
| `latency` | `latency/{by-system,by-operation,by-collection,by-namespace,by-server,heatmap}` |
| `slowqueries` | `slow-queries/{patterns,collections,rate,p99-by-text}` |
| `summary` | `summary` (Cached) |
| `system` | `system/{latency,ops,top-collections-by-latency,top-collections-by-volume,errors,namespaces}` |
| `systems` | `systems` |
| `volume` | `ops/{by-system,by-operation,by-collection,read-vs-write,by-namespace}` |
| `kafka` | `kafka/{summary-stats,produce-rate-by-topic,publish-latency-by-topic,consume-rate-by-topic,...}` (Cached) |

### HTTP Metrics module routes

All under `/http/` prefix, Cached:
- Root: `request-rate`, `request-duration`, `active-requests`, `body-sizes`, `dns-duration`, `tls-duration`, `status-distribution`, `error-timeseries`
- `/http/routes/`: `top-by-volume`, `top-by-latency`, `error-rate`, `error-timeseries`
- `/http/external/`: `top-hosts`, `host-latency`, `error-rate`

### Logs live tail

- **`internal/infra/livetailws/handler.go`** — WebSocket entrypoint; depends on platform session + live-tail hub contracts
- **`internal/modules/logs/search/livetail_payload.go`** — `SubscribeLogsPayload`: filter fields: Severities, Services, Hosts, Pods, Containers, Environments, TraceID, SpanID, Search, SearchMode, ExcludeSeverities, ExcludeServices, ExcludeHosts, AttributeFilters
- WebSocket protocol: client sends `subscribe:logs` op to `/api/v1/ws/live`, backend receives events from the runtime live-tail hub fed by OTLP stream workers

## Ingestion

- **OTLP Pipeline**: `internal/ingestion/otlp/` — gRPC export explicitly mapped to concrete structs (`LogRow`, `SpanRow`, `MetricRow`).
- **Authentication**: `internal/ingestion/otlp/auth/` — TTL-cached team resolution via API keys.
- **Dispatch contracts**: `internal/platform/ingestion/` — `Dispatcher[T]`, `TelemetryBatch[T]`, OTLP dependency interfaces.
- **Default dispatcher implementation**: `internal/infra/ingestion/dispatcher.go` — in-memory channel fanout used by the runtime bundle.
- **Background consumers**: `internal/ingestion/otlp/streamworkers/` — `BackgroundRunner` with separate routines per type. **ClickHouse** writers use `CHFlusher[T]` with `AppendStruct(row)`. Live tail components tap into these same pipelines and broadcast to WebSocket clients.

## Platform Layer (`internal/platform/`)

| Package | Purpose |
|---------|---------|
| `runtime` | Builds the runtime dependency bundle used by `server.New` |
| `session` | Session/auth contract used by app, middleware, WebSocket auth, and auth module |
| `ratelimit` | Rate limiter contract and provider-facing API |
| `livetail` | Live-tail hub contract |
| `ingestion` | OTLP dispatcher + dependency contracts |
| `dashboardcfg` | Dashboard schema, validation, hydration, registry, and embedded defaults |

## Internal Infrastructure (`internal/infra/`)

| Package | Purpose |
|---------|---------|
| `circuitbreaker` | Resilience patterns for external/DB calls (`breaker.go`) — wraps `sony/gobreaker` |
| `timebucket` | Adaptive time bucketing for ClickHouse aggregations (minute/5min/hour/day) |
| `validation` | Schema-based validation logic |
| `cache` | Query and object caching |
| `database` | **`NativeQuerier`** and ClickHouse/MySQL connection management |
| `ingestion` | Default in-memory dispatcher implementation backing `platform/ingestion.Dispatcher[T]` |
| `livetail` | Default live-tail hub implementation behind `platform/livetail.Hub` |
| `livetailws` | Live tail WebSocket handler (`GET /api/v1/ws/live`) wired against platform hub + session contracts |
| `livetailredis` | Redis Stream keys for live tail (`livetail:logs:stream:{teamId}`, `livetail:spans:stream:{teamId}`; field `data`) |
| `otlpredis` | Ingest stream names and consumer group ids; `EnsureIngestStreams` (`MKSTREAM` + `XGROUP CREATE`) |
| `middleware` | HTTP middleware: CORS, error recovery, tenant context, rate limiting middleware |
| `session` | Default `scs/v2` session manager implementation; keys: `auth_user_id`, `auth_email`, `auth_role`, `auth_default_team_id`, `auth_team_ids` |
| `utils` | String conversion and time parsing helpers |

## Module Anatomy (LLD)

Every feature module follows a strict 6-file pattern under `internal/modules/<domain>/`:

| File | Purpose |
|------|---------|
| `handler.go` | HTTP handlers — param parsing, response writing |
| `service.go` | Interface + concrete impl — business logic, orchestration |
| `repository.go` | Interface + concrete impl — raw ClickHouse/MySQL queries (all methods here) |
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
    → middleware.TenantMiddleware (platform session contract + team resolution)
    → RateLimiter (platform-injected, local provider by default)
    → [cache.CacheResponse 30s for registry.Cached modules]
    → Handler → Service → Repository → ClickHouse/MySQL
  → contracts.APIResponse envelope
```

**Public prefixes** (skip TenantMiddleware): `/api/v1/auth/login`, `/otlp/`, `/health`

**Session keys** (`internal/infra/session/manager.go`): `auth_user_id`, `auth_email`, `auth_role`, `auth_default_team_id`, `auth_team_ids`

## ClickHouse Query Helpers (`internal/infra/database/`)

### Query execution (`query.go`)

| Helper | Signature | Purpose |
|--------|-----------|---------|
| `QueryMaps` | `(querier, sql, args...) → []map[string]any` | Execute query, 10K row limit |
| `QueryMapsLimit` | `(querier, limit, sql, args...) → []map[string]any` | Custom row limit |
| `QueryMap` | `(querier, sql, args...) → map[string]any` | Single row result |
| `QueryCount` | `(querier, sql, args...) → int64` | Execute COUNT query |
| `InClause` | `([]string) → (clause, args)` | Positional `(?,?,?)` for strings |
| `InClauseInt64` | `([]int64) → (clause, args)` | Positional `(?,?,?)` for int64 |
| `NamedInClause` | `(prefix, []string) → (clause, map)` | Named `(@p0,@p1)` for strings |
| `NamedInClauseInt64` | `(prefix, []int64) → (clause, map)` | Named `(@p0,@p1)` for int64 |
| `JSONString` | `(any) → string` | Marshal to JSON with `{}` fallback |
| `MustAtoi64` | `(string, fallback) → int64` | Parse int64 with fallback |
| `RowsAffected` | `(sql.Result) → int64` | Safe extraction |

### Type-safe extraction (`utils.go`)

| Helper | Purpose |
|--------|---------|
| `SqlTime(ms)` | Unix ms → `time.Time` UTC for ClickHouse params |
| `Int64FromAny(v)` | Handles int64, int32, uint64, float64, []byte, string |
| `Float64FromAny(v)` | Handles float variants, sanitizes NaN/Inf → 0 |
| `StringFromAny(v)` | Handles string, *string, []byte, nil |
| `BoolFromAny(v)` | Handles bool, int, float, []byte, string |
| `TimeFromAny(v)` | Handles time.Time, unix s/ms/ns, RFC3339, multiple layouts |
| `NullableString(v)` | Empty/whitespace → nil, else string |
| `NullableStringFromAny(v)` | → `*string` (nil if empty) |
| `NullableFloat64FromAny(v)` | → `*float64` (nil-safe) |
| `NullableTimeFromAny(v)` | → `*time.Time` (nil if empty) |
| `ToInt64Slice(v)` | `[]any` → `[]int64` |
| `NormalizeRows(rows)` | Batch sanitize NaN/Inf across result set |
| `NormalizeMap(m)` | Single-map sanitization |
| `DefaultString(v, fallback)` | Return fallback if empty |

**Time bucketing** (`internal/infra/timebucket/`): `timebucket.NewAdaptiveStrategy(startMs, endMs)` auto-picks minute/5min/15min/hour/day; use `.GetBucketExpression()` in SELECT and GROUP BY. `timebucket.ByName("15m")` for explicit step selection. Named params: `clickhouse.Named("teamID", teamID)` with `@teamID` in SQL.

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
- Error codes in `internal/shared/contracts/errorcode/`: `BadRequest`, `Validation`, `Unauthorized`, `Forbidden`, `NotFound`, `Internal`, `QueryFailed`, `QueryTimeout`, `ConnectionError`, `RateLimited`, `Unavailable`, `CircuitOpen`
- Comparison support: `httputil.WithComparison(c, startMs, endMs, queryFn)` wraps primary + optional comparison range

## Dashboard JSON (backend-authored pages)

| Path | Purpose |
|------|---------|
| `internal/platform/dashboardcfg/` | Loader, models, panel layout, validation, hydration |
| `internal/modules/dashboard/` | HTTP/service for default config API |

**Schema:** `page.schemaVersion` is **2** in embedded defaults (`CurrentSchemaVersion` in `internal/platform/dashboardcfg/types.go`). **1** remains accepted for older stored configs. **v2** adds explicit **`layout.w` and `layout.h`** (grid units, 12-column model); values must match the canonical footprint for `layoutVariant` (`panel_size_policy.go`). If `w`/`h` are omitted (legacy JSON), the loader hydrates them once from `layoutVariant` before validation. The **optic-frontend** reads `panel.layout.w` / `panel.layout.h` for `react-grid-layout`; pixel spacing stays frontend-only.

### Default pages (`internal/platform/dashboardcfg/defaults/`)

The `service` page at `/service` is **fully frontend-owned** (Discovery + Topology tabs in optic-frontend) — no backend default config. Service detail is frontend-owned as a side drawer; the backend contributes the shared drawer entity contract and the overview services table `drawerAction` that opens that drawer from backend-driven rows.

| Page ID | Directory | Tabs | Default tab | Group | Order |
|---------|-----------|------|-------------|-------|-------|
| `overview` | `defaults/overview/` | summary, latency-analysis, apm, errors, http, slo | summary | observe | 10 |
| `ai-observability` | `defaults/ai_observability/` | overview, performance, cost, security | overview | operate | 80 |
| `infrastructure` | `defaults/infrastructure/` | resource-utilization, jvm, kubernetes, pods | resource-utilization | operate | 60 |
| `saturation` | `defaults/saturation/` | database, queue, redis | database | operate | 70 |

### Dashboard schema enums (`internal/platform/dashboardcfg/enums.go`)

**Panel types (24):** `ai-bar`, `ai-line`, `bar`, `db-systems-overview`, `error-hotspot-ranking`, `error-rate`, `exception-type-line`, `gauge`, `heatmap`, `latency`, `latency-heatmap`, `latency-histogram`, `log-histogram`, `pie`, `request`, `service-catalog`, `service-health-grid`, `service-map`, `slo-indicators`, `stat-card`, `stat-cards-grid`, `stat-summary`, `table`, `trace-waterfall`

**Layout variants (10):** `kpi`, `summary`, `standard-chart`, `wide-chart`, `ranking`, `summary-table`, `detail-table`, `hero`, `hero-map`, `hero-detail`

**Section templates (8):** `kpi-band`, `summary-plus-health`, `two-up`, `three-up`, `stacked`, `hero-plus-table`, `chart-grid-plus-details`, `table-stack`

**Drawer entities (7):** `aiModel`, `databaseSystem`, `errorGroup`, `kafkaGroup`, `kafkaTopic`, `node`, `redisInstance`

**Formatters (6):** `ms`, `ns`, `bytes`, `percent1`, `percent2`, `number`

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
| Metrics Explorer | `internal/modules/metricsexplorer` (`/metrics/names`, `/:metricName/tags`, `/explorer/query`) | `src/features/metrics` (`metricsExplorerApi.ts`) |
| Dashboard panels | `internal/platform/dashboardcfg/`, panel types | `dashboard/renderers/`, `dashboardPanelRegistry` |
| Auth | `internal/modules/user/auth/` | `shared/api/auth/` |
| Default config | `internal/modules/dashboard/`, `internal/platform/dashboardcfg/` | `defaultConfigService.ts` |
| Live tail (logs/traces) | `internal/infra/livetailws/`, `logs/search/livetail_*.go`, `traces/livetail/` | `useSocketStream.ts`, `useLiveTailStream.ts` |

---

## Maintenance

When you add a **new feature domain**: new package under `internal/modules/...`, implement `registry.Module`, register in `modules_manifest.go`. Update this index when module lists or contracts change.
