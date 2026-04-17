# Optik backend — codebase index

Orientation for **optikk-backend** (Go modular monolith). Read this file and `.cursor/rules/optik-backend.mdc` before substantive work in this repository.

## How assistants should use this document

- **Before** any substantive task: read **`CODEBASE_INDEX.md`** (this file), **`.cursor/rules/optik-backend.mdc`**, and **`.agent/philosophy/`** for strategic alignment. Follow **`.cursor/rules/engineering-workflow.mdc`** for planning and quality bar.
- **Plan before code:** Produce a plan (with options where appropriate) and **do not change code until the user approves** the plan, except for trivial one-line/typo fixes.
- **Agent Philosophy**: Mandatory reading for staff-level alignment:
  - **ADR-001**: [adr-001-strict-architecture.md](file:///Users/ramantayal/pro/optikk-backend/.agent/philosophy/adr-001-strict-architecture.md)
  - **Vision**: [vision-and-extensibility.md](file:///Users/ramantayal/pro/optikk-backend/.agent/philosophy/vision-and-extensibility.md)
  - **Architecture**: [system-architecture.md](file:///Users/ramantayal/pro/optikk-backend/.agent/philosophy/system-architecture.md)
- **Low-level design**: [.agent/architecture/lld.md](.agent/architecture/lld.md) — Mermaid flow + sequence diagrams for the **ingest path**, **query path** (incl. overview vs explorer profile routing), and **overall architecture topology**. Every arrow is wired to a concrete file:line. Includes runtime lifecycle, storage responsibilities, tenancy boundaries, and shipped-vs-gapped matrix.
- **Scalability audits** (`/.agent/audits/`): read before any perf/scale-motivated change.
  - **2026-04-17 Datadog-grade scalability audit**: [2026-04-17-scalability-audit.md](.agent/audits/2026-04-17-scalability-audit.md) — 15 gaps (ingest commit ordering, rollups, sharded flushers, distributed live-tail hub, leased alert evaluator, per-tenant limits, multi-region)
  - **2026-04-17 Capacity estimate vs Datadog**: [2026-04-17-capacity-estimate.md](.agent/audits/2026-04-17-capacity-estimate.md) — current-code ceiling (30–50 k spans/s ingest/pod, 20–50 tenants comfortable, 100–200 strained); component-by-component throughput math; ranked next-8 work items with lift estimates. **Headline: ~3–4 orders of magnitude short on ingest, ~2–3 on query fan-out. Rollups + sharded flushers close 90 % of the gap; cells + tenant isolation close the rest.**

## Related repository

The web app lives in the sibling repo **`optikk-frontend`** (see that repo's `CODEBASE_INDEX.md`).

**Hybrid model:** backend exposes data APIs under `/api/v1/...` (overview, infrastructure, saturation, etc.); **optikk-frontend** owns dashboard layout, tabs, and panel wiring (including the overview page). No backend `/default-config` or embedded dashboard JSON in this repo.

---

## Stack and entry

- **Stack:** Go 1.25, Gin, ClickHouse, MySQL, Redis, Kafka (optional OTLP ingest queue), native WebSocket live tail (`/api/v1/ws/live`), OTLP ingestion.
- **Module:** `github.com/Optikk-Org/optikk-backend`
- **Server entry:** `cmd/server/main.go` (telemetry in `logger.go`); MySQL and ClickHouse open in `internal/app/server/infra.go` with Redis, Kafka, and OTLP helpers

## Composition (where modules are wired)

| File | Purpose |
|------|---------|
| `internal/app/server/modules_manifest.go` | **`configuredModules()`** — single list of all `registry.Module` constructors (52 total: 48 HTTP + 4 ingestion, including `alerting` which also implements `BackgroundRunner`); add new HTTP/domain modules here |
| `internal/app/server/app.go` | App wiring; calls `newInfra` (`infra.go`) for MySQL, ClickHouse, Redis, sessions, Kafka topics, dispatchers, OTLP deps; native querier, module graph |
| `internal/app/registry/registry.go` | Shared dependency aliases for modules (querier, DB, tenant, config, platform session contract) |

## Runtime ownership

- `internal/infra/` owns cross-cutting capability contracts and provider selection.
- `internal/app/server/infra.go` builds the `Infra` bundle (Redis, sessions, live tail hub, Kafka topic ensure, OTLP dispatchers) used by `server.New`.
- `internal/infra/` owns concrete low-level implementations behind those platform contracts.
- `internal/modules/` and `internal/app/` should not import provider implementations like `internal/infra/session` or `internal/ingestion` directly; use the registry type aliases.

## Module packages (`internal/modules/`)

52 registered modules across 13 domains. Every module **must** follow the strict 6-file pattern: `handler.go`, `service.go`, `repository.go`, `module.go`, `dto.go`, `models.go`. All repository implementation methods must reside in the single `repository.go` file.

| Domain | Packages | Route prefix | Cache |
|--------|----------|-------------|-------|
| **Alerting** (5) | `alerting/{rules,incidents,silences,slack,engine}` (plus `evaluators`, `channels`) | `/alerts/*` | V1 |
| **APM** (1) | `overview/apm` | `/apm/*` | Cached |
| **Deployments** (1) | `services/deployments` | `/deployments/*` | Cached |
| **Explorer** (shared) | `explorer/analytics` (shared types+builder), `explorer/queryparser` (query parser) | Analytics routes owned by logs/traces explorers | — |
| **AI / GenAI** (5) | `ai/{overview,runs,analytics}` + `ai/explorer` + `llm/hub` | `GET /ai/overview/*` (summary/timeseries/top-models/top-prompts/quality); `POST /ai/explorer/query` + `GET /ai/runs/:runId[/related]`; `POST /explorer/ai/analytics`; explorer: `POST /ai/explorer/query`, `POST /ai/explorer/sessions/query`; hub: `POST/GET /ai/llm/scores`, `POST /ai/llm/scores/batch`, `GET/POST /ai/llm/prompts`, `PATCH/DELETE /ai/llm/prompts/:id`, `GET/POST /ai/llm/datasets`, `GET /ai/llm/datasets/:id`, `GET/PATCH /ai/llm/settings`. **Parent `ai/` has NO .go files** — shared Service + Repository + DTOs + models live in `ai/shared/`; `ai/factory/` wires the three submodules. Mirrors the alerting decomposition pattern. | V1 |
| **HTTP Metrics** (1) | `overview/httpmetrics` | `/http/*`, `/http/routes/*`, `/http/external/*` | Cached |
| **Infrastructure** (9) | `infrastructure/{cpu,disk,fleet,jvm,kubernetes,memory,network,nodes,resourceutil}` (consts: `infraconsts`) | `/infrastructure/*` | Cached |
| **Logs** (2) | `logs/explorer`, `logs/search` (shared: `logs/internal/shared`) | `/logs/*`, `POST /logs/explorer/query`, `POST /explorer/logs/analytics` | V1 |
| **Metrics** (1) | `metrics` | `/metrics/names`, `/metrics/:metricName/tags`, `POST /metrics/explorer/query` | V1 |
| **Overview** (4) | `overview/overview`, `overview/errors`, `overview/slo`, `overview/redmetrics` | `/overview/*`, `/errors/groups/*`, `/spans/red/*` | Cached |
| **Saturation** (10) | `saturation/database/{collection,connections,errors,latency,slowqueries,summary,system,systems,volume}`, `saturation/kafka` | `/saturation/*` | V1 (db), Cached (kafka/summary) |
| **Traces** (5) | `traces/{query,explorer,tracedetail,tracecompare,livetail}` (shared: `traces/shared`) | `/traces/*`, `/spans/*`, `/services/*`, `/latency/*`, `/errors/*`, `POST /explorer/traces/analytics` | Mixed |
| **User** (3) | `user/auth`, `user/team`, `user/user` (shared: `user/internal`) | `/auth/*`, `/users/*`, `/teams/*`, `/settings/*` | V1 |
| **Ingestion** (4) | via `internal/ingestion/otlp/{streamworkers,spans,logs,metrics}` | gRPC only (no HTTP routes) | — |


### Overview module routes

| Submodule | Key endpoints |
|-----------|--------------|
| `overview/overview` | `GET /overview/request-rate`, `/overview/error-rate`, `/overview/p95-latency`, `/overview/services`, `/overview/top-endpoints`, `/overview/endpoints/metrics` (alias), `/overview/endpoints/timeseries`, `/overview/summary` |
| `overview/errors` | `GET /overview/errors/{service-error-rate,error-volume,latency-during-error-windows,groups}`, `/errors/groups/:groupId/*`, `/errors/fingerprints/*`, `/spans/{exception-rate-by-type,error-hotspot,http-5xx-by-route}` |
| `overview/slo` | `GET /overview/slo`, `/overview/slo/stats`, `/overview/slo/burn-down`, `/overview/slo/burn-rate` |
| `overview/redmetrics` | `GET /spans/red/{summary,apdex,top-slow-operations,top-error-operations,request-rate,error-rate,p95-latency,span-kind-breakdown,errors-by-route}`, `/spans/latency-breakdown` |

### Alerting module routes

All under `/alerts/` prefix. Datadog-grade monitors: multi-window, multi-state (`ok|warn|alert|no_data|muted`), hysteresis (`for_secs`/`recover_for_secs`), per-group instances, Slack dispatch, deploy correlation, backtest.

| Endpoint | Purpose |
|----------|---------|
| `POST /alerts/rules` | Create rule |
| `GET /alerts/rules` | List team rules |
| `GET /alerts/rules/:id` | Rule + instance state |
| `PATCH /alerts/rules/:id` | Update rule |
| `DELETE /alerts/rules/:id` | Delete rule |
| `POST /alerts/rules/:id/mute` | Mute until timestamp |
| `POST /alerts/rules/:id/test` | Dry-run against live data |
| `POST /alerts/rules/:id/backtest` | Replay over historical range |
| `GET /alerts/rules/:id/audit` | Audit log (from ClickHouse `alert_events`) |
| `GET /alerts/incidents` | Live list of firing instances (team-scoped) |
| `POST /alerts/instances/:id/ack` | Ack an instance with optional `until` |
| `POST /alerts/instances/:id/snooze` | Snooze an instance N minutes |
| `GET /alerts/silences` / `POST` / `PATCH /:id` / `DELETE /:id` | Maintenance-window CRUD |
| `POST /alerts/callback/slack` | Slack action-button callback (v1 stub) |

**Storage:** single MySQL `observability.alerts` table (rule + instances + silences inline as JSON); append-only ClickHouse `observability.alert_events` for transitions/audit; MySQL `observability.alert_outbox` (Phase 3.3) for durable notification queue. **Runtime:** the module implements `registry.BackgroundRunner` — `NewEvaluatorLoop` ticks every 30s, runs `evaluators.Registry` (`slo_burn_rate`, `error_rate`, `http_check`, `ai_latency`, `ai_error_rate`, `ai_cost_spike`, `ai_quality_drop`) → `Decide` state machine → **outbox enqueue** + `Dispatcher` fast-path with Slack channel and deploy correlation via `repository.DeploysInRange`. Span-backed evaluators use `observability.spans` through the shared `NativeQuerier`; `http_check` performs outbound HTTP(S) probes with SSRF checks on resolved IPs.

**Phase 3 HA (2026-04-17 audit):**
- **Redis lease** (`internal/modules/alerting/lease.go`): `newLeaser(redisClient, 60s)`; `evalRule` calls `leaser.Acquire(ctx, rule.ID)` before evaluating. `SET NX PX 60000` per rule — dead pod's rules migrate within lease TTL. Fail-closed on Redis errors to prevent double-dispatch. Redis nil ⇒ always-acquire (single-pod dev).
- **Durable outbox** (`internal/modules/alerting/outbox.go`): on every transition the evaluator writes a row to `observability.alert_outbox` keyed by `(alert_id, instance_key, transition_seq)`; the in-memory `Dispatcher` fast-path sends Slack immediately and marks the row delivered. `OutboxRelay` goroutine polls every 15s, uses `SELECT ... FOR UPDATE SKIP LOCKED` to claim batches, exponential backoff (30s → 2h) on Slack failures, survives pod crash and webhook outage.

**Phase 3 submodule split (2026-04-17 audit):** The former monolithic `alerting` module is now a true subpackage tree. **Parent `internal/modules/alerting/` contains no .go files** — it is a pure directory container. Every file lives inside a subpackage:

- `alerting/shared/` — Rule, Instance, Silence, DeployRef, AlertEvent, DTOs, definition.go, statemachine.go, template.go, helpers.go, errors.go. All shared types + rendering + state-machine logic.
- `alerting/rules/` — `handler.go`, `service.go`, `repository.go` (MySQL alerts CRUD + inline instance state + scanRule), `dto.go`, `module.go` — owns `/alerts/rules/*` plus mute/test/backtest/audit.
- `alerting/incidents/` — `handler.go`, `service.go` (depends on `rules.Repository` + `engine.EventStore`), `dto.go`, `module.go` — owns `/alerts/incidents`, `/activity`, `/instances/:id/{ack,snooze}`.
- `alerting/silences/` — same layout; `/alerts/silences/*`.
- `alerting/slack/` — same layout; service takes `*engine.Dispatcher` for the outbound SendSlack.
- `alerting/engine/` — `evaluator_loop.go`, `dispatcher.go`, `outbox.go` (Store + Relay), `lease.go` (Redis SET NX PX), `backtest.go`, `store.go` (ClickHouse-backed `EventStore` + `DataSource`), `runner.go`, `module.go` (BackgroundRunner). No HTTP routes.
- `alerting/factory/` — composes all five submodules into `[]registry.Module` for `modules_manifest.go`.

Import graph: `factory → rules + incidents + silences + slack + engine`; `rules, engine → shared`; `incidents, silences → shared + rules`; `slack → shared + engine`. No cycles; parent directory is type-free.

### Traces module routes

| Submodule | Key endpoints |
|-----------|--------------|
| `traces/query` | `GET /traces`, `/traces/:traceId/spans`, `/spans/:spanId/tree`, `/spans/search`, `/services/:serviceName/errors/*`, `/latency/*`, `/errors/*` |
| `services/topology` | `GET /services/topology` — runtime service map (nodes + directed RED-weighted edges from parent→child span joins); optional `?service=<name>` for 1-hop neighborhood. Cached (30s). |
| `traces/explorer` | `POST /traces/explorer/query` |
| `traces/tracedetail` | `GET /traces/:traceId/{span-events,span-kind-breakdown,critical-path,span-self-times,error-path,flamegraph,logs,related}`, `/traces/:traceId/spans/:spanId/attributes` |
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
| `fleet` | `fleet/pods` — root-span aggregates per `k8s.pod.name` + host (for fleet UI pod lens) |
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

- **`internal/modules/livetail/handler.go`** — WebSocket entrypoint; depends on session + live-tail hub contracts
- **`internal/modules/logs/search/livetail_payload.go`** — `SubscribeLogsPayload`: filter fields: Severities, Services, Hosts, Pods, Containers, Environments, TraceID, SpanID, Search, SearchMode, ExcludeSeverities, ExcludeServices, ExcludeHosts, AttributeFilters
- WebSocket protocol: client sends `subscribe:logs` op to `/api/v1/ws/live`, backend receives events from the runtime live-tail hub fed by OTLP stream workers

## Ingestion

- **OTLP Pipeline**: `internal/ingestion/otlp/` — gRPC export explicitly mapped to concrete structs (`LogRow`, `SpanRow`, `MetricRow`).
- **Authentication**: `internal/ingestion/otlp/auth/` — team resolution via API keys; optional Redis cache (TTL) when Redis is enabled.
- **Dispatch contracts & implementations**: `internal/ingestion/` — `Dispatcher[T]`, `TelemetryBatch[T]`, **`AckableBatch[T]`** (Phase 1: persistence channel now yields ack-carrying batches), OTLP dependency interfaces; `kafka_dispatcher.go` — Kafka-backed OTLP ingest queue with ack-gated offset commit + per-signal DLQ produce on flush failure.
- **Kafka admin (topics)**: `internal/infra/kafka/topics.go` — `EnsureTopics`, `IngestTopicNames` (now also returns `.dlq` siblings per signal), `DLQTopicFor(topic)`.
- **Background consumers**: `internal/ingestion/otlp/streamworkers/` — `BackgroundRunner` with separate routines per signal. Persistence is **per-AckableBatch** (one Kafka record = one CH insert = one Ack); no cross-record batching. **ClickHouse** writers use `CHFlusher[T].Flush(rows, dedupToken)` — `dedupToken` is the SHA-1 of the source Kafka record bytes and is passed as `insert_deduplication_token` so redelivered records collapse to a single write.
- **Phase 1 data-loss guarantees** (2026-04-17 audit): no silent drops. Flush error → DLQ produce + offset commit. Crash between flush and commit → redelivery collapses via dedup token. CH schemas set `non_replicated_deduplication_window = 1000` (local) / `replicated_deduplication_window = 1000` (prod).

## Infrastructure Layer (`internal/infra/`)

| Package | Purpose |
|---------|---------|
| `session` | Session/auth contract used by app, middleware, WebSocket auth, and auth module |
| `ratelimit` | Rate limiter contract and provider-facing API |
| `livetail` | Live-tail hub contract |
| `kafka` | Kafka topic creation for OTLP ingest queues |

## Internal Infrastructure (`internal/infra/`)

| Package | Purpose |
|---------|---------|
| `timebucket` | Adaptive time bucketing for ClickHouse aggregations (minute/5min/hour/day) |
| `validation` | Schema-based validation logic |
| `cache` | Query and object caching |
| `database` | **`NativeQuerier`** and ClickHouse/MySQL connection management |
| `kafka` | `EnsureTopics` / `IngestTopicNames` for ingest Kafka topics |
| `livetail` | Redis-Streams-backed live-tail hub (`internal/modules/livetail/redis_hub.go`). `livetail.NewHub(redisClient, maxLen)` is the **only** constructor; Redis is a hard dependency (LocalHub was removed in Phase 2 of the 2026-04-17 audit for cross-pod fan-out). |
| `livetail` | Live tail WebSocket module (`GET /api/v1/ws/live`); `handler.go` upgrades WS; registered from `modules_manifest.go` |
| `redis` | go-redis + Redigo pool construction and ping |
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
    → RateLimiter (middleware default limiter)
    → [cache.CacheResponse 30s for registry.Cached modules]
    → Handler → Service → Repository → ClickHouse/MySQL
  → contracts.APIResponse envelope
```

**Public prefixes** (skip TenantMiddleware): `/api/v1/auth/login`, `/otlp/`, `/health`

**Session keys** (`internal/infra/session/manager.go`): `auth_user_id`, `auth_email`, `auth_role`, `auth_default_team_id`, `auth_team_ids`

## ClickHouse Query Helpers (`internal/infra/database/`)

### Query profiles (Phase 4, 2026-04-17 audit)

Profile-aware resource budgets on `NativeQuerier`:

| Method | Profile | Budget |
|--------|---------|--------|
| `SelectOverview` / `QueryRowOverview` | `ProfileOverview` | 15 s / 100M rows / 2 GB |
| `SelectExplorer` / `QueryRowExplorer` | `ProfileExplorer` | 60 s / 1B rows / 8 GB |
| `Select` / `QueryRow` | *deprecated alias for Overview* | — |

Call-site guidance:
- **Overview** (dashboards, infra, saturation, HTTP metrics, services): default — no change needed from existing `.Select` / `.QueryRow` call sites.
- **Explorer** (logs/explorer, traces/explorer, tracedetail, ai/explorer, metrics timeseries): **must** use `SelectExplorer` / `QueryRowExplorer`. Already migrated: `modules/logs/explorer/repository.go`, `modules/traces/tracedetail/repository.go`, `modules/ai/explorer/repository.go`, `modules/traces/explorer/analytics_handler.go`, `modules/metrics/repository.go::QueryTimeseries`.
- Typed helpers: `SelectTyped` / `QueryRowTyped` use Overview; `SelectTypedExplorer` / `QueryRowTypedExplorer` use Explorer.

### Query execution (`query.go`)

| Helper | Signature | Purpose |
|--------|-----------|---------|
| `QueryMaps` | `(querier, sql, args...) → []map[string]any` | Execute query, 10K row limit |
| `QueryMapsLimit` | `(querier, limit, sql, args...) → []map[string]any` | Custom row limit |
| `QueryMap` | `(querier, sql, args...) → map[string]any` | Single row result |
| `QueryCount` | `(querier, sql, args...) → int64` | Execute COUNT query |
| `JSONString` | `(any) → string` | Marshal to JSON with `{}` fallback |
| `MustAtoi64` | `(string, fallback) → int64` | Parse int64 with fallback |
| `RowsAffected` | `(sql.Result) → int64` | Safe extraction |

**Native Slice Expansion:** Both MySQL (`sqlx`) and ClickHouse (`clickhouse-go/v2`) now support direct slice expansion. Use `WHERE col IN (?)` (MySQL) or `WHERE col IN @name` (ClickHouse) and pass the slice directly in the arguments. Manual `InClause` builders have been removed to reduce boilerplate.

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

## Overview & dashboard UI (frontend-owned)

The **overview** hub (tabs, panel grid, which `/overview/*` or related endpoints each chart calls) lives in **optikk-frontend** — see that repo’s `CODEBASE_INDEX.md` and dashboard/panel registry. This backend only provides **JSON data APIs** via `internal/modules/overview/*` (`/overview/...`, `/errors/...`, `/spans/red/...`, etc.) and does not serve dashboard layout or `/default-config`.

**Infrastructure** and **saturation** UIs are also frontend-owned; **`internal/modules/infrastructure/*`** and **`internal/modules/saturation/*`** are the HTTP data plane.

## Config Structure (`internal/config/config.go`)

`config.yml` → Viper → `Config` struct. Loaded via `config.Load()` using `github.com/spf13/viper`.

**Environment variable overrides:** All config values can be overridden via env vars with the `OPTIKK_` prefix. Convention: `OPTIKK_` + uppercase YAML path with `.` replaced by `_`.

| YAML key | Env var |
|----------|---------|
| `mysql.host` | `OPTIKK_MYSQL_HOST` |
| `clickhouse.production` | `OPTIKK_CLICKHOUSE_PRODUCTION` |
| `session.cookie_name` | `OPTIKK_SESSION_COOKIE_NAME` |
| `redis.enabled` | `OPTIKK_REDIS_ENABLED` |
| `otl_redis_stream.ch_batch_size` | `OPTIKK_OTL_REDIS_STREAM_CH_BATCH_SIZE` |
| `kafka.broker_list` | `OPTIKK_KAFKA_BROKER_LIST` |
| `kafka.consumer_group` | `OPTIKK_KAFKA_CONSUMER_GROUP` |
| `kafka.topic_prefix` | `OPTIKK_KAFKA_TOPIC_PREFIX` |

Note: `ENV`, `LOG_LEVEL`, `LOG_FORMAT` are separate (`os.Getenv` in `main.go`) — not managed by Viper.

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
| `app` | `AppConfig` | `region` |

## Extension Interfaces

**`internal/app/registry/registry.go`** defines the core extension points:
- **`Module`**: Standard HTTP/domain module.
- **`GRPCRegistrar`**: For modules exposing gRPC services.
- **`BackgroundRunner`**: For modules with lifecycle-managed workers.
---

## Backend ↔ frontend map (cross-repo)

Use when a change spans API and UI. Frontend paths refer to **`optikk-frontend`**.

| Product area | This repo | Frontend (`optikk-frontend`) |
|--------------|-----------|----------------------------|
| Registry / route wiring | `modules_manifest.go` | `domainRegistry.ts`, feature `index.ts` |
| Explorer APIs | `internal/modules/.../handler.go` | Feature `api/` or `shared/api` |
| Metrics | `internal/modules/metrics` (`/metrics/names`, `/:metricName/tags`, `/explorer/query`) | `src/features/metrics` (`metricsExplorerApi.ts`) |
| Overview & dashboard layout | `internal/modules/overview/*` (data only) | Overview hub, `dashboard/renderers/`, panel registry in **optikk-frontend** |
| Auth | `internal/modules/user/auth/` | `shared/api/auth/` |
| Live tail (logs/traces) | `internal/modules/livetail/`, `logs/search/livetail_payload.go`, `traces/livetail/` | `useSocketStream.ts`, `useLiveTailStream.ts` |
| Infrastructure | `internal/modules/infrastructure/*/` (`/v1/infrastructure/*`); includes `fleet/pods` for pod-level aggregates | `src/features/infrastructure/` — **frontend-owned** hub, host + pod fleet lens, map, query UI |

---

## Maintenance

When you add a **new feature domain**: new package under `internal/modules/...`, implement `registry.Module`, register in `modules_manifest.go`. Update this index when module lists or contracts change.
