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
| **Ingestion** (3) | `internal/ingestion/{spans,metrics,logs}` (each: `handler.go` + `mapper.go` + `producer.go` + `consumer.go` + optional `livetail.go` + `module.go`) | OTLP gRPC on `:4317` (no HTTP OTLP) | — |


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

**Storage:** single MySQL `observability.alerts` table (rule + instances + silences inline as JSON); append-only ClickHouse `observability.alert_events` for transitions/audit; MySQL `observability.alert_outbox` (Phase 3.3) for durable notification queue. **Runtime:** the module implements `registry.BackgroundRunner` — `NewEvaluatorLoop` ticks every 30s, runs `evaluators.Registry` (`slo_burn_rate`, `error_rate`, `http_check`, `ai_latency`, `ai_error_rate`, `ai_cost_spike`, `ai_quality_drop`) → `Decide` state machine → **outbox enqueue** + `Dispatcher` fast-path with Slack channel and deploy correlation via `repository.DeploysInRange`. Span-backed evaluators use `observability.spans` via the shared `clickhouse.Conn` (wrap the request ctx with `database.OverviewCtx` or `database.ExplorerCtx` to apply the budget); `http_check` performs outbound HTTP(S) probes with SSRF checks on resolved IPs.

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

**Note on Saturation Performance:** The `kafka/summary` and related explorer queries make multiple heavy reads to `observability.metrics`. These are parallelized via `errgroup` in `GetKafkaSummary` and `buildKafkaGroupRows` to keep response times under 6s even under massive OTLP metric ingestion loads.
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

See **[docs/hld/ingest/ingest.md](docs/hld/ingest/ingest.md)** for the end-to-end ingest HLD diagram.

- **OTLP entry**: gRPC only on port `4317` (no OTLP HTTP variant). Authenticated via `internal/auth/` interceptors which resolve `team_id` from an API-key metadata header before any handler runs. No shared `internal/ingestion/otlp/` directory — each signal owns its own gRPC service implementation.
- **Per-signal pipelines**: `internal/ingestion/spans/`, `internal/ingestion/metrics/`, `internal/ingestion/logs/`. Each directory contains: `handler.go` (gRPC `*ServiceServer` impl) → `mapper.go` (OTLP proto → signal-local `Row` protobuf) → `producer.go` (Kafka produce via `internal/infra/kafka/producer.go`) → `consumer.go` (Kafka poll + ClickHouse batch insert) → `module.go` (wiring). Spans and logs additionally ship `livetail.go` — a second Kafka consumer on the same topic with a different group that publishes to `livetail.Hub` for WebSocket subscribers.
- **Kafka**: `github.com/twmb/franz-go/pkg/kgo`. Topics: `<prefix>.<signal>` (prefix from `cfg.KafkaTopicPrefix()`, default `optikk.ingest`). Consumer groups: `<base>.<signal>.<role>` where `role ∈ {persistence, livetail}` (default `base` is `optikk-ingest`). Five consumer clients (spans persistence + livetail, metrics persistence, logs persistence + livetail) plus one shared producer. Topics created idempotently at startup via `internal/infra/kafka/topics.go:EnsureTopics`.
- **ClickHouse write**: each persistence `consumer.go` uses native `clickhouse-go/v2` batch API — `ch.PrepareBatch(ctx, insertSQL)` → `batch.Append(values...)` per row → `batch.Send()`. One Kafka poll-batch = one CH batch = one offset commit. 30 s insert timeout. At-least-once delivery; ClickHouse's own `non_replicated_deduplication_window = 1000` (local) / `replicated_deduplication_window = 1000` (prod) covers in-flight redeliveries. There is no separate `chbatch` / `CHFlusher` / `streamworkers` package in the current codebase — the driver's batch is the flusher.
- **Sketch aggregator**: historically, the spans and metrics consumers each called `sketch.Aggregator.ObserveLatency(...)` / `ObserveIdentity(...)` after every CH flush, producing in-memory DDSketches that a 15 s flush loop persisted to Redis under `optikk:sk:*`. Phase 5 **removed** this — the new rollup MVs in ClickHouse (`spans_rollup_1m`, `metrics_histograms_rollup_1m`) take over.

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
| `database` | ClickHouse/MySQL connection management; `OverviewCtx` / `ExplorerCtx` per-query budget helpers |
| `kafka` | `EnsureTopics` / `IngestTopicNames` for ingest Kafka topics |
| `livetail` | Redis-Streams-backed live-tail hub (`internal/modules/livetail/redis_hub.go`). `livetail.NewHub(redisClient, maxLen)` is the **only** constructor; Redis is a hard dependency (LocalHub was removed in Phase 2 of the 2026-04-17 audit for cross-pod fan-out). |
| `livetail` | Live tail WebSocket module (`GET /api/v1/ws/live`); `handler.go` upgrades WS; registered from `modules_manifest.go` |
| `redis` | go-redis + Redigo pool construction and ping |
| `middleware` | HTTP middleware: CORS, error recovery, tenant context, rate limiting middleware |
| `session` | Default `scs/v2` session manager implementation; keys: `auth_user_id`, `auth_email`, `auth_role`, `auth_default_team_id`, `auth_team_ids` |
| `utils` | String conversion and time parsing helpers |
| `stats` | Stdlib-only numeric helpers (`AvgNonNull`, `MaxNonNull`, `AvgNonNullPtr`) — replaces SQL `arrayReduce('avg', arrayFilter(isNotNull, [...]))` patterns. |
| `sketch` | Datadog-style mergeable distribution + cardinality sketches. `Digest` wraps `github.com/caio/go-tdigest/v4` (same algorithm as CH `quantileTDigest`); `HLL` wraps `github.com/axiomhq/hyperloglog` (same class as CH `uniq()`). `Aggregator` folds records in-process during ingest; `RedisStore` persists minute-bucketed sketches (`optikk:sk:<kindID>:<teamID>:<dimHash>:<unix>`); `Querier` + `CHFallback` power service-layer percentile / cardinality reads without heavy SQL. Used by `internal/ingestion/{spans,metrics}/consumer.go` today; available for repositories to adopt incrementally. |

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

**Constructor chain:** `NewModule(chConn, getTenant)` → `Handler{GetTenant, Service: NewService(NewRepository(chConn))}` → register in `modules_manifest.go:configuredModules()`. `chConn` is `clickhouse.Conn` (native driver). Repositories wrap the request ctx with `database.OverviewCtx` / `database.ExplorerCtx` at call time to apply the query budget.

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

### Query budgets (Phase 4, 2026-04-17 audit)

Repositories hold `clickhouse.Conn` directly. Budget is applied per-query by wrapping the ctx with one of the helpers in `internal/infra/database/clickhouse.go`:

| Helper | Budget | Who uses it |
|--------|--------|-------------|
| `database.OverviewCtx(ctx)` | 15 s / 100M rows / 2 GB / 100K result rows | Dashboards, infra, saturation, HTTP metrics, services |
| `database.ExplorerCtx(ctx)` | 60 s / 1B rows / 8 GB / 1M result rows | Explorer / tracedetail / ai-explorer / metrics timeseries |

Call style:
```go
err := r.db.Select(database.OverviewCtx(ctx), &rows, query, args...)
err := r.db.QueryRow(database.ExplorerCtx(ctx), query, args...).ScanStruct(&row)
```

Settings ride on the ctx via `clickhouse.Context(parent, clickhouse.WithSettings(...))`. Budgets are defined as typed `QueryBudget` structs (`database.Overview` / `database.Explorer`); tests in `tests/database/budget_test.go` assert against those fields directly.

Typed helpers: `database.SelectTyped[T]` / `database.QueryRowTyped[T]` — caller passes the right ctx (no Overview/Explorer flavor split).

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

### Phase 7 — gauge + DB + topology rollups (2026-04-21)

Closes the remaining raw-scan aggregate gaps opened at the tail of Phase 6. Five new cascade rollups — `metrics_gauges_rollup`, `metrics_gauges_by_status_rollup`, `db_histograms_rollup`, `messaging_histograms_rollup`, `spans_topology_rollup` — all `_1m`/`_5m`/`_1h` tiers populated by ingest + cascade MVs. Tier selection via `rollup.TierTableFor` (unchanged helper).

| Repo | Migrated methods | Target rollup |
|------|------------------|---------------|
| `overview/apm/repository.go` | `GetUptime`, `GetOpenFDs`, `GetProcessCPU`, `GetProcessMemory`, `GetActiveRequests` | `metrics_gauges_rollup` |
| `overview/httpmetrics/repository.go` | `GetRequestRate`, `GetActiveRequests` | `metrics_gauges_by_status_rollup` + `metrics_gauges_rollup` |
| `infrastructure/{cpu,disk,memory,network}/repository.go` | simple single-metric gauge methods (see file TODOs) | `metrics_gauges_rollup` |
| `saturation/database/{collection,system,connections,slowqueries,systems,volume}/repository.go` | histogram-percentile methods | `db_histograms_rollup` |
| `services/topology/repository.go` | `GetNodes`, `GetEdges` (removes span self-join) | `spans_rollup` + `spans_topology_rollup` |

Deferred to Phase 8 (documented in-file with `// TODO(phase8):`):
- Infrastructure utilization/percentage methods (multi-metric avgIf fallbacks that the gauge rollup can't model without carrying more state).
- `infrastructure/{jvm,kubernetes,connpool}` — multi-metric composition + attribute group-bys outside `state_dim`.
- `saturation/kafka` — multi-alias topic/operation attribute coalesce.
- `saturation/database/slowqueries.GetSlowQueryPatterns` + `GetP99ByQueryText` — group by high-cardinality `db.query.text`.
- `GetCollectionQueryTexts` — same reason.
- Connection-count / connection-timeout gauge queries in `saturation/database/connections` — pool_name + state aren't in the generic gauge `state_dim`.

### Phase 6 — rollup-backed aggregate reads (2026-04-20)

Five repository clusters now read from `observability.<kind>_rollup_{1m,5m,1h}` cascades via `rollup.TierTableFor(prefix, startMs, endMs)`:

| Repo | Migrated methods | Target rollup |
|------|------------------|---------------|
| `infrastructure/nodes/repository.go` | `GetInfrastructureNodes`, `GetInfrastructureNodeServices` | `spans_host_rollup` |
| `infrastructure/fleet/repository.go` | `GetFleetPods` | `spans_host_rollup` |
| `traces/query/repository.go` | `getTraceSummary`, `GetTraceFacets`, `GetTraceTrend`, `GetErrorTimeSeries`, `GetLatencyHistogram` | `spans_rollup` |
| `logs/explorer/repository.go` | `GetLogHistogram`, `GetLogVolume`, `GetLogStats`, `GetTopGroups`, `GetAggregateSeries` | `logs_rollup` |
| `ai/explorer/repository.go` | `GetAISummary`, `GetAITrend` | `ai_spans_rollup` |
| `ai/shared/repository.go` | `GetTopModels` | `ai_spans_rollup` |
| `services/deployments/repository.go` | `ListDeployments`, `ListServiceDeployments`, `GetLatestDeploymentsByService`, `GetDeploysInRange`, `GetVersionTraffic` | `spans_by_version` |

Each migrated file carries a local `queryIntervalMinutes(tierStepMin, startMs, endMs)` helper matching the pattern in `overview/overview/repository.go`. Drill-down/per-trace/per-span methods and queries that require attributes / body / per-span columns not carried by the rollup stay on the raw `observability.{spans,logs}` tables. Rollup-incompatible filters (attribute filters, body search, trace_id, min/max duration, span kind, etc.) are silently dropped when a migrated method is called.

Known limitations (stay on raw due to DTO contract requiring per-span fields):
- `traces/tracedetail.GetRelatedTraces` — DTO returns span_id/trace_id/start_time which the rollup does not retain.
- `ai/shared.GetTopPrompts` — rollup does not carry `prompt_template` as a key dimension.

`GetLatencyHistogram` is approximated from 10 equal-mass t-digest quantile samples (p0…p1 in 0.1 steps); `span_count` is evenly distributed across buckets so the shape of the returned DTO is preserved.

### Percentiles / cardinality conventions (2026-04-20 "compute-in-service" refactor)

Heavy compute functions never use exact variants in SQL:

| Never emit | Emit instead | Why |
|---|---|---|
| `quantile(...)` (ambiguous) | `quantileTDigest(...)` | Explicit t-digest is the same algorithm CH uses by default today, but pinning prevents surprise regressions if the alias ever changes. |
| `quantileExact(...)` | `quantileTDigest(...)` | Exact percentile materializes every sample; t-digest is a ~1 KB sketch per group. 10–100× less memory/CPU, ≤1 % error. |
| `quantileExactWeighted(...)` | `quantileTDigestWeighted(...)` | Same tradeoff for weighted histogram data. |
| `quantileExactWeightedIf(...)` | `quantileTDigestWeightedIf(...)` | Same tradeoff with the `-If` combinator. |
| `uniqExact(...)` / `uniqExactIf(...)` | `uniq(...)` / `uniqIf(...)` or `sketch.Querier.Cardinality` | CH `uniq*` is HLL — already approximate. Exact variants build a full hash table. |
| `arrayReduce('avg', arrayFilter(isNotNull, [...]))` | `internal/infra/stats.AvgNonNullPtr(...)` in Go **or** `(coalesce(a,0)+coalesce(b,0)+…) / nullIf((if(a IS NULL,0,1)+if(b IS NULL,0,1)+…), 0)` in SQL | Array materialization per row is wasteful; stdlib helper is 10 lines. |
| `(groupArray(col))[1]` as a sample picker | `any(col)` | `groupArray` collects the entire array in memory before indexing; `any()` picks one value with zero materialization. |

For genuinely performance-critical quantile/cardinality paths, skip CH entirely and use `internal/infra/sketch`:

1. Ingest (`internal/ingestion/{spans,metrics}/consumer.go`) feeds every record into `sketch.Aggregator.ObserveLatency` / `ObserveIdentity`.
2. Aggregator ticks every 15 s, flushing minute-aligned closed buckets to Redis under `optikk:sk:<kindID>:<teamID>:<dimHash>:<unixMinute>` with a 15-day TTL.
3. Service layer reads via `sketch.Querier.Percentiles(kind, teamID, startMs, endMs, qs...)` or `.Cardinality(...)`. ClickHouse fallback (`sketch.CHFallback`) activates when a dim has no sketch coverage (cold start, Redis outage); it issues a narrow `quantileTDigest` / `uniq` query.

Sketch kinds today: `SpanLatencyService`, `SpanLatencyEndpoint`, `DbOpLatency`, `KafkaTopicLatency`, `NodePodCount`, `AiTraceCount` (see `internal/infra/sketch/kind.go`). Add a kind by appending to `AllKinds` — never renumber.

Follow-up (tracked): per-repository migration to `sketch.Querier`-first reads. The tactical `quantileExact* → quantileTDigest*` swap in this PR harvests the algorithmic win while the per-module sketch-backed path is rolled out.

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
| `clickhouse.secure` | `OPTIKK_CLICKHOUSE_SECURE` |
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
| `clickhouse` | `ClickHouseConfig` | `host`, `port`, `database`, `user`, `password`, `secure` |
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
