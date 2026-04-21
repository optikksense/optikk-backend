# Optik Backend — Claude Code Instructions

## Before any task

1. Read **`CODEBASE_INDEX.md`** (repo root) — full map of modules, ingestion, LLD patterns, and cross-repo references.
2. Read **`.cursor/rules/optik-backend.mdc`** — hot paths, handler/service/repository patterns, ClickHouse helpers, middleware stack, API envelope.
3. Read **`.cursor/rules/engineering-workflow.mdc`** — plan before code, two approaches with pros/cons, approval gate.
4. **Do not modify files** until the user approves the plan (except trivial one-line fixes).

## After every iteration

After completing any task — no matter how small — review and update the following if anything changed:

1. **`CODEBASE_INDEX.md`** — new modules, endpoints, helpers, config sections, cross-repo contracts
2. **`.cursor/rules/optik-backend.mdc`** — new patterns, conventions, hot paths, or LLD details
3. **`.agent/SKILL.md`** — keep aligned with cursor rules
4. **This file (`CLAUDE.md`)** — new quick-reference paths or principles

This is **mandatory**, not optional. The documentation must always reflect the current architecture so the next session (by any AI tool) does not need to scan the full codebase. If nothing changed, skip — but always check.

## Scale audits

- `.agent/audits/2026-04-17-scalability-audit.md` — Datadog-grade gap list (15 items) + 5-phase refactor plan. Read before any perf/scale-motivated change to ingestion, CH query paths, alerting, or live tail.

## Quick reference

- **Stack**: Go 1.25, Gin, ClickHouse, MySQL, Redis, WebSocket live tail, OTLP
- **Server entry**: `cmd/server/main.go`
- **Module registration**: `internal/app/server/modules_manifest.go` → `configuredModules()` — the two composite factories `alerting_factory.NewModules` and `ai_factory.NewModules` spread their subpackage slices alongside the other direct-registered modules; `alerting.engine` is the only `BackgroundRunner`.
- **Handler helpers**: `internal/shared/httputil/base.go` — `RespondOK`, `RespondErrorWithCause`, `ParseRequiredRange`
- **Error codes**: `internal/shared/contracts/errorcode/codes.go`
- **ClickHouse helpers**: `internal/infra/database/` — `QueryMaps`, `QueryCount`, `InClause`, `NamedInClause`, `SqlTime`, type extractors (`Int64FromAny`, `Float64FromAny`, `StringFromAny`, `BoolFromAny`, `TimeFromAny`, nullable variants)
- **Time bucketing**: `internal/infra/timebucket/timebucket.go` — adaptive (1m/5m/15m/1h/1d) + `ByName()` for explicit steps
- **Session**: `internal/infra/session/manager.go` — keys: `auth_user_id`, `auth_email`, `auth_role`, `auth_default_team_id`, `auth_team_ids`
- **Middleware**: `internal/infra/middleware/` — public prefixes: `/api/v1/auth/login`, `/otlp/`, `/health`
- **Overview & dashboard UI**: **optikk-frontend** owns layout/tabs/panels; backend data only via `internal/modules/overview/{overview,errors,slo,redmetrics}/` — `/api/v1/overview/*`, `/errors/*`, `/spans/red/*`, etc.
- **HTTP Metrics**: `internal/modules/httpmetrics/` — `/api/v1/http/*`
- **Infrastructure**: `internal/modules/infrastructure/{cpu,disk,jvm,kubernetes,memory,network,nodes,resourceutil}/` — `/api/v1/infrastructure/*`
- **Saturation DB**: `internal/modules/saturation/database/{collection,connections,errors,latency,slowqueries,summary,system,systems,volume}/` — `/api/v1/saturation/*`
- **Saturation Kafka**: `internal/modules/saturation/kafka/` — `/api/v1/saturation/kafka/*`
- **Deployments API**: `internal/modules/deployments/` — `/api/v1/deployments/*` (exposes `GetDeploysInRange` for alerting deploy correlation)
- **Alerting**: `internal/modules/alerting/{rules,incidents,silences,slack,engine}` (plus existing `evaluators/`, `channels/` and the composition `factory/`) — `/api/v1/alerts/*`. Parent carries only shared types + helpers; each submodule is a proper directory with handler + service (+ repo for rules). `engine/` is the sole `BackgroundRunner` (evaluator loop + dispatcher + outbox relay + Redis lease).
- **Logs live tail**: `internal/modules/logs/search/livetail_run.go`, `livetail_payload.go` — Redis Stream subscription, no ClickHouse polling
- **Explorer**: analytics owned by logs/traces explorers (`POST /explorer/logs/analytics`, `POST /explorer/traces/analytics`); shared types in `explorer/analytics/`, query parser in `explorer/queryparser/`
- **Traces**: `internal/modules/traces/{query,explorer,tracedetail,redmetrics,errorfingerprint,errortracking,tracecompare,livetail}/` — tracedetail includes `/traces/:traceId/logs` for trace-correlated log retrieval
- **Config**: `internal/config/config.go` (loads `config.yml`; `redis.password` / `redis.db` optional for secured Redis)
- **Rollup tier selection**: `internal/infra/rollup/tier.go` — `TierTableFor(prefix, startMs, endMs) (table, stepMin)` picks the coarsest cascade tier (`_1m` / `_5m` / `_1h`) for the query range. Every repository that reads a CH rollup table goes through this helper. Thresholds: ≤ 3h → 1m, ≤ 24h → 5m, > 24h → 1h. See [docs/hld/ingest/ingest.md](docs/hld/ingest/ingest.md) "Phase 6" section for the cascade model.
- **CH rollup tables** (all AggregatingMergeTree, 1m/5m/1h cascade, 90-day TTL). Phase 5/6: `spans_rollup`, `metrics_histograms_rollup`, `logs_rollup`, `ai_spans_rollup`, `spans_error_fingerprint`, `spans_host_rollup`, `spans_by_version`. **Phase 7 additions**: `metrics_gauges_rollup` (service/host/pod/state_dim gauges for apm + httpmetrics + infrastructure), `metrics_gauges_by_status_rollup` (http req-rate by status code), `db_histograms_rollup` (saturation DB latency/volume/errors keyed on db_system/db_operation/db_collection/db_namespace/pool_name/error_type/server_address), `messaging_histograms_rollup` (saturation Kafka — **currently unused, kafka repository stays on raw until Phase 8**), `spans_topology_rollup` (service-to-service edges via `mat_peer_service` on CLIENT spans). State columns via `quantilesTDigestWeighted` + `sum` + `any` + `min` / `max` + `argMax` (for gauge last-value). **Never emit `quantileExact*` or `uniqExact*` in SQL**; use the `*Merge` combinator on the rollup's state column (e.g. `quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2` for p95).
- **Stats helpers**: `internal/infra/stats/` — stdlib-only `AvgNonNull` / `MaxNonNull` / `AvgNonNullPtr`, replaces `arrayReduce('avg', arrayFilter(isNotNull, [...]))` in infrastructure/cpu + infrastructure/memory.
- **Sibling repo**: `optic-frontend` (see its `CODEBASE_INDEX.md`)

## Engineering principles

- **Module Architecture**: Strict 6-file pattern (`handler.go`, `service.go`, `repository.go`, `module.go`, `dto.go`, `models.go`). All repository methods must stay in `repository.go`.
- **SOLID & DRY**: Factor shared behavior when a pattern appears more than once.
- **Quality**: Leave the code clearer or simpler with every change.
- **No unsolicited tests**: Do not add tests unless explicitly asked.
