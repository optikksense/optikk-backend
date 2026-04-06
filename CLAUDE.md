# Optik Backend — Claude Code Instructions

## Before any task

1. Read **`CODEBASE_INDEX.md`** (repo root) — full map of modules, ingestion, dashboard config, LLD patterns, and cross-repo references.
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

## Quick reference

- **Stack**: Go 1.25, Gin, ClickHouse, MySQL, Redis, WebSocket live tail, OTLP
- **Server entry**: `cmd/server/main.go`
- **Module registration**: `internal/app/server/modules_manifest.go` → `configuredModules()` — 51 constructors (47 HTTP + 4 ingestion)
- **Handler helpers**: `internal/shared/httputil/base.go` — `RespondOK`, `RespondErrorWithCause`, `ParseRequiredRange`
- **Error codes**: `internal/shared/contracts/errorcode/codes.go`
- **ClickHouse helpers**: `internal/infra/database/` — `QueryMaps`, `QueryCount`, `InClause`, `NamedInClause`, `SqlTime`, type extractors (`Int64FromAny`, `Float64FromAny`, `StringFromAny`, `BoolFromAny`, `TimeFromAny`, nullable variants)
- **Time bucketing**: `internal/infra/timebucket/timebucket.go` — adaptive (1m/5m/15m/1h/1d) + `ByName()` for explicit steps
- **Session**: `internal/infra/session/manager.go` — keys: `auth_user_id`, `auth_email`, `auth_role`, `auth_default_team_id`, `auth_team_ids`
- **Middleware**: `internal/infra/middleware/` — public prefixes: `/api/v1/auth/login`, `/otlp/`, `/health`
- **Dashboard JSON**: `internal/infra/dashboardcfg/`; enums in `enums.go` (24 panel types, 10 layout variants, 8 section templates)
- **Dashboard default pages**: overview (6 tabs), service (1 tab), ai-observability (4 tabs), infrastructure (4 tabs), saturation (3 tabs) — under `internal/infra/dashboardcfg/defaults/`
- **AI modules**: `internal/modules/ai/{dashboard,runs,rundetail,conversations,traces}/` — `/api/v1/ai/*`
- **Overview module**: `internal/modules/overview/{overview,errors,slo}/` — `/api/v1/overview/*`
- **HTTP Metrics**: `internal/modules/httpmetrics/` — `/api/v1/http/*`
- **Infrastructure**: `internal/modules/infrastructure/{cpu,disk,jvm,kubernetes,memory,network,nodes,resourceutil}/` — `/api/v1/infrastructure/*`
- **Saturation DB**: `internal/modules/saturation/database/{collection,connections,errors,latency,slowqueries,summary,system,systems,volume}/` — `/api/v1/saturation/*`
- **Saturation Kafka**: `internal/modules/saturation/kafka/` — `/api/v1/saturation/kafka/*`
- **Deployments API**: `internal/modules/deployments/` — `/api/v1/deployments/*`
- **Logs live tail**: `internal/modules/logs/search/livetail_run.go`, `livetail_payload.go` — Redis Stream subscription, no ClickHouse polling
- **Explorer**: `internal/modules/explorer/analytics/` — `POST /api/v1/explorer/:scope/analytics` (scope: `logs` or `traces`); query parser: `explorer/queryparser/`
- **Traces**: `internal/modules/traces/{query,explorer,tracedetail,redmetrics,errorfingerprint,errortracking,tracecompare,livetail}/` — tracedetail includes `/traces/:traceId/logs` for trace-correlated log retrieval
- **Config**: `internal/config/config.go` (loads `config.yml`; `redis.password` / `redis.db` optional for secured Redis)
- **Sibling repo**: `optic-frontend` (see its `CODEBASE_INDEX.md`)

## Module map (dashboard page → directory)

| Page | Default config directory |
|------|------------------------|
| overview (summary, latency-analysis, apm, errors, http, slo) | `internal/infra/dashboardcfg/defaults/overview/` |
| service (deployments) | `internal/infra/dashboardcfg/defaults/service/` |
| ai-observability (overview, performance, cost, security) | `internal/infra/dashboardcfg/defaults/ai_observability/` |
| infrastructure (resource-utilization, jvm, kubernetes, nodes) | `internal/infra/dashboardcfg/defaults/infrastructure/` |
| saturation (database, queue, redis) | `internal/infra/dashboardcfg/defaults/saturation/` |

## Engineering principles

- **Module Architecture**: Strict 6-file pattern (`handler.go`, `service.go`, `repository.go`, `module.go`, `dto.go`, `models.go`). All repository methods must stay in `repository.go`.
- **SOLID & DRY**: Factor shared behavior when a pattern appears more than once.
- **Quality**: Leave the code clearer or simpler with every change.
- **No unsolicited tests**: Do not add tests unless explicitly asked.
