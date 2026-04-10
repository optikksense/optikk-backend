---
description: Optik backend — development standards, architecture, and LLD patterns
alwaysApply: true
---

# Optik Backend — Engineering Skill

This skill defines the development standards and architectural patterns for the **optikk-backend** repository. As an AI assistant, you must adhere to these guidelines for all tasks within this workspace.

## Session Workflow (Mandatory)

1. **Read [CODEBASE_INDEX.md](../../CODEBASE_INDEX.md)** at the repository root. This provides the full map of modules, ingestion, and dashboard configuration.
2. **Review [Active Rules](../../.cursor/rules/optik-backend.mdc)** for latest hot paths.
3. **Read [Agent Philosophy](./philosophy/)**: Review strategic alignment, ADRs, and extensibility rules for Staff-level context.
4. **Plan First, Code After Approval**:
   - For non-trivial work, produce a plan.
   - **At least two viable approaches** must be presented with Pros/Cons.
   - Do **not** modify project files until the user explicitly approves the plan.
4. **After every iteration (mandatory)**: Review and update if anything changed:
   - `CODEBASE_INDEX.md` — new modules, endpoints, helpers, config, cross-repo contracts
   - `.cursor/rules/optik-backend.mdc` — new patterns, conventions, LLD details
   - This file (`.agent/SKILL.md`) and `CLAUDE.md` — keep aligned
   
   Documentation must always reflect current architecture. Always check, even for small changes.

## Modular Monolith Patterns

- **Module Registration**: New modules **must** be registered in **`internal/app/server/modules_manifest.go`** within the `configuredModules()` function (currently 52 constructors: 48 HTTP + 4 ingestion; `alerting` is both an HTTP module and a `BackgroundRunner`).
- **Dependency Injection**: Use the shared **`registry.Module`** pattern.
- **Real-time Ingestion**: Spans, logs, and metrics are handled via the OTLP pipeline in `internal/ingestion/otlp/`.
- **Dashboard Configuration**: 
  - Backend-authored pages live in `internal/infra/dashboardcfg/defaults/`.
  - 3 backend default pages: **overview** (6 tabs), **infrastructure** (4 tabs), **saturation** (3 tabs). The **service** page (`/service`) is fully frontend-owned (Discovery + Topology).
  - Coordinate schema updates with the frontend.

## Engineering Principles

- **SOLID & DRY**: Factor shared behavior when a pattern appears more than once.
- **Quality Improvement**: Leave the code clearer or simpler with every change.
- **No Unsolicited Tests**: Do not add or expand test cases unless explicitly asked by the user.

## Canonical Paths

- **Server Entry**: `cmd/server/main.go`
- **Application Configuration**: `internal/config/config.go`
- **WebSocket Live Tail**: `internal/modules/livetail/` (`GET /api/v1/ws/live`)
- **Logs Live Tail**: `internal/modules/logs/search/livetail_run.go`, `livetail_payload.go`
- **HTTP Helpers**: `internal/shared/httputil/base.go`
- **Error Codes**: `internal/shared/contracts/errorcode/codes.go`
- **API Response**: `internal/shared/contracts/response.go`
- **ClickHouse Helpers**: `internal/infra/database/` (`query.go`, `utils.go`)
- **Time Bucketing**: `internal/infra/timebucket/timebucket.go`
- **Middleware**: `internal/infra/middleware/`
- **Session**: `internal/infra/session/manager.go`
- **AI Modules**: `internal/modules/ai/{dashboard,runs,rundetail,conversations,traces}/`
- **HTTP Metrics**: `internal/modules/httpmetrics/`
- **Infrastructure**: `internal/modules/infrastructure/{cpu,disk,jvm,kubernetes,memory,network,nodes,resourceutil}/`
- **Saturation DB**: `internal/modules/saturation/database/{collection,connections,errors,latency,slowqueries,summary,system,systems,volume}/`
- **Saturation Kafka**: `internal/modules/saturation/kafka/`
- **Alerting**: `internal/modules/alerting/` (subpackages `evaluators/`, `channels/`) — `/api/v1/alerts/*`. Datadog-grade monitors with evaluator loop (`EvaluatorLoop`) + `Dispatcher` wired as a combined `Module`+`BackgroundRunner`. Storage: `observability.alerts` (MySQL, JSON-inline instances/silences), `observability.alert_events` (ClickHouse, append-only transitions/audit).
- **Dashboard Enums**: `internal/infra/dashboardcfg/enums.go` (24 panel types, 10 layout variants, 8 section templates)

## Low-Level Design Patterns

### Handler Pattern

```go
type Handler struct {
    GetTenant httputil.GetTenantFunc
    Service   Service // interface
}

func (h *Handler) ListItems(c *gin.Context) {
    teamID := h.GetTenant(c).TeamID
    startMs, endMs, ok := httputil.ParseRequiredRange(c)
    if !ok { return }

    items, err := h.Service.ListItems(c.Request.Context(), teamID, startMs, endMs)
    if err != nil {
        httputil.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "failed to list items", err)
        return
    }
    httputil.RespondOK(c, items)
}
```

### Service / Repository Separation

- Service = interface + concrete struct (business logic)
- Repository = interface + concrete struct (raw queries)
- Chain: `NewService(NewRepository(nativeQuerier))`

### Module Registration

- `module.go` → `NewModule(nativeQuerier, getTenant) registry.Module`
- Implements: `Name()`, `RouteTarget()`, `RegisterRoutes(group)`
- Register in `modules_manifest.go:configuredModules()`

### ClickHouse Helpers

**Query execution** (`internal/infra/database/query.go`):
- `dbutil.QueryMaps(querier, sql, args...)` → `[]map[string]any` (10K limit)
- `dbutil.QueryMapsLimit(querier, limit, sql, args...)` — custom limit
- `dbutil.QueryMap(querier, sql, args...)` — single row
- `dbutil.QueryCount(querier, sql, args...)` — COUNT → int64
- `dbutil.InClause([]string)` / `InClauseInt64([]int64)` — positional `(?,?,?)`
- `dbutil.NamedInClause(prefix, []string)` / `NamedInClauseInt64(prefix, []int64)` — named `(@p0,@p1)`

**Type-safe extraction** (`internal/infra/database/utils.go`):
- `dbutil.SqlTime(ms)` → `time.Time` UTC
- `dbutil.Int64FromAny`, `Float64FromAny`, `StringFromAny`, `BoolFromAny`, `TimeFromAny`
- Nullable: `NullableStringFromAny` → `*string`, `NullableFloat64FromAny` → `*float64`, `NullableTimeFromAny` → `*time.Time`
- `NormalizeRows(rows)` — sanitize NaN/Inf for JSON
- `ToInt64Slice(v)` — `[]any` → `[]int64`

**Time bucketing** (`internal/infra/timebucket/`):
- `timebucket.NewAdaptiveStrategy(startMs, endMs)` → auto-bucket (minute/5min/hour/day)
- `.GetBucketExpression()` — SQL for SELECT + GROUP BY

### Middleware Stack (order)

1. `gin.Default()` → 2. `ErrorRecovery()` → 3. `CORSMiddleware` → 4. `TenantMiddleware` → 5. Rate limiter (2000 req/s) → 6. Optional `CacheResponse` (30s)

Public prefixes (skip auth): `/api/v1/auth/login`, `/otlp/`, `/health`

### API Response Envelope

- `contracts.Success(data)` / `contracts.Failure(code, msg, path)`
- Error codes: `BadRequest`, `Unauthorized`, `NotFound`, `Internal`, `QueryFailed`, `QueryTimeout`, `CircuitOpen`, `RateLimited`, `Validation`, `Forbidden`, `ConnectionError`, `Unavailable`
