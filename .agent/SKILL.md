---
description: Optik backend — development standards, architecture, and LLD patterns
alwaysApply: true
---

# Optik Backend — Engineering Skill

This skill defines the development standards and architectural patterns for the **optikk-backend** repository. As an AI assistant, you must adhere to these guidelines for all tasks within this workspace.

## Session Workflow (Mandatory)

1. **Read [CODEBASE_INDEX.md](../../CODEBASE_INDEX.md)** at the repository root. This provides the full map of modules, ingestion, and dashboard configuration.
2. **Review [Active Rules](../../.cursor/rules/optik-backend.mdc)** for latest hot paths.
3. **Plan First, Code After Approval**:
   - For non-trivial work, produce a plan.
   - **At least two viable approaches** m
   'ust be presented with Pros/Cons.
   - Do **not** modify project files until the user explicitly approves the plan.
4. **After every iteration (mandatory)**: Review and update if anything changed:
   - `CODEBASE_INDEX.md` — new modules, endpoints, helpers, config, cross-repo contracts
   - `.cursor/rules/optik-backend.mdc` — new patterns, conventions, LLD details
   - This file (`.agent/SKILL.md`) and `CLAUDE.md` — keep aligned
   
   Documentation must always reflect current architecture. Always check, even for small changes.

## Modular Monolith Patterns

- **Module Registration**: New modules **must** be registered in **`internal/app/server/modules_manifest.go`** within the `configuredModules()` function.
- **Dependency Injection**: Use the shared **`registry.Module`** pattern.
- **Real-time Ingestion**: Spans, logs, and metrics are handled via the OTLP pipeline in `internal/ingestion/otlp/`.
- **Dashboard Configuration**: 
  - Backend-authored pages live in `internal/infra/dashboardcfg/pages/`.
  - Coordinate schema updates with the frontend.

## Engineering Principles

- **SOLID & DRY**: Factor shared behavior when a pattern appears more than once.
- **Quality Improvement**: Leave the code clearer or simpler with every change.
- **No Unsolicited Tests**: Do not add or expand test cases unless explicitly asked by the user.

## Canonical Paths

- **Server Entry**: `cmd/server/main.go`
- **Application Configuration**: `internal/config/config.go`
- **Socket.IO Real-time Subscriptions**: `internal/infra/livetailws/ (GET /api/v1/ws/live)`
- **HTTP Helpers**: `internal/shared/httputil/base.go`
- **Error Codes**: `internal/shared/contracts/errorcode/codes.go`
- **API Response**: `internal/shared/contracts/response.go`
- **ClickHouse Helpers**: `internal/infra/database/` (`query.go`, `utils.go`)
- **Time Bucketing**: `internal/infra/timebucket/timebucket.go`
- **Middleware**: `internal/infra/middleware/`

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

- `dbutil.QueryMaps(querier, sql, args...)` → `[]map[string]any`
- `dbutil.SqlTime(ms)` → `time.Time` UTC
- `dbutil.Int64FromAny`, `Float64FromAny`, `StringFromAny` — type-safe extraction
- `timebucket.NewAdaptiveStrategy(startMs, endMs)` → auto-bucket

### Middleware Stack (order)

1. `gin.Default()` → 2. `ErrorRecovery()` → 3. `CORSMiddleware` → 4. `TenantMiddleware` → 5. Rate limiter (2000 req/s) → 6. Optional `CacheResponse` (30s)

### API Response Envelope

- `contracts.Success(data)` / `contracts.Failure(code, msg, path)`
- Error codes: `BadRequest`, `Unauthorized`, `NotFound`, `Internal`, `QueryFailed`, `QueryTimeout`, `CircuitOpen`, etc.
