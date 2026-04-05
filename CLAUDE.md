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
- **Module registration**: `internal/app/server/modules_manifest.go` → `configuredModules()`
- **Handler helpers**: `internal/shared/httputil/base.go` — `RespondOK`, `RespondErrorWithCause`, `ParseRequiredRange`
- **Error codes**: `internal/shared/contracts/errorcode/codes.go`
- **ClickHouse helpers**: `internal/infra/database/` — `QueryMaps`, `SqlTime`, type extractors
- **Time bucketing**: `internal/infra/timebucket/timebucket.go`
- **Dashboard JSON**: `internal/infra/dashboardcfg/pages/`; default pages also under `internal/infra/dashboardcfg/defaults/` (e.g. `defaults/service/` for deployments UI)
- **Deployments API**: `internal/modules/deployments/` — `/api/v1/deployments/*`
- **Config**: `internal/config/config.go` (loads `config.yml`; `redis.password` / `redis.db` optional for secured Redis)
- **Sibling repo**: `optic-frontend` (see its `CODEBASE_INDEX.md`)

## Engineering principles

- **SOLID & DRY**: Factor shared behavior when a pattern appears more than once.
- **Quality**: Leave the code clearer or simpler with every change.
- **No unsolicited tests**: Do not add tests unless explicitly asked.
