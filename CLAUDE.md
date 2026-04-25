# Optik Backend — Claude Code Instructions

## Before any task

1. Read **`CODEBASE_INDEX.md`** (repo root) — full map of modules, ingestion, and architecture.
2. Read **`.cursor/rules/optik-backend.mdc`** — hot paths, handler/service/repository patterns, and middleware stack.
3. Read **`.cursor/rules/engineering-workflow.mdc`** — plan before code, two approaches with pros/cons, approval gate.
4. **Do not modify files** until the user approves the plan (except trivial one-line fixes).

## After every iteration

After completing any task — no matter how small — review and update the following if anything changed:

1. **`CODEBASE_INDEX.md`** — new modules, endpoints, helpers, or config sections.
2. **`.cursor/rules/optik-backend.mdc`** — new patterns, conventions, or LLD details.
3. **This file (`CLAUDE.md`)** — new quick-reference paths or principles.

This is **mandatory**. Documentation must always reflect the current architecture.

## Quick reference

- **Stack**: Go 1.25, Gin, ClickHouse, MySQL, Redis, Kafka, OTLP gRPC (ingest surface only — self-telemetry is Prometheus-only).
- **Server entry**: `cmd/server/main.go`
- **Module registration**: `internal/app/server/modules_manifest.go` → `configuredModules()`.
- **Handler helpers**: `internal/shared/httputil/base.go` — `RespondOK`, `RespondErrorWithCause`, `ParseRequiredRange`.
- **Error codes**: `internal/shared/errorcode/codes.go`.
- **ClickHouse helpers**: `internal/infra/database/` — `QueryMaps`, `QueryCount`, `SqlTime`, type extractors.
- **Time bucketing**: `internal/infra/timebucket/timebucket.go` — adaptive (1m/5m/15m/1h/1d).
- **Rollup Selection**: `internal/infra/rollup/tier.go` — `TierTableFor(prefix, startMs, endMs)` for smart table choice.
- **Session**: `internal/infra/session/manager.go`.
- **Middleware**: `internal/infra/middleware/` — public prefixes: `/api/v1/auth/login`, `/otlp/`, `/health`.
- **Ingestion**: `internal/ingestion/{spans,metrics,logs}/` — handler → mapper → kafka producer → dispatcher → per-partition worker → writer (CH batch + retry + DLQ). Shared generics in `internal/infra/kafka_ingest/` (`dispatcher.go`, `worker.go`, `writer.go`, `accumulator.go`, `metrics.go`, `pools.go`, `pipeline_cfg.go`). Tuning knobs live in `internal/config/ingestion.go` → `IngestPipelineConfig` (per-signal YAML overrides).
- **Local monitoring**: `deploy/monitoring/stack/docker-compose.yml` — Prometheus `:19091`, Grafana `:13001`. Dashboards in `deploy/monitoring/grafana/dashboards/`: `optikk_overview`, `optikk_http_api` (per-API drill-down), `optikk_grpc`, `optikk_db`, `optikk_redis`, `optikk_kafka`, `optikk_ingest`. All Prometheus-sourced; there is no OTel collector or Tempo — the `/metrics` endpoint is the only self-telemetry surface.
- **Load test (query-side)**: `make loadtest-smoke` for CI sanity, `make loadtest-all` for the full sweep. k6 scenarios live in `loadtest/scenarios/<module>/`; entrypoints in `loadtest/entrypoints/`. See `loadtest/docs/README.md` for the env-flag table and the Prometheus remote-write setup.
- **Schema migrations**: `db/clickhouse/*.sql` applied via `internal/infra/database_chmigrate`.
- **Traces explorer contract**: `internal/modules/traces/explorer/` reads `observability.traces_index` directly. Keep DB scan structs aligned with ClickHouse unsigned types (`start_ms`, `end_ms`, `duration_ns`, `last_seen_ms`, `root_http_status`) and normalize mixed facet types at the SQL boundary (for example `toString(root_http_status)` in facet queries).

## Engineering principles

- **Module Architecture**: Strict 6-file pattern (`handler.go`, `service.go`, `repository.go`, `module.go`, `dto.go`, `models.go`). All repository methods must stay in `repository.go`.
- **SOLID & DRY**: Factor shared behavior when a pattern appears more than once.
- **Quality**: Leave the code clearer or simpler with every change.
- **No unsolicited tests**: Do not add tests unless explicitly asked.
