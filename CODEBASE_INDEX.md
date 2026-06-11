# Optikk Backend — Codebase Index

Orienting index for [optikk-backend](.). Keep this file compact (< 6 KB) to optimize context window tokens.

## Snapshot & Architecture

- **Stack**: Go, Gin, gRPC, ClickHouse, MySQL, Redis, Kafka/Redpanda
- **Main Server Entry**: [cmd/server/main.go](cmd/server/main.go)
- **App Bootstrap**: [internal/app/server/app.go](internal/app/server/app.go)
- **Modules Manifest**: [internal/app/server/modules_manifest.go](internal/app/server/modules_manifest.go)
- **HTTP Routing**: [internal/app/server/routes.go](internal/app/server/routes.go) (single engine, `/api/v1` group, `TenantMiddleware`)
- **OTLP Ingestion**: [internal/ingestion/](internal/ingestion/) (spans, logs, metrics). Standardized flat 7-file packages. Kafka publish (producer-side batching via franz-go) -> Kafka consume -> ClickHouse write (`async_insert=1, wait_for_async_insert=1`).
- **Database Clients**: [internal/infra/database/](internal/infra/database/) (MySQL + ClickHouse). Budgets: `DashboardCtx` (3s), `OverviewCtx` (15s), `ExplorerCtx` (60s). ClickHouse query cache enabled.
- **Shared Helpers**:
  - [internal/shared/chargs/](internal/shared/chargs/): ClickHouse named-arg builders (`RangeArgs`, `BucketBounds`, `WithMetricNames`).
  - [internal/shared/httputil/](internal/shared/httputil/): Range query wrapper (`HandleRangeQuery`), tenant retrieval, API response wrappers.
  - [internal/infra/timebucket/](internal/infra/timebucket/): Single source of truth for bucket start times (`BucketSeconds = 300`). Snaps display grain (1m, 5m, 1h, 1d) Go-side.

## Key Directory Structure

- [cmd/](cmd/): Entry points.
- [db/](db/): Database migration files ([db/clickhouse/](db/clickhouse/) and [db/mysql/](db/mysql/)).
- [internal/app/](internal/app/): Bootstrap, configuration, and server registry.
- [internal/auth/](internal/auth/): Auth middleware/interceptors.
- [internal/infra/](internal/infra/): Infrastructure clients (database, kafka, redis, token, middleware, timebucket).
- [internal/ingestion/](internal/ingestion/): OTLP spans, logs, and metrics ingestion.
- [internal/modules/](internal/modules/): Feature/Domain modules (alerting, infrastructure, logs, metrics, saturation, services, traces, user).
- [internal/shared/](internal/shared/): Common contracts, error codes, and HTTP helpers.
- [loadtest/](loadtest/): k6 query performance test suites.

## Registered Module Inventory

Verify and wire modules in [modules_manifest.go](internal/app/server/modules_manifest.go).

- **alerting**: CRUD & scheduling in `monitors`, integrations in `notifications`, daemon in `evaluator`, transports in `dispatch`.
- **infrastructure**: saturation and utilization metrics under `cpu`, `fleet`, `hosts`, `memory`, `nodes` (uses `infraconsts`).
- **logs**: querying and summary under `explorer`, `facets`, `logdetail`, `trace_logs`, `trends` (uses shared `filter` and models).
- **metrics**: tag lists and metric exploration under `explorer` (uses shared `filter`).
- **saturation**: overview queries under `database/{explorer, latency, slowqueries, volume}` and `kafka/{consumer, explorer, producer}`.
- **services**: health metrics, routes, and exception topology under `errors`, `redmetrics`, `topology`.
- **traces**: query and analysis under `detail`, `explorer`, `paths`, `servicemap` (uses shared `filter`).
- **user**: authentication (`auth`), tenant team management (`team`), profile management (`user`).
- **ingestion**: OTLP ingest consumers and handlers for `logs`, `metrics`, `spans`.

## Database Schema Registry

### ClickHouse Tables (obs database)
Defined in [db/clickhouse/](db/clickhouse/).
- `spans`: Raw span storage. Rollups in `spans_1m` and `spans_1h`. Errors in `spans_errors_1m`.
- `logs`: Raw log storage. Skip indexes on `log_id` and inverted index `idx_body_text` on body.
- `metrics`: Raw metric data points. Rollup in `metrics_1m`.
- `spans_resource`, `logs_resource`, `metrics_resource`: Resource details, populated via Materialized Views (MVs).
- `trace_index`: Spans-fed mapping of trace ID to ts_bucket/fingerprint (PK optimized for trace lookup).

### MySQL Tables
Defined in [db/mysql/](db/mysql/).
- `teams`, `users`: Authentication & Multi-tenancy metadata.
- `monitors`, `monitor_state`, `monitor_events`: Alerting monitor configs and runtime states.
- `notification_channels`, `notification_templates`, `notification_policies`: Alert routing rules.

## Local Command Reference

Full runbook is defined in the [README.md](README.md) and [CLAUDE.md](CLAUDE.md).
```bash
docker compose up -d           # Start Redpanda, MySQL, ClickHouse, Redis
go run ./cmd/server            # Start the backend server
make test                      # Run Go unit tests
```
