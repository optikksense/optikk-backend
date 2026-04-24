# Optikk Backend — Codebase Index

Orientation for [optikk-backend](/Users/ramantayal/Desktop/pro/optikk-backend). This file is meant to reflect the codebase as it exists now.

## Snapshot

- Stack: Go, Gin, gRPC, ClickHouse, MySQL/MariaDB, Redis, Kafka/Redpanda
- Entry point: [cmd/server/main.go](/Users/ramantayal/Desktop/pro/optikk-backend/cmd/server/main.go)
- App bootstrap: [internal/app/server/app.go](/Users/ramantayal/Desktop/pro/optikk-backend/internal/app/server/app.go)
- Module manifest: [internal/app/server/modules_manifest.go](/Users/ramantayal/Desktop/pro/optikk-backend/internal/app/server/modules_manifest.go)
- HTTP router: [internal/app/server/routes.go](/Users/ramantayal/Desktop/pro/optikk-backend/internal/app/server/routes.go)
- Config source: [config.yml](/Users/ramantayal/Desktop/pro/optikk-backend/config.yml)

## Architecture

### Bootstrap

- `cmd/server/main.go` loads config, sets up logging, creates the app, and starts the HTTP and gRPC servers.
- `internal/app/server/app.go` wires infra, loads configured modules, starts background runners, and manages shutdown.
- `internal/app/server/routes.go` builds the Gin router, health endpoints, `/metrics`, and `/api/v1` route groups.

### Ingestion

`internal/ingestion/` owns OTLP ingest for spans, logs, and metrics.

- `handler.go`: gRPC-facing OTLP handlers
- `mapper*.go`: transform OTLP payloads into internal row models
- `producer.go`: enqueue ingest records to Kafka
- `consumer.go`: background persistence consumers
- `row.go` and generated `*.pb.go`: internal row formats
- `module.go`: module registration

Current queueing model is Kafka-backed, not Redis-stream-backed. Local development uses Redpanda from [docker-compose.yml](/Users/ramantayal/Desktop/pro/optikk-backend/docker-compose.yml).

### Data and platform infrastructure

- `internal/infra/database/`: MySQL and ClickHouse clients, migration helpers; `clickhouse_otel.go` exposes `Traced(ctx, op, stmt)` for wrapping CH reads with OTel spans (used on hot paths: traces.ListTraces, logs.ListLogs — extend as needed).
- `internal/infra/kafka/`: broker client, producers, consumers, topic helpers
- `internal/infra/redis/`: Redis client
- `internal/infra/session/`: session persistence and middleware integration
- `internal/infra/middleware/`: recovery, CORS (allows `traceparent`/`tracestate` for W3C propagation), tenant, body limit, response cache
- `internal/infra/rollup/`: time-range-aware rollup tier selection
- `internal/infra/cursor/`: cursor helpers for explorer-style APIs
- `internal/infra/otel/`: self-telemetry OTel SDK bootstrap (`provider.go` — OTLP gRPC exporter to the monitoring collector at `127.0.0.1:14317`) + gin middleware re-export. Wired in `app.go:New`.

### Observability / monitoring stack

Self-telemetry forwards to **Grafana Cloud** via a thin `otel-collector` running in `docker-compose.yml`. Intentionally NOT routed through the app's own OTLP receiver on `:4317`; monitoring the app is a distinct concern from the customer-facing observability product. Three signals, one collector:

- **Traces** — emitted by `internal/infra/otel` + `otelgin` (HTTP) + `otelgrpc` (gRPC) + `redisotel` (Redis) + the DB instrument seam (`dbutil.{Select,Query,Exec}CH` for ClickHouse and `{Get,Select,Exec}SQL` for MariaDB).
- **Logs** — `slog.InfoContext/…Context` records fan to stdout (tint/JSON) AND the OTel logs bridge via `internal/shared/slogx.{TraceAttrHandler,FanoutHandler}` + `contrib/bridges/otelslog`. `trace_id`/`span_id` injected from ctx.
- **Metrics** — `promauto` collectors under `internal/infra/metrics/` (`http.go/grpc.go/db.go/kafka.go/auth.go/ingest.go`) + Redis-specific collectors in `internal/infra/redis/metrics_hook.go`; exposed on `/metrics`, scraped by the collector's `prometheus` receiver.

Key wiring points:

- `docker-compose.yml` `otel-collector` — host ports `:14317` (OTLP gRPC for backend SDK), `:14318` (OTLP HTTP for browser). Reads creds from `.env` via `env_file`.
- `observability/otel-collector.yaml` — OTLP receivers + prometheus scrape receiver; single `otlphttp/grafana-cloud` exporter with `${env:GRAFANA_CLOUD_OTLP_ENDPOINT}` + `Authorization: ${env:GRAFANA_CLOUD_OTLP_AUTH}`. One pipeline per signal.
- `.env` (gitignored) — `GRAFANA_CLOUD_OTLP_*` creds; shape in `.env.example`.
- `internal/infra/otel/provider.go` — bootstraps both tracer + logger providers + W3C propagator; combined shutdown wired into `app.Start`.
- `internal/infra/middleware/observability.go` — HTTP Prom + per-request access log; wired after `otelgin` in `routes.go`.
- `internal/infra/grpcutil/observability.go` — gRPC unary + stream interceptors (Prom timing + per-RPC log), chained with `auth` interceptors alongside `grpc.StatsHandler(otelgrpc.NewServerHandler())`.
- `internal/infra/kafka/observability.go` — franz-go `kgo.Hooks` (produce/fetch/broker/group-error) + `LagPoller`; wired through `Hooks()` in `client.go`.
- `internal/infra/database/{clickhouse,mysql}_instrument.go` + `instrument_common.go` — the seam every repository method uses (`SelectCH/QueryCH/ExecCH` + `SelectSQL/GetSQL/ExecSQL`). Replaced the earlier `Traced()` helper (now deleted).
- `internal/infra/redis/client.go` — `redisotel.InstrumentTracing` + `client.AddHook(metricsHook{})`.
- `cmd/server/logger.go` — composes the `TraceAttrHandler(FanoutHandler{stdout, otelBridge})` root.
- `config.yml` → `telemetry.otel` — enablement, endpoint, sample ratio, service name.

See `docs/observability.md` for the full metric catalogue, label conventions, and logging contract.

### Traces read path (split into sibling submodules)

The trace read surface used to live in two monoliths (`explorer` + `tracedetail`); it is now split by concern so each submodule owns its own `dto/handler/models/module/repository/service` bundle:

- `internal/modules/traces/explorer/` — core list + single: `POST /api/v1/traces/query`, `GET /api/v1/traces/:traceId`. Reads `observability.traces_index`.
- `internal/modules/traces/trace_analytics/` — `POST /api/v1/traces/analytics` (group-by + aggregations over traces_index).
- `internal/modules/traces/span_query/` — `POST /api/v1/spans/query` (span-level explorer view over `observability.spans`).
- `internal/modules/traces/tracedetail/` — per-span drill-downs: `/traces/:id/span-events`, `/traces/:id/spans/:spanId/attributes`, `/traces/:id/logs`, `/traces/:id/related`, plus the spans list/tree (`/traces/:id/spans`, `/spans/:id/tree`).
- `internal/modules/traces/trace_shape/` — "shape" of a trace: `/traces/:id/flamegraph`, `/traces/:id/span-kind-breakdown`. Self-times are computed on the frontend from the span list returned by `/traces/:id/bundle`; there is no server-side `/traces/:id/span-self-times` endpoint.
- `internal/modules/traces/trace_paths/` — chain analysis: `/traces/:id/critical-path`, `/traces/:id/error-path`.
- `internal/modules/traces/trace_servicemap/` — per-trace aggregates: `/traces/:id/service-map`, `/traces/:id/errors`.
- `internal/modules/traces/trace_suggest/` — DSL autocomplete: `POST /traces/suggest` for field/attribute value completion on the traces query bar.
- `internal/modules/traces/shared/traceidmatch/` — shared ClickHouse predicate (`WhereTraceIDMatchesCH`) so every trace-scoped reader normalizes trace_id identically.

### Logs read path

- `internal/modules/logs/explorer/` — `POST /api/v1/logs/query`, `POST /api/v1/logs/analytics`, `GET /api/v1/logs/:id`. `RouteTarget = Cached` so reads pass through the Redis response-cache + the ClickHouse query-cache layer (via the `Explorer` query-budget context). `Query` fans `summary / facets / trend` plus the base list fetch out in parallel via `errgroup.Group`; response = max of all branches, not sum.
- `internal/modules/logs/explorer/repository.go` — `ListLogs` + `GetByID` use `PREWHERE team_id = @teamID AND ts_bucket_start BETWEEN …` so partition pruning runs before the rest of the predicates. `repo_facets.go` unions 5 rollup legs — each leg has the same `PREWHERE` on `(team_id, bucket_ts)` as the lead.
- `internal/modules/logs/querycompiler/` — `Compile(Filters, Target)` → `Compiled{Where, Args, DroppedClauses}`. Targets: `TargetRaw` (observability.logs), `TargetRollup` (logs_rollup_{1m,5m,1h}), `TargetFacetRollup` (logs_facets_rollup_5m).

### Data Type Consistency

To maintain a clean and predictable codebase, follow these type-alignment rules:

- **Integers**: Use `int64` as the default type for all counts, IDs, and metrics in domain models and API responses. This avoids unsigned underflow bugs and aligns with ~90% of the existing code.
- **ClickHouse Scanning**:
  - **Unsigned Parity**: The `clickhouse-go/v2` driver is strict. To scan a `UInt64` (from `count`, `sum`, etc.) into an `int64` Go field, you **must** use `toInt64(...)` in the SQL query.
  - **Timestamps**: If the Go field is a `string`, use `formatDateTime(...)` in SQL (via the `timebucket.ExprForColumn` helper). If the Go field is a `time.Time`, use native `DateTime` (via the `timebucket.ExprForColumnTime` helper).
  - **Facet Normalization**: Mixed-type facet output should be normalized in SQL before scanning, such as casting `root_http_status` with `toString(...)` for shared facet bucket DTOs.

### Module shape

Most feature modules still follow the familiar package split:

- `module.go`
- `handler.go`
- `service.go`
- `repository.go`
- `dto.go` where needed
- `models.go`

Not every package uses the exact same six-file layout, so treat the manifest and package contents as the source of truth.

## Request surfaces

### HTTP

- Health: `/health`, `/health/live`, `/health/ready`
- Prometheus metrics: `/metrics`
- Product APIs: `/api/v1/...`

`/api/v1` has two route groups:

- uncached routes with tenant middleware
- cached routes with tenant middleware plus Redis-backed response caching

Modules choose the target group through the registry contract.

### gRPC

- OTLP gRPC server listens on `otlp.grpc_port`
- auth is enforced through gRPC interceptors in `internal/auth`

## Current module inventory

From the live module manifest:

- overview: `overview`, `redmetrics`, `httpmetrics`, `errors`, `slo`, `apm`
- traces: `explorer`, `trace_analytics`, `span_query`, `tracedetail`, `trace_shape`, `trace_paths`, `trace_servicemap`, `trace_suggest`, `errors`, `latency`
- logs: `search`, `explorer`
- metrics
- services: `topology`, `deployments`
- infrastructure: `connpool`, `cpu`, `disk`, `fleet`, `jvm`, `kubernetes`, `memory`, `network`, `nodes`, `resourceutil`
- saturation: `kafka`, `database/collection`, `connections`, `errors`, `explorer`, `latency`, `slowqueries`, `summary`, `system`, `systems`, `volume`
- user: `auth`, `team`, `user`
- ingestion: spans, logs, metrics

## Key directories

| Path | Purpose |
|------|---------|
| `cmd/server/` | Main server binary |
| `cmd/migrate/` | ClickHouse migration entrypoint |
| `db/clickhouse/` | ClickHouse schema and migration files |
| `internal/app/` | App composition and registry |
| `internal/auth/` | HTTP/gRPC auth helpers |
| `internal/config/` | Config structs, defaults, validation |
| `internal/infra/` | Cross-cutting infra packages |
| `internal/ingestion/` | OTLP ingest pipeline |
| `internal/modules/` | Product/domain APIs |
| `internal/shared/` | Shared contracts and helpers |
| `docs/` | Supporting design and ops docs |

## Local runbook

Start local dependencies:

```bash
docker compose up -d
```

Run the backend:

```bash
go run ./cmd/server
```

Useful commands:

```bash
make run
make build
make vet
go test ./...
```

## Related docs

- Overview doc: [README.md](/Users/ramantayal/Desktop/pro/optikk-backend/README.md)
- Observability doc: [docs/observability.md](/Users/ramantayal/Desktop/pro/optikk-backend/docs/observability.md)
- Frontend sibling repo: [../optikk-frontend/README.md](/Users/ramantayal/Desktop/pro/optikk-frontend/README.md)
