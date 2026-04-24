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

`internal/ingestion/` owns OTLP ingest for spans, logs, and metrics. **All three signals share the same generic pipeline** (`Dispatcher[T] → Worker[T] → Accumulator[T] → Writer[T]` in `internal/infra/kafka_ingest/`): the legacy single-goroutine `metrics/consumer.go` was retired in the ingest refactor round.

Per-signal files (flat directory, shape-symmetric across logs/spans/metrics):

- `handler.go`: gRPC-facing OTLP handler; instruments `optikk_ingest_{mapper_duration,mapper_rows_per_request,handler_publish_duration}` per request.
- `mapper*.go`: OTLP → internal Row; uses shared `otlp.TypedAttrs` (logs) or `otlp.CapStringMap` (spans) for deterministic, sort-stable attribute capping. `time.Now()` is resolved once per request and threaded down.
- `producer.go`: Kafka publish with pooled proto `MarshalAppend` scratch buffer (see `kafka/ingest/pools.go`).
- `dispatcher.go`: wraps `ingest.Dispatcher[*Row]`; configures Pause/Resume thresholds from per-signal `IngestPipelineConfig`.
- `worker.go`: composes `ingest.Worker[*Row]` with the signal's size function and the per-signal accumulator config.
- `writer.go`: `ingest.Writer[*Row]` with CH batch insert; wraps attempt ctx with `clickhouse.WithSettings(async_insert=1, wait_for_async_insert=1)` when `pipeline.{signal}.async_insert` is true.
- `dlq.go`: DLQ topic writer with rate-limited warn (10s cooldown) + `optikk_ingest_writer_dlq_publish_failed_total` counter — a dead DLQ broker can't flood stderr at 200k rps.
- `module.go`: module registration + `Deps` struct carrying the pipeline config.

Pipeline tuning (YAML under `ingestion.pipeline.{logs,spans,metrics}`): `max_rows`, `max_bytes`, `max_age_ms`, `worker_queue_size`, `pause_depth_ratio`, `resume_depth_ratio`, `writer_{max_attempts,base_backoff_ms,max_backoff_ms,attempt_timeout_ms}`, `async_insert`. Defaults (`config.DefaultIngestPipelineConfig`): 10k rows / 16 MiB / 250ms, 4096 queue, 80%/40% pause/resume, 5-attempt retry 100ms→5s, 30s attempt timeout, async_insert on.

Shared ingest infra in `internal/infra/kafka_ingest/`:

- `accumulator.go` — size/bytes/time triggers; `FlushReason` enum (size/bytes/time/stop) feeds the Prometheus `reason` label; `BytesAtFlush()` feeds the `worker_flush_bytes` histogram.
- `worker.go` — one goroutine per (topic, partition); samples `optikk_ingest_worker_queue_depth{signal,partition}` on every tick, emits `worker_flush_{duration,rows,bytes}` on every flush.
- `writer.go` — retry loop + DLQ; emits `writer_{ch_insert_duration,ch_rows_total,retry_attempts_total,dlq_sent_total}`.
- `dispatcher.go` — PollFetches loop; Pause/Resume + `optikk_ingest_worker_paused_partitions{signal}` gauge; opens short-lived `kafka.consume <signal>` span per record with `traceparent` header lookup.
- `metrics.go` — every `promauto`-registered ingest collector lives here (one source of truth).
- `pools.go` — `sync.Pool` helpers for `map[string]{string,float64,bool}` + `[]byte` marshal scratch buffers.
- `pipeline_cfg.go` — projections from `config.IngestPipelineConfig` to `AccumulatorConfig`/`WriterConfig`/`DispatcherOptions` + the `WithAsyncInsert` ctx decorator.

Shared OTLP helpers in `internal/infra/otlp/`:

- `protoconv.go` — `AttrsToMap`, `ResourceFingerprint`, `AnyValueString`, `BytesToHex`.
- `typed_attrs.go` — single-pass `TypedAttrs(kvs, maxStringKeys)` → (str,num,bool,dropped) + `CapStringMap(m, max)` for merged-map signals. Dropped counts fan into `optikk_ingest_mapper_attrs_dropped_total{signal}`.

Current queueing model is Kafka-backed, not Redis-stream-backed. Local development uses Redpanda from [docker-compose.yml](docker-compose.yml).

### Data and platform infrastructure

- `internal/infra/database/`: MySQL and ClickHouse clients, migration helpers; `clickhouse_otel.go` exposes `Traced(ctx, op, stmt)` for wrapping CH reads with OTel spans (used on hot paths: traces.ListTraces, logs.ListLogs — extend as needed).
- `internal/infra/kafka/`: broker client, producers, consumers, topic helpers
- `internal/infra/redis/`: Redis client
- `internal/infra/session/`: session persistence and middleware integration
- `internal/infra/middleware/`: recovery, CORS (allows `traceparent`/`tracestate` for W3C propagation), tenant, body limit, response cache
- `internal/infra/rollup/`: time-range-aware rollup tier selection
- `internal/infra/cursor/`: cursor helpers for explorer-style APIs
- `internal/infra/otel/`: self-telemetry OTel SDK bootstrap (`provider.go` — OTLP gRPC exporter to the monitoring collector at `127.0.0.1:14317`) + gin middleware re-export. Wired in `app.go:New`.

### Local monitoring stack (opt-in)

Side-by-side with Grafana Cloud, `deploy/monitoring/` ships a standalone Prometheus + Grafana pair for ingest-pipeline dashboards:

- **Ports (bespoke — chosen to avoid colliding with the main compose)**: Prometheus `:19091`, Grafana `:13001`.
- **Bring-up**: `docker compose -f deploy/monitoring/stack/docker-compose.yml up -d`.
- **Scrape target**: `host.docker.internal:19090` (the Go backend's `/metrics`, 5s interval).
- **Directory layout** (flat-vs-nested rule — `monitoring/` holds only subdirs):
  - `deploy/monitoring/stack/` — `docker-compose.yml`, `prometheus.yml`.
  - `deploy/monitoring/grafana/dashboards/optikk_ingest.json` — starter dashboard (ingest rate, handler/worker/CH latency, queue depth, paused partitions, flush reasons, DLQ rate, consumer lag, attr drops).
  - `deploy/monitoring/grafana/provisioning/{datasources,dashboards}/*.yml` — auto-registration so no UI setup is needed.

Complements (does not replace) the Grafana Cloud pipeline — both run simultaneously, the local stack gives sub-minute resolution on ingest panels without the Grafana Cloud ingest cost.

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
- `internal/infra/kafka/observability.go` — franz-go `kgo.Hooks` (produce/fetch/broker/group-error) + `LagPoller` (populates `optikk_kafka_consumer_lag_records` via raw `kmsg` protocol calls — Metadata/OffsetFetch/ListOffsets — so no `kadm` top-level dep). Wired through `Hooks()` in `client.go`; one `LagPoller.Run(ctx)` actor per ingest consumer is started by `app.Start`.
- `internal/infra/kafka/tracecontext.go` — W3C `traceparent` inject on produce (`Producer.Produce` + `Producer.PublishBatch`) and extract on consume (`ingest/dispatcher.go` opens a `kafka.consume <signal>` span per record). Keeps producer ↔ consumer trace-linked across the Kafka boundary.
- `internal/infra/database/{clickhouse,mysql}_instrument.go` + `instrument_common.go` — the seam every repository method uses (`SelectCH/QueryCH/ExecCH` + `SelectSQL/GetSQL/ExecSQL`). Replaced the earlier `Traced()` helper (now deleted).
- `internal/infra/redis/client.go` — `redisotel.InstrumentTracing` + `client.AddHook(metricsHook{})`.
- `cmd/server/logger.go` — composes the `TraceAttrHandler(FanoutHandler{stdout, otelBridge})` root.
- `config.yml` → `telemetry.otel` — enablement, endpoint, sample ratio, service name.

See `docs/ops/observability.md` for the full metric catalogue, label conventions, and logging contract.

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
- Observability doc: [docs/ops/observability.md](/Users/ramantayal/Desktop/pro/optikk-backend/docs/ops/observability.md)
- Frontend sibling repo: [../optikk-frontend/README.md](/Users/ramantayal/Desktop/pro/optikk-frontend/README.md)
