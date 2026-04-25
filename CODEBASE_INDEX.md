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

`internal/ingestion/` owns OTLP ingest for spans, logs, and metrics. **All three signals share the same generic pipeline** (`Dispatcher[T] → Worker[T] → Accumulator[T] → Writer[T]` in `internal/infra/kafka/ingest/`): the legacy single-goroutine `metrics/consumer.go` was retired in the ingest refactor round.

Each signal lives under its own subdirectory with a consistent package hierarchy:

```
internal/ingestion/{spans,logs,metrics}/
  ingress/   handler.go, producer.go         — gRPC handler + Kafka publish
  mapper/    mapper.go (+ signal-specific files below)
  consumer/  dispatcher.go, worker.go, writer.go
  dlq/       dlq.go
  enrich/    enrich.go, severity.go          — spans + logs only
  indexer/   assembler.go, emitter.go, state.go  — spans only (trace-assembly)
  schema/    *_row.pb.go, row.go
  module/    module.go
```

Signal-specific mapper extras:
- **spans**: `mapper_attrs.go` (attribute capping via `otlp.TypedAttrs`), `mapper_status.go` (HTTP/error status normalization)
- **metrics**: `mapper_points.go` (gauge/histogram/sum point projection)
- **logs**: single `mapper.go`

Key per-package roles:
- `ingress/handler.go`: gRPC-facing OTLP export handler; instruments `optikk_ingest_{mapper_duration,mapper_rows_per_request,handler_publish_duration}` per request.
- `ingress/producer.go`: Kafka publish with pooled proto `MarshalAppend` scratch buffer (see `kafka/ingest/pools.go`).
- `enrich/enrich.go`: attribute normalization, exception-to-error promotion (spans), severity level resolution (logs + spans).
- `consumer/dispatcher.go`: wraps `ingest.Dispatcher[*Row]`; configures Pause/Resume thresholds from per-signal `IngestPipelineConfig`.
- `consumer/worker.go`: composes `ingest.Worker[*Row]` with the signal's size function and the per-signal accumulator config.
- `consumer/writer.go`: `ingest.Writer[*Row]` with CH batch insert; wraps attempt ctx with `clickhouse.WithSettings(async_insert=1, wait_for_async_insert=1)` when `pipeline.{signal}.async_insert` is true. For spans, also feeds written rows into the Trace Assembler.
- `dlq/dlq.go`: DLQ topic writer with rate-limited warn (10s cooldown) + `optikk_ingest_writer_dlq_publish_failed_total` counter — a dead DLQ broker can't flood stderr at 200k rps.
- `indexer/assembler.go`: stateful in-memory LRU buffer keyed by `(teamID, traceID)`. Emits a `TraceIndexRow` to `observability.traces_index` when root span observed + 10s quiet window elapses, or 60s hard timeout fires. Partial traces marked `truncated=true`.
- `module/module.go`: module registration + `Deps` struct carrying the pipeline config.

Pipeline tuning (YAML under `ingestion.pipeline.{logs,spans,metrics}`): `max_rows`, `max_bytes`, `max_age_ms`, `worker_queue_size`, `pause_depth_ratio`, `resume_depth_ratio`, `writer_{max_attempts,base_backoff_ms,max_backoff_ms,attempt_timeout_ms}`, `async_insert`. Defaults (`config.DefaultIngestPipelineConfig`): 10k rows / 16 MiB / 250ms, 4096 queue, 80%/40% pause/resume, 5-attempt retry 100ms→5s, 30s attempt timeout, async_insert on.

Shared ingest infra in `internal/infra/kafka/ingest/`:

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

- `internal/infra/database/`: MySQL and ClickHouse clients; `{clickhouse,mysql}_instrument.go` + `instrument_common.go` provide the `SelectCH/QueryCH/ExecCH` + `SelectSQL/GetSQL/ExecSQL` seam that every repository uses — each call emits `optikk_db_{queries_total,query_duration_seconds}` labelled by `system` + `op`.
- `internal/infra/kafka/`: broker client, producers, consumers, topic helpers; `ingest/` sub-package contains the shared generic pipeline (`dispatcher.go`, `worker.go`, `accumulator.go`, `writer.go`, `metrics.go`, `pools.go`, `pipeline_cfg.go`).
- `internal/infra/redis/`: Redis client with a go-redis metrics hook feeding `optikk_redis_{commands_total,command_duration_seconds}`.
- `internal/infra/session/`: session persistence and middleware integration
- `internal/infra/middleware/`: recovery, HTTP Prometheus metrics (`metrics.go`), CORS, tenant, body limit, response cache
- `internal/infra/rollup/`: TTL-aware rollup tier selection. Families registered in `families.go`; `For(family, startMs, endMs)` returns a `Tier{Table, StepMin, BucketExpr}`; `BucketInterval(tier, uiStep)` replaces the pre-rewrite duplicated helpers. Tier TTLs: `_1m`=72h, `_5m`=14d, `_1h`=90d. See `db/clickhouse/README.md` for the full family map.
- `internal/infra/cursor/`: cursor helpers for explorer-style APIs
- `internal/infra/utils/`: time-bucketing (`timeutil.go`) and unit-conversion (`conv.go`) helpers shared across modules.

### Local monitoring stack (opt-in)

`deploy/monitoring/` ships a local Prometheus + Grafana pair for ingest-pipeline dashboards:

- **Ports (bespoke — chosen to avoid colliding with the main compose)**: Prometheus `:19091`, Grafana `:13001`.
- **Bring-up**: `docker compose -f deploy/monitoring/stack/docker-compose.yml up -d`.
- **Scrape target**: `host.docker.internal:19090` (the Go backend's `/metrics`, 5s interval).
- **Directory layout** (flat-vs-nested rule — `monitoring/` holds only subdirs):
  - `deploy/monitoring/stack/` — `docker-compose.yml`, `prometheus.yml`.
  - `deploy/monitoring/grafana/dashboards/optikk_ingest.json` — starter dashboard (ingest rate, handler/worker/CH latency, queue depth, paused partitions, flush reasons, DLQ rate, consumer lag, attr drops).
  - `deploy/monitoring/grafana/provisioning/{datasources,dashboards}/*.yml` — auto-registration so no UI setup is needed.

This is the primary monitoring stack for local development — all ingest metrics flow here via Prometheus scrape.

### Observability / monitoring stack

Self-telemetry is Prometheus-only. The customer-facing OTLP receiver on `:4317` is unrelated — it ingests external spans/metrics/logs from clients and has its own pipeline (`internal/ingestion/`).

**Metrics** — `promauto` collectors exposed on `/metrics`, scraped by the local Prometheus + Grafana stack (`deploy/monitoring/stack/`).

- `internal/infra/metrics/` — `http.go`, `grpc.go`, `db.go`, `kafka.go`, `auth.go`, `ingest.go` — all `promauto`-registered collectors.
- `internal/infra/redis/metrics_hook.go` — Redis-specific collectors via a `goredis.Hook`.
- `internal/infra/middleware/metrics.go` — `HTTPMetricsMiddleware` populates `optikk_http_*` (route label = Gin `FullPath()` template, method, status class).
- `internal/app/server/grpc_metrics.go` — unary + stream interceptors populate `optikk_grpc_*` (full method, canonical gRPC code).
- `internal/infra/kafka/observability/observability.go` — franz-go `kgo.Hooks` (produce/fetch/broker/group-error) + `LagPoller` (`optikk_kafka_consumer_lag_records` via raw `kmsg` calls). One `LagPoller.Run(ctx)` per ingest consumer, started by `app.Start`.
- `internal/infra/database/{clickhouse,mysql}_instrument.go` + `instrument_common.go` — `SelectCH/QueryCH/ExecCH` + `SelectSQL/GetSQL/ExecSQL` seam that emits `optikk_db_*` for every query.

**Logs** — `slog.InfoContext/…Context` records fan out through `internal/shared/slogx.FanoutHandler` so additional sinks (file, syslog) can be added without touching call sites. Currently the fanout wraps a single stdout leg (tint for local dev, JSON when `LOG_FORMAT=json`).

**Grafana dashboards** (`deploy/monitoring/grafana/dashboards/`) — provisioned read-only, one file per concern:
- `optikk_overview.json` — service-wide health tiles + top-level timeseries (start here).
- `optikk_http_api.json` — **per-API** HTTP dashboard: QPS/latency/error rate with route + method template variables; top-10 slowest and error-prone routes.
- `optikk_grpc.json` — per-method gRPC dashboard (OTLP ingest surface + any other gRPC).
- `optikk_db.json` — MySQL + ClickHouse query rate/latency/errors split by `system` + `op`.
- `optikk_redis.json` — Redis commands/sec, latency, error ratio (from the metrics hook).
- `optikk_kafka.json` — produce/consume rate, produce latency, consumer lag per partition, rebalances, broker connects.
- `optikk_ingest.json` — OTLP ingest pipeline (records/sec by signal, handler/worker/CH latency, queue depth, paused partitions, DLQ rate, attr drops).

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
- `internal/modules/logs/querycompiler/` — `Compile(Filters, Target)` → `Compiled{Where, Args, DroppedClauses}`. Targets: `TargetRaw` (observability.logs), `TargetRollup` (logs_volume_{1m,5m,1h}), `TargetFacetRollup` (logs_facets_{1m,5m,1h}).

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
- traces: `explorer`, `trace_analytics`, `span_query`, `tracedetail`, `trace_shape`, `trace_paths`, `trace_servicemap`, `trace_suggest`, `errors`, `latency`, `querycompiler`
- logs: `explorer`, `querycompiler`
- metrics
- services: `topology`, `deployments`
- infrastructure: `connpool`, `cpu`, `disk`, `fleet`, `jvm`, `kubernetes`, `memory`, `network`, `nodes`, `resourceutil`, `infraconsts` (shared constants, not a routable module)
- saturation: `kafka`, `database/collection`, `connections`, `errors`, `explorer`, `latency`, `slowqueries`, `summary`, `system`, `systems`, `volume`
- user: `auth`, `team`, `user`
- ingestion: spans, logs, metrics

## Key directories

| Path | Purpose |
|------|---------|
| `cmd/server/` | Main server binary. ClickHouse migrations run automatically on boot before the HTTP/gRPC servers start. |
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

## Load test (query-side)

`loadtest/` is a top-level k6 project parallel to `internal/` that exercises every read endpoint on the running backend. Ingestion-side load generation is out of scope and lives in a separate tool.

```
loadtest/
  lib/         shared helpers — config, bootstrap, auth, client, payloads, summary
  scenarios/   one subdir per backend module (traces, logs, metrics, overview,
               infrastructure, saturation, services). Each .js file owns 1–10
               related endpoints and exports named exec functions.
  entrypoints/ one .js per scope: smoke, all, traces, logs, metrics,
               overview, infrastructure, saturation, services. Each composes
               an options.scenarios{} block from its scenario imports.
  docs/        README.md
```

Bootstrap (`lib/bootstrap.js`) is idempotent: it logs in first, and only
falls through to `POST /api/v1/teams` + `POST /api/v1/users` against
the public auth surface if no user exists. A safety rail blocks the
create flow on non-localhost hosts unless `ALLOW_REMOTE_BOOTSTRAP=1`.

Run via `make loadtest-smoke|loadtest-all|loadtest-<module>`. Output:
stdout summary, live ticker, optional `JSON_OUT=...` results file, and
optional Prometheus remote-write to `:19091/api/v1/write` (the local
stack now ships with `--web.enable-remote-write-receiver`).

## Related docs

- Overview doc: [README.md](/Users/ramantayal/Desktop/pro/optikk-backend/README.md)
- Flow diagrams: [docs/flows/](/Users/ramantayal/Desktop/pro/optikk-backend/docs/flows/) — ingestion, auth, http-request, trace-assembly
- Ingest dashboard: [deploy/monitoring/grafana/dashboards/optikk_ingest.json](/Users/ramantayal/Desktop/pro/optikk-backend/deploy/monitoring/grafana/dashboards/optikk_ingest.json)
- Frontend sibling repo: [../optikk-frontend/README.md](/Users/ramantayal/Desktop/pro/optikk-frontend/README.md)
