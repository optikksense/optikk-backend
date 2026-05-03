# Optikk Backend — Codebase Index

Orientation for [optikk-backend](.). This file reflects the codebase as it exists now. Update it after every iteration.

## Snapshot

- Stack: Go, Gin, gRPC, ClickHouse, MySQL, Redis, Kafka/Redpanda
- Entry point: [cmd/server/main.go](cmd/server/main.go)
- App bootstrap: [internal/app/server/app.go](internal/app/server/app.go)
- Module manifest: [internal/app/server/modules_manifest.go](internal/app/server/modules_manifest.go)
- HTTP router: [internal/app/server/routes.go](internal/app/server/routes.go)
- Config source: [config.yml](config.yml)

## Architecture

### Bootstrap

- [cmd/server/main.go](cmd/server/main.go) loads config, sets up signal handlers (SIGINT, SIGTERM), creates the app via `server.New`, and calls `app.Start(ctx)`.
- [internal/app/server/app.go](internal/app/server/app.go) wires `Infra` (`newInfra`), constructs the module list (`configuredModules`), then `Start` runs an `oklog/run.Group` with four actor types: a context-cancel actor, the HTTP server, the gRPC server, and one Kafka consumer-lag poller per ingest consumer. Background modules (anything implementing `registry.BackgroundRunner`, currently the three ingestion consumers) are started before the run group and stopped after. ClickHouse migrations are applied during `newInfra` before the servers come up.
- [internal/app/server/routes.go](internal/app/server/routes.go) builds a single Gin engine: `ErrorRecovery` → `HTTPMetricsMiddleware` → `CORS` → `BodyLimit(10 MiB)` → `gzip` (excluding `/metrics`). Health routes (`/health`, `/health/live`, `/health/ready`), Prometheus `/metrics`, and a single `/api/v1` group with `TenantMiddleware` — all modules register under that one group; **there is no separate cached/uncached split** and **no response-cache middleware**.
- gRPC server (`addGRPCServerActor`) listens on `otlp.grpc_port` with `MaxConcurrentStreams(10_000)`, 30s `ConnectionTimeout`, keepalive params, and chained interceptors `grpcMetricsUnary/Stream` → `auth.UnaryInterceptor/StreamInterceptor`. Modules implementing `registry.GRPCRegistrar` (the three ingestion handlers) register their services here.

### Ingestion

`internal/ingestion/` owns OTLP ingest for spans, logs, and metrics. The pipeline is intentionally minimal — **no app-side accumulator, no per-partition workers, no Dispatcher/Worker generics, no pause/resume backpressure, no retry loop**.

```
gRPC  →  otlp_proto  →  project_proto  →  kafka publish  →  kafka consume  →  CH write
```

Producer-side batching is delegated to franz-go (`ProducerLinger` + `ProducerBatchMaxBytes`); consumer batching is whatever a single `PollFetches` returns; ClickHouse `async_insert=1, wait_for_async_insert=1` provides server-side coalescing on top.

Each signal is a flat 7-file Go package — no `ingress/`, `mapper/`, `enrich/`, `consumer/`, `dlq/`, `module/` subdirectories. The only subdir is `schema/` which holds the protoc-generated proto + pb.go.

```
internal/ingestion/{spans,logs,metrics}/
  schema/         row.proto + row.pb.go (generated wire format)
  handler.go      gRPC OTLP Export — auth, mapper, producer.Publish
  mapper.go       OTLP → []*schema.Row
  producer.go     ProducerConfig + Producer
  consumer.go     ConsumerConfig + Consumer (decodes records → writer)
  writer.go       chTable + chColumns + chValues + Insert (CH async_insert=1, wait=1)
  dlq.go          DLQ — republishes original record bytes to optikk.dlq.{signal}
  module.go       registry.Module wiring (RegisterGRPC + Start/Stop the consumer goroutine)
```

**Hot path (gRPC → Kafka)**: handler resolves teamID from ctx → mapper walks ResourceX → ScopeX → record level → produces `[]*schema.Row` (resource fingerprint via [internal/infra/fingerprint](internal/infra/fingerprint), attribute caps via [internal/infra/otlp](internal/infra/otlp) `TypedAttrs` / `CapStringMap`, hot-attribute promotion to dedicated CH columns) → `producer.Publish(ctx, rows)` marshals every row to proto, builds N `*kgo.Record` (key = teamID, topic = `optikk.ingest.{signal}`), submits as one async batch, waits for all acks via `kafka.Producer.PublishBatch`. Failure surfaces as `codes.Unavailable`.

**Cold path (Kafka → ClickHouse)**: `consumer.Run` delegates to `kafka.Consumer.Run(ctx, handle)` which `PollFetches` in a loop. Decode each `*kgo.Record` to `*schema.Row` (malformed records are logged once and dropped; they can't be replayed usefully), pass the slice to `writer.Insert`. Writer prepares one CH batch, appends N rows, sends under `clickhouse.WithSettings(async_insert=1, wait_for_async_insert=1)`. **On any failure** (`prepare`/`append`/`send`) the consumer fires `dlq.PublishAll(ctx, recs, err)` which republishes the original `kgo.Record.Value` bytes verbatim to `optikk.dlq.{signal}` (with `x-dlq-reason` + `x-dlq-signal` headers), then returns nil so offsets commit and the partition keeps moving.

**Shared kafka primitives** (`internal/infra/kafka/`, flat — no subpackages):
- `client.go` — `Config{Brokers, LingerMs, BatchMaxBytes, Compression}`, `NewProducerClient`, `NewConsumerClient`. Producer opts: `ProducerLinger` (default 20ms), `ProducerBatchMaxBytes` (default 1 MiB), `MaxBufferedRecords(1<<18)`, `RequiredAcks(AllISRAcks)`, `StickyKeyPartitioner`, ZSTD compression. Consumer opts: `ConsumerGroup`, `ConsumeTopics`, `DisableAutoCommit`, `CooperativeStickyBalancer`, `FetchMaxWait(2s)`.
- `producer.go` — `Producer{client *kgo.Client}`. `PublishBatch` (async fan-out + WaitGroup, first-error returned), `PublishSync`, `Flush`, `Close`, `Client`.
- `consumer.go` — `Consumer{client *kgo.Client}`. `Run(ctx, RecordHandler)`: PollFetches → drain → call handler → on nil error CommitRecords; on error log and leave uncommitted (handler does its own DLQ-and-return-nil for at-least-once semantics).
- `topics.go` — `TopicSpec{Name, Partitions, Replicas, RetentionHours}` + `EnsureTopics(ctx, brokers, specs)`. Idempotent `kadm.CreateTopics`. Called once at app boot for every signal + DLQ topic.
- `observability.go` — kgo `Hooks()` (produce/fetch counter, broker-connect, group-manage-error) + `LagPoller` (15s ticker, `optikk_kafka_consumer_lag_records` via raw `kmsg` MetadataRequest + OffsetFetchRequest + ListOffsetsRequest). One `LagPoller` per ingest consumer, started by `app.Start` as run.Group actors.

**Topology config** ([internal/config/ingestion.go](internal/config/ingestion.go)): `IngestionConfig{Spans, Logs, Metrics SignalConfig}`; `SignalConfig{Partitions, Replicas, RetentionHours, ConsumerGroup}`. Topic names derive from `kafka.topic_prefix` (default `optikk.ingest`) + signal; DLQ from `kafka.dlq_prefix` (default `optikk.dlq`) + signal.

Shared OTLP helpers in [internal/infra/otlp/](internal/infra/otlp/): `protoconv.go` — `AttrsToMap`, `ResourceFingerprint`, `AnyValueString`, `BytesToHex`. `typed_attrs.go` — single-pass `TypedAttrs(kvs, maxStringKeys)` → (str,num,bool,dropped) + `CapStringMap(m, max)`. Dropped counts fan into `optikk_ingest_mapper_attrs_dropped_total{signal}`.

Local development uses Redpanda from [docker-compose.yml](docker-compose.yml).

### Data and platform infrastructure

| Path | Purpose |
|------|---------|
| [internal/infra/database/](internal/infra/database/) | MySQL + ClickHouse clients. `clickhouse.go` exposes three budget contexts: `DashboardCtx` (3s execution, 1 GiB memory, priority 1), `OverviewCtx` (15s, 2 GiB, priority 5), `ExplorerCtx` (60s, 8 GiB, priority 10). **All three budgets enable CH per-shard query cache** with 60s TTL and per-user isolation, so identical queries within the TTL window return without re-execution. `clickhouse_instrument.go` / `mysql_instrument.go` / `instrument_common.go` provide `SelectCH/QueryCH/ExecCH` + `SelectSQL/GetSQL/ExecSQL` seams that emit `optikk_db_{queries_total,query_duration_seconds}` per call. `migrate.go` + `migrate_chmigrate.go` apply schema during boot. |
| [internal/infra/kafka/](internal/infra/kafka/) | Flat package: `client.go`, `producer.go`, `consumer.go`, `topics.go`, `observability.go`. franz-go-based; one shared producer client across all signals, one consumer client per signal. |
| [internal/infra/redis/](internal/infra/redis/) | go-redis client + metrics hook (`metrics_hook.go` → `optikk_redis_{commands_total,command_duration_seconds}`). |
| [internal/infra/session/](internal/infra/session/) | Session manager + auth-state plumbing. Mounted via `Wrap(handler)` in `addHTTPServerActor`. |
| [internal/infra/middleware/](internal/infra/middleware/) | Two files only: `metrics.go` (`HTTPMetricsMiddleware`, route-templated metrics) and `middleware.go` (`CORSMiddleware`, `ErrorRecovery`, `BodyLimitMiddleware`, `TenantMiddleware`, `RequireRole`, `GetTenant`, public-prefix list `/api/v1/auth/login`, `/otlp/`, `/health` + public-POST list `/api/v1/auth/forgot-password`, `/api/v1/users`, `/api/v1/teams`). |
| [internal/infra/timebucket/](internal/infra/timebucket/) | Single source of truth for bucket math. `BucketSeconds = 300` (5-minute grain) — `ts_bucket UInt32 = (unixSeconds / 300) * 300`. `DisplayBucket` / `DisplayGrain` window-adaptive: ≤3h → 1m, ≤24h → 5m, ≤7d → 1h, else 1d. ClickHouse never computes a bucket itself in any reader SQL. |
| [internal/infra/cursor/](internal/infra/cursor/) | Cursor encode/decode helpers for explorer-style keyset pagination. |
| [internal/infra/utils/](internal/infra/utils/) | `conv.go` — unit-conversion helpers (e.g. `SanitizeFloat` for NaN scrub). |
| [internal/infra/metrics/](internal/infra/metrics/) | `promauto`-registered Prometheus collectors: `http.go`, `grpc.go`, `db.go`, `kafka.go`, `auth.go`, `ingest.go`. |
| [internal/infra/otlp/](internal/infra/otlp/) | `protoconv.go`, `typed_attrs.go` — OTLP proto→Go helpers shared by all three ingest signals. |
| [internal/infra/fingerprint/](internal/infra/fingerprint/) | Resource fingerprint computation used by ingest mappers. |
| [internal/shared/quantile/](internal/shared/quantile/) | `FromHistogram(buckets, counts, q)` — linear interpolation from fixed-bucket histograms. |
| [internal/shared/displaybucket/](internal/shared/displaybucket/) | Generic time-bucket folds: `SumByTime`, `AvgByTime`, `SumByTimeAndKey`, `AvgByTimeAndKey`. |
| [internal/shared/httputil/](internal/shared/httputil/) | `RespondOK`, `RespondError`, `RespondErrorWithCause`, `ParseRequiredRange`, `ParseRange`, `ParseComparisonRange`, `WithComparison`, `ExtractIDParam`, `MaxPageSize=200`, `GetTenantFunc`, `DBTenant`. |
| [internal/shared/errorcode/](internal/shared/errorcode/) | Stable error-code constants returned in API error responses. |
| [internal/shared/contracts/](internal/shared/contracts/) | `context.go` (TenantContext type) + `response.go` (response envelopes). |

### Local monitoring stack (opt-in)

[monitoring/](monitoring/) ships a local Prometheus + Grafana pair for ingest-pipeline dashboards.

- **Ports** (chosen to avoid colliding with the main compose): Prometheus `:19091`, Grafana `:13001`.
- **Bring-up**: `docker compose -f monitoring/stack/docker-compose.yml up -d`.
- **Scrape target**: `host.docker.internal:19090` (the Go backend's `/metrics`, 5s interval).
- **Layout** (`monitoring/` holds only subdirs):
  - `monitoring/stack/` — `docker-compose.yml`, `prometheus.yml`.
  - `monitoring/grafana/dashboards/` — `optikk_overview`, `optikk_http_api`, `optikk_grpc`, `optikk_db`, `optikk_redis`, `optikk_kafka`, `optikk_ingest`.
  - `monitoring/grafana/provisioning/{datasources,dashboards}/*.yml` — auto-registration so no UI setup is needed.

### Observability / monitoring stack

Self-telemetry is Prometheus-only. The customer-facing OTLP receiver on `:4317` is unrelated — it ingests external spans/metrics/logs from clients and has its own pipeline (`internal/ingestion/`).

- **Metrics**: `promauto` collectors on `/metrics`. `internal/infra/middleware/metrics.go` populates `optikk_http_*` (route label = Gin `FullPath()` template). `internal/app/server/grpc_metrics.go` populates `optikk_grpc_*`. `internal/infra/kafka/observability.go` provides franz-go `kgo.Hooks` + `LagPoller`. `internal/infra/database/{clickhouse,mysql}_instrument.go` emits `optikk_db_*`.
- **Logs**: `slog.InfoContext/…Context` directly — no fanout handler, no extra sinks. Tint formatter for local dev, JSON when `LOG_FORMAT=json`.

### ClickHouse schema

Eight migration files in [db/clickhouse/](db/clickhouse/), applied in lexical order at boot. `06_*.sql` is intentionally absent. See [db/clickhouse/README.md](db/clickhouse/README.md) for the file-by-file table.

| Table | PK | Partition | TTL | MV in / out |
|---|---|---|---|---|
| `observability.spans` ([01_spans.sql](db/clickhouse/01_spans.sql)) | `(team_id, ts_bucket, fingerprint, service, name, timestamp, trace_id, span_id)` | `(toYYYYMMDD(timestamp), toHour(timestamp))` | 30 d | source; `spans_to_spans_resource` MV out, `spans_to_deployments` MV out |
| `observability.logs` ([02_logs.sql](db/clickhouse/02_logs.sql)) | `(team_id, ts_bucket, fingerprint, timestamp)` | `(toYYYYMMDD(timestamp), toHour(timestamp))` | 30 d | source; `logs_to_logs_resource` MV out, `logs_to_trace_index` MV out. `log_id String` column + `idx_log_id` bloom-filter for deep-link lookup. `idx_body_text` native text (inverted) index on `body` (CH 26.2 GA, `tokenizer='splitByNonAlpha'` + `preprocessor='lowerUTF8(str)'`) accelerates `hasToken` / `ILIKE` search |
| `observability.metrics` ([03_metrics.sql](db/clickhouse/03_metrics.sql)) | `(team_id, ts_bucket, fingerprint, metric_name, timestamp)` | `(toYYYYMMDD(timestamp), toHour(timestamp))` | 30 d | source; `metrics_to_metrics_resource` MV out, `metrics_1m_mv` MV out |
| `observability.spans_resource` ([04_resources.sql](db/clickhouse/04_resources.sql)) | `(team_id, ts_bucket, fingerprint)` | `toYYYYMMDD(toDateTime(ts_bucket)) × hour` | 90 d | MV from `spans` |
| `observability.logs_resource` ([04_resources.sql](db/clickhouse/04_resources.sql)) | `(team_id, ts_bucket, fingerprint)` | `toYYYYMMDD(toDateTime(ts_bucket))` | 90 d | MV from `logs` |
| `observability.metrics_resource` ([04_resources.sql](db/clickhouse/04_resources.sql)) | `(team_id, ts_bucket, metric_name, fingerprint)` | `toYYYYMMDD(toDateTime(ts_bucket))` | 90 d | MV from `metrics` |
| `observability.trace_index` ([08_trace_index.sql](db/clickhouse/08_trace_index.sql)) | `(trace_id, team_id, ts_bucket, fingerprint, timestamp, span_id)` | `toYYYYMMDD(toDateTime(ts_bucket))` | 30 d | `spans_to_trace_index` MV from `spans` (spans-fed); reverse-key projection used by both [trace_logs](internal/modules/logs/trace_logs/) and [traces/explorer.GetByID](internal/modules/traces/explorer/) |
| `observability.deployments` ([05_deployments.sql](db/clickhouse/05_deployments.sql)) | `(team_id, service, service_version, environment)` | `toYYYYMM(first_seen)` | 180 d | MV from `spans` (root spans w/ `vcs.*` attrs) |
| `observability.metrics_1m` ([07_metrics_1m.sql](db/clickhouse/07_metrics_1m.sql)) | `(team_id, ts_bucket, metric_name, fingerprint, attr_hash, timestamp)` | `toYYYYMMDD(timestamp)` | 90 d | `metrics_1m_mv` from `metrics` |
| `observability.spans_1m` ([09_spans_1m.sql](db/clickhouse/09_spans_1m.sql)) | `(team_id, ts_bucket, fingerprint, service, name, kind_string)` | `toYYYYMMDD(timestamp)` | 30 d | `spans_1m_mv` from `spans` |

#### Spans schema

Hot resource/span attributes are JSON typed-path subcolumns inside `attributes JSON(\`service.name\` LowCardinality(String), \`host.name\` LowCardinality(String), …, max_dynamic_paths=100)`. Type-hinted paths are stored as native subcolumns with column-equivalent read performance — no JSON parse per row. Reader-facing names live as zero-cost `ALIAS` columns: `operation_name` (= `name`), `start_time` (= `timestamp`), `duration_ms` (= `duration_nano/1e6`), `status` (= `status_code_string`), `http_status_code` (= `toUInt16OrZero(response_status_code)`), `is_error` (= `if(has_error OR toUInt16OrZero(response_status_code) >= 400, 1, 0)`), `is_root` (= `if(parent_span_id IN ('', '0000000000000000'), 1, 0)`). `fingerprint` is a real top-level column written by the mapper.

**Two rollup tiers exist.** `observability.spans_1m` (introduced in [09_spans_1m.sql](db/clickhouse/09_spans_1m.sql)) is the spans-side analog of `metrics_1m`: a 1-minute `AggregatingMergeTree` populated by `spans_1m_mv` from raw spans. It carries `latency_state AggregateFunction(quantileTiming, Float64)` plus `SimpleAggregateFunction(sum, …)` columns (`request_count`, `error_count`, `duration_ms_sum`, `duration_ms_max`) and a wide set of group dimensions (`service`, `name`, `kind_string`, `peer_service`, `host`, `pod`, `environment`, `is_root`, `http_method`, `http_route`, `http_status_bucket`, `response_status_code`, `status_code_string`, `service_version`, `db_system`, `db_operation_name`, `db_collection_name`, `db_namespace`, `db_response_status`, `server_address`, `exception_type`, `error_type`, `status_message_hash`). PK extends to `(... kind_string, exception_type, status_message_hash)` so error-grouping queries hit a tight contiguous range; non-error rows share `('', 0)` tail keys → no row explosion. `sample_status_message`, `sample_trace_id`, `sample_exception_stacktrace` carry `any()` aggregates for drill-in display. **Aggregate / count / percentile readers across `services/*` (redmetrics, slo, errors, deployments, httpmetrics spans-side, latency.Histogram, topology), `infrastructure/{nodes, fleet}`, all `saturation/database/*` non-detail panels, and `traces/explorer.{Trend, Facets}` + `trace_suggest.SuggestScalar` project from `spans_1m`.** DETAIL queries stay on raw `observability.spans` (use `spans_resource` to narrow `fingerprint IN (...)`): per-trace lookups, per-span listings, free-text `db_statement` grouping, dynamic-threshold queries, heatmaps, and arbitrary-attribute typeahead.

`observability.trace_index` (repurposed in [08_trace_index.sql](db/clickhouse/08_trace_index.sql)) is now **spans-fed** — the `logs_to_trace_index` MV is dropped, replaced by `spans_to_trace_index`. The table holds `(trace_id, team_id, ts_bucket, fingerprint, timestamp, span_id, is_root)` keyed leading-PK on `trace_id`. Both `traces/explorer.GetByID` and `logs/trace_logs` resolve `(team_id, trace_id) → (ts_bucket bounds, fingerprint set)` from this table in O(one granule), then narrow the raw scan against `observability.spans` (for trace details) or `observability.logs` (for trace logs). `log_id` is fetched from the logs row scan directly — trace_index doesn't carry it.

#### Metrics_1m rollup

`observability.metrics_1m` is a 1-minute `AggregatingMergeTree` rollup populated from raw `observability.metrics` via the `metrics_1m_mv` materialized view. The rollup carries five `SimpleAggregateFunction` scalar columns (`val_min`, `val_max`, `val_sum`, `val_count`, `val_last`) for Gauge/Sum metrics. **Histograms** carry two array columns — `hist_buckets SimpleAggregateFunction(max, Array(Float64))` (representative bounds; deterministic since SDK-emitted bounds are stable per fingerprint) and `hist_counts AggregateFunction(sumForEach, Array(UInt64))` (element-wise per-bucket counts; readers must call `-Merge`) — plus `hist_sum SimpleAggregateFunction(sum, Float64)` and `hist_count SimpleAggregateFunction(sum, UInt64)`. Per-fingerprint constants (`service`, `host`, `environment`, `k8s_namespace`, `http_method`, `http_status_code`) and per-DP varying columns (`attributes`, `resource`) sit in the MV's `GROUP BY` directly. The `attr_hash UInt64 = cityHash64(toJSONString(attributes))` discriminates attribute combos in the rollup PK.

Reader pattern for percentiles: project `max(hist_buckets) AS hist_buckets, sumForEachMerge(hist_counts) AS hist_counts` (plus `sum(hist_sum)`/`sum(hist_count)` where avg-duration panels need them) and interpolate p50/p95/p99 Go-side via [internal/shared/quantile.FromHistogram](internal/shared/quantile/quantile.go). Bucket-alignment invariant: `quantile.FromHistogram` assumes aligned bucket bounds per fingerprint, which OTel SDKs guarantee per metric.

### Bucket consistency invariant

Every `ts_bucket` value used in a PREWHERE — for spans, logs, and metrics, raw or `_resource` — is computed Go-side via [internal/infra/timebucket/](internal/infra/timebucket/). ClickHouse never computes a bucket itself in any reader SQL we emit: no `toStartOfHour`, `toStartOfInterval`, `toStartOfDay`, `toStartOfMinute`, or `toStartOfFiveMinutes` appears in any query. The exceptions are the `metrics_1m_mv` body in `07_metrics_1m.sql` and the `spans_1m_mv` body in `09_spans_1m.sql`, both of which derive `ts_bucket` from `timestamp` server-side at MV evaluation time — the value still matches what `timebucket.BucketStart` would produce. Display-time aggregation (`toStartOfInterval(timestamp, INTERVAL @stepMin MINUTE)` in trend queries) is permitted because no row is being matched against a bucket value — buckets are only computed Go-side when the alignment must agree byte-for-byte between writer and reader.

Round-trip enforced by [internal/infra/timebucket/consistency_test.go](internal/infra/timebucket/consistency_test.go). Changing `BucketSeconds` is a breaking schema change requiring a table rebuild.

### Read paths

Every read-path module follows the apm pattern: queries-only `repository.go` (each query is `const query = `…``, all values bound via `clickhouse.Named()` — **no `fmt.Sprintf` in any reader SQL**, **no phantom rollup table references**); derivations live in `service.go` (rate folds, percentile interpolation, error-rate math, NaN scrub, top-N sorting). Most filter-bearing modules use an inline `WITH active_fps AS (SELECT [DISTINCT] fingerprint FROM observability.<signal>_resource PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd …)` CTE to pre-narrow before the main scan; trace-ID-scoped point lookups (most of `tracedetail`, all of `trace_paths`, `trace_servicemap`, `trace_shape`, `trace_suggest`) skip the resource CTE because the trace_id+span_id PREWHERE is already maximal.

#### Logs read path ([internal/modules/logs/](internal/modules/logs/))

- [explorer/](internal/modules/logs/explorer/) — `POST /api/v1/logs/query`. List-only. Reads `observability.logs`; the inline `logs_resource` CTE is emitted **only when a resource-side filter is present** — when `f.Services`/`Hosts`/`Pods`/`Containers`/`Environments`/`Exclude*` are all empty the query drops the CTE and PREWHEREs `(team_id, ts_bucket)` directly. Search predicate uses `hasToken(body, lower(@search))` (default) or `lower(body) LIKE concat('%', lower(@search), '%')` (exact mode) against the `idx_body_text` native text (inverted) index.
- [logdetail/](internal/modules/logs/logdetail/) — `GET /api/v1/logs/:id`. Single-log deep link. The `:id` is the row's `log_id` — a stable FNV-64a hex hash of `(trace_id, timestamp_ns, body, fingerprint)` computed by the ingestion mapper ([internal/ingestion/logs/mapper.go::computeLogID](internal/ingestion/logs/mapper.go)) and stored on the `observability.logs` row. Repository PREWHEREs `(team_id, log_id = @logID)` only — the `idx_log_id` bloom-filter skip-index defined inline in [02_logs.sql](db/clickhouse/02_logs.sql) prunes granules across the full 30-day partition set without needing a `ts_bucket` bound.
- [trace_logs/](internal/modules/logs/trace_logs/) — `GET /api/v1/logs/trace/:traceID?limit=1000`. All logs for a trace via two `const` queries: step 1 reads `observability.trace_index` (PREWHERE leads on `trace_id`, single granule) to resolve `(min ts_bucket, max ts_bucket, fingerprint set)`; step 2 scans `observability.logs` PREWHEREd on three PK slots `(team_id, ts_bucket BETWEEN, fingerprint IN @fps)` plus the trace_id row-side check. `traceID` is lowercased once Go-side in `traceIDArgs` (storage is canonical lowercase hex from `hex.EncodeToString`) — comparisons are direct, no `lowerUTF8` per row. `service.GetByTraceID` orchestrates the two-step lookup and maps via existing `models.MapLogs`. Replaces the old `tracedetail.GetTraceLogs` flow — the bundle endpoint (`GET /traces/:traceId/bundle`) now consumes `trace_logs.Service` directly for its `logs` field.
- [facets/](internal/modules/logs/facets/) (package `log_facets`) — `POST /api/v1/logs/facets`. **One** CH query: a 4-arm `UNION ALL` over `observability.logs_resource`, one arm per resource dim (service / host / pod / environment). Severity is a static label list returned from `models.SeverityLabels` — closed enum, no DB call. Service folds the unioned rows into `models.Facets` by `dim`.
- [trends/](internal/modules/logs/trends/) (package `log_trends`) — `POST /api/v1/logs/summary` and `POST /api/v1/logs/trend` (peer endpoints; the old composite `/logs/trends` is removed — frontend fires both in parallel). `Summary` reads SQL-side `count() / countIf(severity_bucket >= 4) / countIf(severity_bucket = 3)`; `Trend` groups by `toStartOfInterval(timestamp, INTERVAL @stepMin MINUTE)` at display grain (1m / 5m / 1h / 1d via `timebucket.DisplayGrain(window)`) — payload size bounded regardless of window length. Both readers emit the `active_fps` CTE only when a resource filter is present.
- [filter/](internal/modules/logs/filter/) — typed `Filters` + `AttrFilter` + `Validate()` (≤30-day clamp, `searchMode="ngram"` default) + `BuildClauses(f) → (resourceWhere, where, args)`. Single shared SQL emitter consumed by all filter-bearing logs repos. When `resourceWhere == ""` callers skip the `active_fps` CTE. Stable bind names (`services`/`excServices`/`hosts`/`severities`/`traceID`/`spanID`/`search`/`akey_N`/`aval_N`) so identical predicate sets produce byte-identical SQL.
- [shared/models/](internal/modules/logs/shared/models/) — output models: `Log`, `LogRow`, `Cursor`, `Facets`, `Summary`, `TrendBucket`, `PageInfo`, `EncodeLogID`, `ParseLogID`, `MapLog(s)`, `LogColumns` (the canonical SELECT projection).

#### Traces read path ([internal/modules/traces/](internal/modules/traces/))

- [explorer/](internal/modules/traces/explorer/) — `POST /api/v1/traces/query`, `GET /api/v1/traces/:traceId`. List + optional `summary` / `facets` / `trend` includes; root-span PREWHERE selects per-trace summary rows. Reads `observability.spans` with `spans_resource` CTE.
- [span_query/](internal/modules/traces/span_query/) — `POST /api/v1/spans/query`. Span-level explorer view (vs trace-grouped). Reads `observability.spans` with `spans_resource` CTE.
- [tracedetail/](internal/modules/traces/tracedetail/) — `GET` per-trace drill-downs: `/traces/:id/spans`, `/spans/:id/tree`, `/traces/:id/span-events`, `/traces/:id/spans/:spanId/attributes`, `/traces/:id/spans/:spanId/logs`, `/traces/:id/related`, `/traces/:id/bundle`. Most queries are trace-id-scoped point lookups (no resource CTE). `GetRelatedTraces` is the lone window-scoped query with the apm-style `spans_resource` CTE plus PREWHERE on `(team_id, ts_bucket, fingerprint IN active_fps, service, name)`. Reads `observability.spans` (and `observability.logs` for `GetSpanLogs`). Service owns derivation: events parsing (`splitEventRows` + `parseEventJSON`), `db.statement` literal normalization (`normalizeDBStatement`), parent-chain Subtree walk (`filterSubtree`). The `/bundle` endpoint composes spans + critical/error paths + span-kind breakdown + logs; the logs portion now consumes [trace_logs](internal/modules/logs/trace_logs/) (the canonical "all logs for a trace" path lives under `/logs/trace/:traceID`).
- [trace_paths/](internal/modules/traces/trace_paths/) — `GET /traces/:id/critical-path`, `/traces/:id/error-path`. Trace-id-scoped queries (no resource CTE). Service runs longest-path DAG traversal + error-chain walk.
- [trace_servicemap/](internal/modules/traces/trace_servicemap/) — `GET /traces/:id/service-map`, `/traces/:id/errors`. Trace-id-scoped. Service folds spans into service-map nodes/edges and groups errors by exception_type.
- [trace_shape/](internal/modules/traces/trace_shape/) — `GET /traces/:id/flamegraph`, `/traces/:id/span-kind-breakdown`. Trace-id-scoped. Service builds flamegraph trees with self-time computation.
- [trace_suggest/](internal/modules/traces/trace_suggest/) — `POST /traces/suggest`. Autocomplete: `IsScalarField` validates the canonical scalar-field name set up front; `fetch` dispatches `@`-prefix → `SuggestAttribute` (JSONExtractString from `attributes`) vs scalar → `SuggestScalar` (closed-set column expression). Both queries PREWHERE `(team_id, ts_bucket)`.
- [filter/](internal/modules/traces/filter/) — typed `Filters` (`Services`/`Operations`/`SpanKinds`/`HTTPMethods`/`HTTPStatuses`/`Statuses`/`Environments`/`PeerServices`/`TraceID`/`MinDurationNs`/`MaxDurationNs`/`HasError` + `Exclude*` + `Attributes []AttrFilter`) + `AttrFilter` + `Validate()` + `BuildClauses(f)`. Mirrors `logs/filter`, shared by `explorer` / `span_query`. (Span-side error analytics live in [services/errors](internal/modules/services/errors/) — there is no separate traces/errors module.)

The OTLP empty / all-zero-hex / case-insensitive `trace_id` matching for trace-id-scoped queries is inlined as a `traceIDMatchPredicate` SQL fragment + `traceIDArgs(teamID, traceID)` helper at the bottom of each repository.go (no shared package).

#### Metrics read path ([internal/modules/metrics/](internal/modules/metrics/))

- [explorer/](internal/modules/metrics/explorer/) — `GET /metrics/names`, `GET /metrics/:metricName/tags`, `POST /metrics/explorer/query`. Reads `observability.metrics_1m` with the `metrics_resource` CTE; `ListMetricNames` uses a `WITH names AS (... metrics_resource …)` candidate-narrowing CTE then projects metric metadata. Aggregation expressions emit via `buildAggExpr` (rate divisor uses `strconv.FormatInt`, not `fmt.Sprintf`). Service `convertFEQuery` maps the FE `[{key, operator, value}]` filter shape into `filter.Filters`, validates, hands off to repo; `buildColumnarResult` folds flat per-bucket rows into the columnar `{timestamps, series}` wire shape.
- [filter/](internal/modules/metrics/filter/) — typed `Filters{Tags []TagFilter, Aggregation, …}` + `TagFilter{Key, Operator, Value}` + `Validate()` (≤30-day clamp, `validAggregations`), `Canonical()` (alias map: `service.name`→`service`, `host.name`→`host`, `deployment.environment`→`environment`, `k8s.namespace.name`→`k8s_namespace`), `ResourceColumn()` (the 4 flat columns on `metrics_resource`), `AttrColumn()` (sanitized `attributes.\`<key>\`::String` JSON-path on `metrics_1m`), `GroupByColumn()`, `SanitizeKey()`, `BuildClauses(f) → (resourceWhere, where, args)`. Stable bind names per canonical resource key; arbitrary attribute filters use indexed `mf0`/`mf1`/… binds. (Deviates from the per-module-helper-at-the-bottom-of-repository.go convention logs/traces follow — chosen for metrics so canonicalization, sanitization, and clause emission share one home.)

#### Services read path ([internal/modules/services/](internal/modules/services/))

- [apm/](internal/modules/services/apm/) — `GET /apm/rpc-duration`, `/apm/rpc-request-rate`, `/apm/messaging-publish-duration`, `/apm/process-cpu`, `/apm/process-memory`, `/apm/open-fds`, `/apm/uptime`. Seven repo methods, one per panel — each binds its OTel metric-name const internally (`QueryRPCDurationHistogram`, `QueryMessagingPublishDurationHistogram`, `QueryRPCRequestCountSeries`, `QueryProcessCPUStateSeries`, `QueryProcessMemoryAvg`, `QueryProcessOpenFDsSeries`, `QueryProcessUptimeSeries`). Reads `observability.metrics_1m` with `metrics_resource` CTE. Service: histogram summary (avg + P50/P95/P99 via `quantile.FromHistogram`), gauge series (`AvgByTime`), state-bucketed series (`AvgByTimeAndKey` for CPU states), multi-metric memory fold.
- [deployments/](internal/modules/services/deployments/) — `GET /deployments/latest-by-service`, `/deployments/list`, `/deployments/compare`, `/deployments/timeline`, `/deployments/impact`, `/deployments/active-version`. Reads `observability.deployments` (its own dimension table) plus `observability.spans` (joined for traffic / error / latency before/after deployment).
- [errors/](internal/modules/services/errors/) — `GET /errors/{service-error-rate,error-volume,latency-during-error-windows,groups}`, `/errors/groups/:groupId{,/traces,/timeseries}`, `/spans/{exception-rate-by-type,error-hotspot,http-5xx-by-route}`, `/errors/fingerprints{,/trend}`. 12 repo methods, each with `*All` and `*ByService` siblings; reads `observability.spans` with `spans_resource` CTE. `service-error-rate` and `latency-during-error-windows` share the same `ServiceErrorRateRows*` repo query — service.go discards zero-error rows for the latency view.
- [httpmetrics/](internal/modules/services/httpmetrics/) — 17 routes under `/api/v1/http/...` served by 11 endpoint-named repo methods. Metrics-backed (8 named methods on `metrics_1m` + `metrics_resource` CTE): `QueryServerRequestDurationHistogram`, `QueryServerRequestBodySizeHistogram`, `QueryServerResponseBodySizeHistogram`, `QueryClientRequestDurationHistogram`, `QueryDNSLookupDurationHistogram`, `QueryTLSConnectDurationHistogram`, `QueryServerRequestStatusSeries` (shared by request-rate / status-distribution / error-timeseries panels), `QueryServerActiveRequestsSeries`. Histogram percentiles computed Go-side via `quantile.FromHistogram`. Spans-backed (3 named methods on `spans_resource` CTE): `QueryRouteAgg`, `QueryRouteErrorSeries`, `QueryExternalHostAgg`; the percentile-bearing methods (`QueryRouteAgg`, `QueryExternalHostAgg`) read `observability.spans_1m` and project `quantileTimingMerge(0.95)(latency_state)`.
- [latency/](internal/modules/services/latency/) — `GET /services/latency/histogram`, `/services/latency/heatmap`. Reads `observability.spans` with `spans_resource` CTE; heatmap is 2D bucketing of (display_bucket × duration_bucket).
- [redmetrics/](internal/modules/services/redmetrics/) — RED summary + Apdex + per-route + breakdowns. 11 repo methods. Latency-percentile methods (`GetSummary`, `GetTopSlowOperations`, `GetP95LatencyTimeSeries`) read `observability.spans_1m` and project `quantileTimingMerge(q)(latency_state)`; Apdex / error-by-route / span-kind / etc. read raw `observability.spans` (Apdex computes exact `countIf(duration_nano <= @satisfiedNs)` / `countIf(duration_nano > @satisfiedNs AND duration_nano <= @toleratingNs)` buckets, which require raw-span granularity). `*ByService` split (no inline `query += ...`). The service-level error-rate timeseries lives at `/errors/service-error-rate` (services/errors), not under `/spans/red/`.
- [slo/](internal/modules/services/slo/) — `GET /slo`, `/slo/stats`, `/slo/burn-down`, `/slo/burn-rate`. 8 repo methods (4 `*All` + 4 `*ByService`); reads `observability.spans` with `spans_resource` CTE. `serviceName` filter lives inside the CTE. Service computes availability (`(total-errs)*100/total`), avg latency, cumulative error-budget burn, and a 3-way `errgroup` for burn-rate (fast 5m + slow 1h + summary). `SLOService.now` is a swappable clock for tests.
- [topology/](internal/modules/services/topology/) — `GET /api/v1/services/topology[?service=<focus>]`. Two queries (`GetNodes`, `GetEdges`) fanned via `errgroup`. Both read `observability.spans_1m` with `spans_resource` CTE. `GetEdges` derives directed edges from `kind_string = 'Client'` spans where `service` and `peer_service` are non-empty and distinct. P50/P95/P99 (and P50/P95 for edges) project `quantileTimingMerge(q)(latency_state)` server-side. Service classifies node health (`unhealthyErrorRate=0.05`, `degradedErrorRate=0.01` → `healthy`/`degraded`/`unhealthy`), `addMissingEdgeNodes` injects pure-CLIENT producers as nodes, optional 1-hop `filterNeighborhood` for the focus filter.

#### Infrastructure read paths ([internal/modules/infrastructure/](internal/modules/infrastructure/))

Apm-style across 9 submodules. Seven modules (`connpool`, `cpu`, `disk`, `jvm`, `kubernetes`, `memory`, `network`) read `observability.metrics_1m` with the `metrics_resource` CTE. Two modules (`nodes`, `fleet`) read `observability.spans_1m` with the `spans_resource` CTE for per-host / per-pod RED aggregates with P95 via `quantileTimingMerge(0.95)(latency_state)`.

- **Resolver narrowing**: `WITH active_fps AS (SELECT fingerprint FROM observability.metrics_resource WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name [= @metricName | IN @metricNames])`. Main query PREWHEREs `(team_id, ts_bucket, fingerprint IN active_fps, metric_name)`.
- **Repository contract**: queries only, **one CH call per method**. Each module declares its own `metricArgs` / `withMetricName{,s}` / `spanArgs` / `spanBucketBounds` helpers locally. Reads OTel semconv 1.30+ canonical attribute paths inline (e.g., `attributes.'system.cpu.state'::String`, `attributes.'jvm.memory.pool.name'::String`, `attributes.'k8s.container.name'::String`).
- **Service derivation**: counter→rate via `displaybucket.SumByTimeAndKey`; gauge avg via custom multi-key folds; JVM GC duration P50/P95/P99 via `quantile.FromHistogram` merged across `(display_bucket, gc_name)` (OTel histograms in `metrics_1m`); multi-metric percentage blends (cpu 3-metric, memory 4-metric, disk 3-metric, connpool 5-metric); `≤1.0 → *100` percentage normalization; NaN scrub via `utils.SanitizeFloat`; RED `error_rate = errCount × 100 / reqCount` and `avg_latency_ms = durationMsSum / reqCount` for `nodes` / `fleet` (computed from `spans_1m`'s `request_count`, `error_count`, `duration_ms_sum` SimpleAggregateFunction columns).
- [infraconsts/](internal/modules/infrastructure/infraconsts/) — shared OTel metric-name + column-name constants. Not a routable module.

Panels assume OTel semconv 1.30+ canonical attribute names. Older instrumentations that emit non-canonical attribute spellings will return empty for those panels until upgraded.

#### Kafka saturation read path ([internal/modules/saturation/kafka/](internal/modules/saturation/kafka/))

Panels module + a sibling `explorer/` composer.

- [saturation/kafka/](internal/modules/saturation/kafka/) — 17 panel routes under `/api/v1/saturation/kafka/...` served by ~22 endpoint-named repo methods, each binding its OTel metric-name set as a closed const internally (e.g. `QueryPublishMessageCount` binds `ProducerMetrics`, `QueryPublishLatencyByTopic` binds `MetricPublishDuration` + `publishOperationAliases`, `QueryRebalanceSignals` binds `RebalanceMetrics`). All reads go to `observability.metrics_1m` via the `metrics_resource` CTE — no panel touches `observability.spans`. Multi-name OTel aliases live in `otel_conventions.go` (`ProducerMetrics`, `ConsumerMetrics`, `ConsumerLagMetrics`, `RebalanceMetrics`, `ProcessMetrics`). Latency-by-topic / -by-group additionally OR-match `messaging.client.operation.duration` filtered by `messaging.operation.name IN @opAliases` so instrumentations emitting only the unified op-duration metric still light up the panels.
- [saturation/kafka/explorer/](internal/modules/saturation/kafka/explorer/) — 9 catalog routes (`/saturation/kafka/{summary, topics, groups, topic/overview, topic/groups, topic/partitions, group/overview, group/topics, group/partitions}`). Composes `saturation/kafka.KafkaService.GetTopicMetricSamples` + `GetConsumerMetricSamples` into per-topic / per-group rollups using a closed set of OTel `kafka.consumer.*` metric names. Mirrors the database-explorer placement pattern.

Service derivations: rate = sum(value) ÷ `timebucket.DisplayGrain(window).Seconds()` per (display_bucket, dim); histogram P50/P95/P99 via `quantile.FromHistogram(buckets, counts, q)` after merging aligned `hist_buckets`/`hist_counts` per (display_bucket, dim); gauge averages via custom multi-key folds; partition-lag is server-side `argMax(value, timestamp)` + DESC LIMIT 200; rebalance fold switches on `metric_name` to populate the 6 fields of `RebalancePoint`. Composite endpoints: `summary-stats` issues 5 repo calls in series; `e2e-latency` and `rebalance-signals` each use one multi-metric repo call with `metric_name` projected as a row dim.

Error-rate panels depend on OTel semconv 1.30+ `error.type` data-point attribute. Older kafka instrumentations don't emit it; those panels return empty for affected services.

#### DB saturation read path ([internal/modules/saturation/database/](internal/modules/saturation/database/))

Apm-style across 9 submodules — `volume`, `errors`, `latency`, `collection`, `system` (single-DB scoped), `systems` (cross-system list), `summary` (top-level KPIs), `slowqueries`, `connections` — plus `explorer` (composer over the others, no repo of its own).

- [filter/](internal/modules/saturation/database/filter/) — typed `Filters` (DBSystem/Collection/Namespace/Server) + OTel constants (`AttrDB*` / `MetricDB*`) + `BuildSpanClauses(f)` + `BuildMetricClauses(f)` + `SpanArgs/SpanBucketBounds` (1-min) + `MetricArgs/MetricArgsMulti/MetricBucketBounds` (1-min) + `BucketWidthSeconds` + `LatencyBucketBoundsMs` (`1/5/10/25/50/100/250/500/1000/2500/5000/10000/30000/+Inf` ms) + `LatencyBucketCountsSQL()` (closed-set `[countIf(duration_nano …) …]` array emitter).
- **Source-table choice (per OTel semconv 1.30+)**:
  - **Spans-side (8 submodules)** — `volume / errors / latency / collection / system / systems / summary / slowqueries`: each DB call is one span. Non-percentile reads (counts / errors / read-vs-write / heatmaps) and `db_statement`-grouped slowqueries panels (`GetSlowQueryPatterns`, `GetP99ByQueryText`, `GetSlowQueryRate` with its dynamic threshold) read raw `observability.spans` with the `spans_resource` CTE. **Percentile reads** (`GetLatencyBy{System,Operation,Collection,Namespace,Server}`, `GetCollectionLatency`, `GetSystemLatency`, `GetSystemTopCollectionsBy{Latency,Volume}`, `GetMainStats` p95/p99, `GetSystemSummariesRaw` p95, `GetSlowestCollections` p99) read `observability.spans_1m` and project `quantileTimingMerge(q)(latency_state)`. Filter helpers split accordingly: `BuildSpanClauses` / `SpanGroupColumn` for raw-spans queries, `BuildSpans1mClauses` / `Spans1mGroupColumn` for `spans_1m`.
  - **Metrics-side (`connections` only)** — the OTel `db.client.connection.*` family is instrumentation-side gauges/counters/histograms, not per-call spans. Reads `observability.metrics_1m` with `metrics_resource` CTE. 8 methods: count / max / idle.max / idle.min / pending_requests (gauges, raw `value`), timeouts (counter → service folds + per-second rate), wait_time / create_time / use_time (OTel histograms; service merges per `(display_bucket, pool.name)` and computes p50/p95/p99 via `quantile.FromHistogram`). The `summary` and `systems` submodules are two-phase composites that fan out spans aggregates + metrics-side active-connection gauge in parallel via errgroup, joining on `db_system`.
- **Repos clean** — every method is `const query = `…``. **No `fmt.Sprintf`, no `%s`, no phantom-rollup column references.**
- [internal/](internal/modules/saturation/database/internal/) holds HTTP-layer helpers (`ParseFilters` / `ParseLimit` / `ParseThreshold` / `RequireCollection` / `RequireDBSystem` / `RegisterGET` / `RegisterGroup` — single-prefix registration under `/saturation/database/*`).

### Module shape

Most feature modules follow the canonical 6-file layout:

- `module.go` — `registry.Module` wiring + `RegisterRoutes`
- `handler.go` — Gin handlers
- `service.go` — derivations + orchestration
- `repository.go` — queries-only
- `dto.go` (where needed) — request/response shapes
- `models.go` (where needed) — internal scan/projection structs

A few packages deviate (multi-file repos like `traces/tracedetail/`'s `spans_list.go`, multi-file repos for `traces/explorer`'s `repo_facets.go`/`repo_trend.go`); treat the manifest and package contents as the source of truth.

## Request surfaces

### HTTP

- Health: `/health`, `/health/live`, `/health/ready`
- Prometheus metrics: `/metrics`
- Product APIs: `/api/v1/...` (single group, `TenantMiddleware`)

Public-prefix list (auth bypass) lives in [internal/infra/middleware/middleware.go](internal/infra/middleware/middleware.go): `/api/v1/auth/login`, `/otlp/`, `/health`. Public-POST list: `/api/v1/auth/forgot-password`, `/api/v1/users`, `/api/v1/teams`.

### gRPC

- OTLP gRPC server on `otlp.grpc_port`. Auth via `auth.UnaryInterceptor` / `auth.StreamInterceptor` (chained after `grpcMetricsUnary/Stream`). Modules implementing `registry.GRPCRegistrar` register their services — the three ingestion handlers (spans/logs/metrics) are the only callers today.

## Current module inventory

From [internal/app/server/modules_manifest.go](internal/app/server/modules_manifest.go):

- **services**: `apm`, `deployments`, `httpmetrics`, `errors`, `redmetrics`, `slo`, `latency`, `topology`
- **traces**: `explorer`, `tracedetail`, `span_query`, `trace_paths`, `trace_servicemap`, `trace_shape`, `trace_suggest` (+ `filter` shared package)
- **logs**: `explorer`, `logdetail` (registered as `log_detail`), `log_facets`, `log_trends`, `trace_logs` (+ `filter` and `shared/models` packages)
- **metrics**: `explorer` (+ `filter` shared package)
- **infrastructure**: `connpool`, `cpu`, `disk`, `jvm`, `kubernetes`, `memory`, `network`, `fleet`, `nodes` (+ `infraconsts` shared package)
- **saturation**: `kafka/{root, explorer}`; `database/{collection, connections, errors, explorer, latency, slowqueries, summary, system, systems, volume}` (+ `database/filter` and `database/internal` packages)
- **user**: `auth`, `team`, `user`
- **saved_views**: stores per-user / per-team filter snapshots (FE explorer pages); MySQL-only, no CH
- **ingestion** (background runners + gRPC registrars): spans, logs, metrics

49 modules total.

## Key directories

| Path | Purpose |
|------|---------|
| [cmd/server/](cmd/server/) | Main server binary. ClickHouse migrations apply during `newInfra`, before HTTP/gRPC start. |
| [db/clickhouse/](db/clickhouse/) | ClickHouse schema and migration files. |
| [internal/app/](internal/app/) | App composition, registry, routes, infra wiring. |
| [internal/auth/](internal/auth/) | HTTP/gRPC auth interceptors. |
| [internal/config/](internal/config/) | Config structs, defaults, validation. |
| [internal/infra/](internal/infra/) | Cross-cutting infra: database, kafka, redis, session, middleware, timebucket, otlp, cursor, utils, fingerprint, metrics. |
| [internal/ingestion/](internal/ingestion/) | OTLP ingest pipeline (spans/logs/metrics, flat 7-file layout per signal). |
| [internal/modules/](internal/modules/) | Product/domain APIs. |
| [internal/shared/](internal/shared/) | Shared contracts and helpers: contracts, displaybucket, errorcode, httputil, quantile. |
| [docs/](docs/) | Supporting design and ops docs. |
| [monitoring/](monitoring/) | Local Prometheus + Grafana stack and dashboards. |
| [loadtest/](loadtest/) | k6 load-test project (query-side). |

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

[loadtest/](loadtest/) is a top-level k6 project parallel to `internal/` that exercises every read endpoint on the running backend. Ingestion-side load generation is out of scope and lives in a separate tool.

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

Bootstrap (`lib/bootstrap.js`) is idempotent: it logs in first, and only falls through to `POST /api/v1/teams` + `POST /api/v1/users` against the public auth surface if no user exists. A safety rail blocks the create flow on non-localhost hosts unless `ALLOW_REMOTE_BOOTSTRAP=1`.

Run via `make loadtest-smoke|loadtest-all|loadtest-<module>`. Output: stdout summary, live ticker, optional `JSON_OUT=...` results file, and optional Prometheus remote-write to `:19091/api/v1/write` (the local stack ships with `--web.enable-remote-write-receiver`).

## Related docs

- Overview: [README.md](README.md)
- Flow diagrams: [docs/flows/](docs/flows/) — ingestion, auth, http-request, trace-assembly
- Ingest dashboard: [monitoring/grafana/dashboards/optikk_ingest.json](monitoring/grafana/dashboards/optikk_ingest.json)
- ClickHouse schema overview: [db/clickhouse/README.md](db/clickhouse/README.md)
