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
| `observability.trace_index` ([08_trace_index.sql](db/clickhouse/08_trace_index.sql)) | `(trace_id, team_id, ts_bucket, fingerprint, timestamp, span_id)` | `toYYYYMMDD(toDateTime(ts_bucket))` | 30 d | `spans_to_trace_index` MV from `spans` (spans-fed); reverse-key projection used by [trace_logs](internal/modules/logs/trace_logs/) and every trace-id-scoped reader in [traces/detail](internal/modules/traces/detail/), [traces/paths](internal/modules/traces/paths/), [traces/shape](internal/modules/traces/shape/), [traces/servicemap](internal/modules/traces/servicemap/) via inline `WITH trace_loc AS (...) ... PREWHERE (ts_bucket, fingerprint) IN ...` |
| `observability.metrics_1m` ([07_metrics_1m.sql](db/clickhouse/07_metrics_1m.sql)) | `(team_id, ts_bucket, metric_name, fingerprint, attr_hash, timestamp)` | `toYYYYMMDD(timestamp)` | 90 d | `metrics_1m_mv` from `metrics` |
| `observability.spans_1m` ([09_spans_1m.sql](db/clickhouse/09_spans_1m.sql)) | `(team_id, ts_bucket, fingerprint, service, name, kind_string)` | `toYYYYMMDD(timestamp)` | 30 d | `spans_1m_mv` from `spans` |

#### Spans schema

Hot resource/span attributes are JSON typed-path subcolumns inside `attributes JSON(\`service.name\` LowCardinality(String), \`host.name\` LowCardinality(String), …, max_dynamic_paths=100)`. Type-hinted paths are stored as native subcolumns with column-equivalent read performance — no JSON parse per row. Reader-facing names live as zero-cost `ALIAS` columns: `operation_name` (= `name`), `start_time` (= `timestamp`), `duration_ms` (= `duration_nano/1e6`), `status` (= `status_code_string`), `http_status_code` (= `toUInt16OrZero(response_status_code)`), `is_error` (= `if(has_error OR toUInt16OrZero(response_status_code) >= 400, 1, 0)`), `is_root` (= `if(parent_span_id IN ('', '0000000000000000'), 1, 0)`). `fingerprint` is a real top-level column written by the mapper. `error_group_id` is a native `MATERIALIZED` column precomputed on insert using MD5/CityHash on the error fingerprint combination.

**Two rollup tiers exist.** `observability.spans_1m` (introduced in [09_spans_1m.sql](db/clickhouse/09_spans_1m.sql)) is the spans-side analog of `metrics_1m`: a 1-minute `AggregatingMergeTree` populated by `spans_1m_mv` from raw spans. It carries `latency_state AggregateFunction(quantileTiming, Float64)` plus `SimpleAggregateFunction(sum, …)` columns (`request_count`, `error_count`, `duration_ms_sum`, `duration_ms_max`) and a wide set of group dimensions (`service`, `name`, `kind_string`, `peer_service`, `host`, `pod`, `environment`, `is_root`, `http_method`, `http_route`, `http_status_bucket`, `response_status_code`, `status_code_string`, `service_version`, `db_system`, `db_operation_name`, `db_collection_name`, `db_namespace`, `db_response_status`, `server_address`, `exception_type`, `error_type`, `status_message_hash`). PK extends to `(... kind_string, exception_type, status_message_hash)` so error-grouping queries hit a tight contiguous range; non-error rows share `('', 0)` tail keys → no row explosion. `sample_status_message`, `sample_trace_id`, `sample_exception_stacktrace` carry `any()` aggregates for drill-in display. **Aggregate / count / percentile readers across `services/*` (redmetrics, errors, deployments, latency.Histogram, topology), `infrastructure/{nodes, fleet}`, all `saturation/database/*` non-detail panels, and `traces/{trend, facets}` + `suggest.SuggestScalar` project from `spans_1m`.** DETAIL queries stay on raw `observability.spans` (use `spans_resource` to narrow `fingerprint IN (...)`): per-trace lookups, per-span listings, free-text `db_statement` grouping, dynamic-threshold queries, heatmaps, and arbitrary-attribute typeahead.

`observability.trace_index` (repurposed in [08_trace_index.sql](db/clickhouse/08_trace_index.sql)) is now **spans-fed** — the `logs_to_trace_index` MV is dropped, replaced by `spans_to_trace_index`. The table holds `(trace_id, team_id, ts_bucket, fingerprint, timestamp, span_id, is_root)` keyed leading-PK on `trace_id`. Every trace-id-scoped reader across `traces/{detail, paths, shape, servicemap}` — `detail.{GetTraceSummary, GetSpanAttributes, GetSpanEvents, ListSpansByTrace}`, `paths.{GetCriticalPath, GetErrorPath}`, `shape.{GetSpanKindBreakdown, GetFlamegraphData}`, `servicemap.{GetServiceMapSpans, GetTraceErrors}` — resolves and applies the trace's `(ts_bucket, fingerprint)` pair set inline via a single statement: `WITH trace_loc AS (SELECT ts_bucket, fingerprint FROM observability.trace_index PREWHERE trace_id = @traceID AND team_id = @teamID) ... PREWHERE (ts_bucket, fingerprint) IN (SELECT ts_bucket, fingerprint FROM trace_loc) AND trace_id = @traceID`. CH builds the tuple set from the trace_index granule lookup and uses it for granule pruning on the spans-side scan — same shape as `traces/detail.GetRelatedTraces`'s `active_fps` CTE. `logs/trace_logs` does the same conceptually but split across two Go-side queries (`LookupBounds` + `FetchByBounds`). `detail.ListSpanSubtree` is the one trace-id-scoped reader in this family that bypasses trace_index — it's keyed by `span_id`, not `trace_id`, and a separate span_id reverse projection would be needed (deferred). `log_id` is fetched from the logs row scan directly — trace_index doesn't carry it.

#### Metrics_1m rollup

`observability.metrics_1m` is a 1-minute `AggregatingMergeTree` rollup populated from raw `observability.metrics` via the `metrics_1m_mv` materialized view. The rollup carries four `SimpleAggregateFunction` scalar columns (`val_min`, `val_max`, `val_sum`, `val_count`) for Gauge/Sum metrics. **Histograms** carry only the per-data-point totals `hist_sum SimpleAggregateFunction(sum, Float64)` and `hist_count SimpleAggregateFunction(sum, UInt64)` plus `latency_state AggregateFunction(quantilesPrometheusHistogram(0.5, 0.95, 0.99), Float64, UInt64)` — the Prometheus-style quantile state is populated via `quantilesPrometheusHistogramArrayStateIf(0.5, 0.95, 0.99)(hist_buckets, arrayCumSum(hist_counts), metric_type = 'Histogram')` reading the bucket arrays from raw `observability.metrics` at MV evaluation time; the rollup itself does not store the arrays. Readers get server-side p50/p95/p99 via `quantilesPrometheusHistogramMerge(...)(latency_state)`. Per-fingerprint constants (`service`, `host`, `environment`, `k8s_namespace`, `http_method`, `http_status_code`) and per-DP varying columns (`attributes`, `resource`) sit in the MV's `GROUP BY` directly. The `attr_hash UInt64 = cityHash64(toJSONString(attributes))` discriminates attribute combos in the rollup PK.

Reader pattern for percentiles: SELECT `quantilesPrometheusHistogramMerge(0.5, 0.95, 0.99)(latency_state) AS qs` and project `qs[1]`/`qs[2]`/`qs[3]` as scalar `p50/p95/p99` (plus `sum(hist_sum)`/`sum(hist_count)` where avg-duration panels need them). The state column gives byte-identical Prometheus-style `histogram_quantile` semantics in one server-side merge, replacing the prior pattern of shipping `hist_buckets` + `hist_counts` arrays to Go and interpolating per row. Spans-side reads in `saturation/database/{collection,slowqueries}` that group by free-text `db_statement` (no `spans_1m` rollup applies — high-cardinality query text would explode the PK) compute their percentiles server-side too, via inline `quantileTiming` / `quantilesTiming` over raw `duration_nano / 1e6` — same `qs[1]/qs[2]/qs[3]` projection shape, just without the `-Merge` since there's no state column to merge.

### Bucket consistency invariant

Every `ts_bucket` value used in a PREWHERE — for spans, logs, and metrics, raw or `_resource` — is computed Go-side via [internal/infra/timebucket/](internal/infra/timebucket/). ClickHouse never computes a `ts_bucket` value in any reader SQL we emit. The only producers are the `metrics_1m_mv` body in `07_metrics_1m.sql` and the `spans_1m_mv` body in `09_spans_1m.sql`, both of which derive `ts_bucket` from `timestamp` server-side at MV evaluation time — the value still matches what `timebucket.BucketStart` would produce. **Display-time aggregation is server-side** via `timebucket.DisplayGrainSQL(windowMs)` returning a `toStartOfX(timestamp)` fragment dispatched to the specific `toStartOf{Minute,FiveMinutes,Hour,Day}` function — 10–15% faster than the generic `toStartOfInterval(timestamp, INTERVAL N UNIT)` for our 4 fixed grains. No row is matched against a Go-computed display bucket value, so no cross-language drift risk; `ts_bucket` (the partition-prune column) is the only place where Go and SQL must agree byte-for-byte.

Round-trip enforced by [internal/infra/timebucket/consistency_test.go](internal/infra/timebucket/consistency_test.go). Changing `BucketSeconds` is a breaking schema change requiring a table rebuild.

### Read paths

Every read-path module follows the apm pattern: queries-only `repository.go` (each query is `const query = `…``, all values bound via `clickhouse.Named()` — **no `fmt.Sprintf` in any reader SQL**, **no phantom rollup table references**); derivations live in `service.go` (rate folds, percentile interpolation, error-rate math, NaN scrub, top-N sorting). Most filter-bearing modules use an inline `WITH active_fps AS (SELECT [DISTINCT] fingerprint FROM observability.<signal>_resource PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd …)` CTE to pre-narrow before the main scan; trace-ID-scoped point lookups (most of `detail`, all of `paths`, `servicemap`, `shape`, `suggest`) skip the resource CTE because the trace_id+span_id PREWHERE is already maximal.

#### Logs read path ([internal/modules/logs/](internal/modules/logs/))

- [explorer/](internal/modules/logs/explorer/) — `POST /api/v1/logs/query`. List-only. Reads `observability.logs`; the inline `logs_resource` CTE is emitted **only when a resource-side filter is present** — when `f.Services`/`Hosts`/`Pods`/`Containers`/`Environments`/`Exclude*` are all empty the query drops the CTE and PREWHEREs `(team_id, ts_bucket)` directly. Search predicate uses `hasToken(body, lower(@search))` (default) or `lower(body) LIKE concat('%', lower(@search), '%')` (exact mode) against the `idx_body_text` native text (inverted) index.
- [logdetail/](internal/modules/logs/logdetail/) — `GET /api/v1/logs/:id`. Single-log deep link. The `:id` is the row's `log_id` — a stable FNV-64a hex hash of `(trace_id, timestamp_ns, body, fingerprint)` computed by the ingestion mapper ([internal/ingestion/logs/mapper.go::computeLogID](internal/ingestion/logs/mapper.go)) and stored on the `observability.logs` row. Repository PREWHEREs `(team_id, log_id = @logID)` only — the `idx_log_id` bloom-filter skip-index defined inline in [02_logs.sql](db/clickhouse/02_logs.sql) prunes granules across the full 30-day partition set without needing a `ts_bucket` bound.
- [trace_logs/](internal/modules/logs/trace_logs/) — `GET /api/v1/logs/trace/:traceID?limit=1000`. All logs for a trace via two `const` queries: step 1 reads `observability.trace_index` (PREWHERE leads on `trace_id`, single granule) to resolve `(min ts_bucket, max ts_bucket, fingerprint set)`; step 2 scans `observability.logs` PREWHEREd on three PK slots `(team_id, ts_bucket BETWEEN, fingerprint IN @fps)` plus the trace_id row-side check. Handler rejects empty `:traceID`; storage is canonical lowercase hex from `hex.EncodeToString`, so comparisons are direct, no `lowerUTF8` per row. `service.GetByTraceID` orchestrates the two-step lookup and maps via existing `models.MapLogs`. Canonical "all logs for a trace" path; the FE consumes this directly on the trace-detail page.
- [facets/](internal/modules/logs/facets/) (package `log_facets`) — `POST /api/v1/logs/facets`. **One** CH query: a 4-arm `UNION ALL` over `observability.logs_resource`, one arm per resource dim (service / host / pod / environment). Severity is a static label list returned from `models.SeverityLabels` — closed enum, no DB call. Service folds the unioned rows into `models.Facets` by `dim`.
- [trends/](internal/modules/logs/trends/) (package `log_trends`) — `POST /api/v1/logs/summary` and `POST /api/v1/logs/trend` (peer endpoints; the old composite `/logs/trends` is removed — frontend fires both in parallel). `Summary` reads SQL-side `count() / countIf(severity_bucket >= 4) / countIf(severity_bucket = 3)`; `Trend` groups by `timebucket.DisplayGrainSQL(window)` at display grain (1m / 5m / 1h / 1d) — payload size bounded regardless of window length. Both readers emit the `active_fps` CTE only when a resource filter is present.
- [filter/](internal/modules/logs/filter/) — typed `Filters` + `AttrFilter` + `Validate()` (≤30-day clamp, `searchMode="ngram"` default) + `BuildClauses(f) → (resourceWhere, where, args)`. Single shared SQL emitter consumed by all filter-bearing logs repos. When `resourceWhere == ""` callers skip the `active_fps` CTE. Stable bind names (`services`/`excServices`/`hosts`/`severities`/`traceID`/`spanID`/`search`/`akey_N`/`aval_N`) so identical predicate sets produce byte-identical SQL.
- [shared/models/](internal/modules/logs/shared/models/) — output models: `Log`, `LogRow`, `Cursor`, `Facets`, `Summary`, `TrendBucket`, `PageInfo`, `EncodeLogID`, `ParseLogID`, `MapLog(s)`, `LogColumns` (the canonical SELECT projection).

#### Traces read path ([internal/modules/traces/](internal/modules/traces/))

- [explorer/](internal/modules/traces/explorer/) — `POST /api/v1/traces/query`. **List-only**; no composite `include`. Root-span PREWHERE selects per-trace summary rows. Reads `observability.spans` with `spans_resource` CTE. (Single-trace summary `GET /api/v1/traces/:traceId` lives in `detail.GetTraceSummary` — all `/traces/:traceId/*` routes are owned by `detail`.)
- [facets/](internal/modules/traces/facets/) — `POST /api/v1/traces/facets`. Top-N (service / operation / http_method / http_status / status) over the filtered window via `topK(N)` aggregates in a single scan against `observability.spans_1m` with `spans_resource` CTE. Peer to `/traces/query`; FE fans out facets and trend in parallel.
- [trend/](internal/modules/traces/trend/) — `POST /api/v1/traces/trend`. Time-bucketed `(total, errors)` over the filtered window grouped by stored 5-min `ts_bucket`. Reads `observability.spans_1m` with `spans_resource` CTE. Peer to `/traces/query` and `/traces/facets`.
- [span_query/](internal/modules/traces/span_query/) — `POST /api/v1/spans/query`. Span-level explorer view (vs trace-grouped). Reads `observability.spans` with `spans_resource` CTE.
- [detail/](internal/modules/traces/detail/) — `GET` per-trace drill-downs: `/traces/:id` (summary card via `GetTraceSummary`), `/traces/:id/spans`, `/spans/:id/tree`, `/traces/:id/span-events`, `/traces/:id/spans/:spanId/attributes`, `/traces/:id/related`. Canonical 6-file layout — one `Service` + one `Handler`, no server-side composer. The FE fans the always-on reads in parallel; sibling [paths/](internal/modules/traces/paths/), [shape/](internal/modules/traces/shape/), and [logs/trace_logs/](internal/modules/logs/trace_logs/) are hit directly. Most queries are trace-id-scoped point lookups (no resource CTE). `GetTraceSummary` is two-phase: step 1 reads `observability.trace_index` (single-granule PK lookup leading on trace_id) to resolve `(ts_bucket bounds, fingerprint set)`; step 2 narrows the raw-spans scan to those PK slots and projects the root span. `GetRelatedTraces` is the lone window-scoped query with the apm-style `spans_resource` CTE plus PREWHERE on `(team_id, ts_bucket, fingerprint IN active_fps, service, name)`. Reads `observability.spans` only — per-span logs are not a separate endpoint; FE filters from the per-trace logs response at [/api/v1/logs/trace/:traceID](internal/modules/logs/trace_logs/). Service owns derivation: events parsing (`splitEventRows` + `parseEventJSON`), `db.statement` literal normalization (`normalizeDBStatement`), parent-chain Subtree walk (`filterSubtree`), span-link JSON parsing (`parseSpanLinks`).
- [paths/](internal/modules/traces/paths/) — `GET /traces/:id/critical-path`, `/traces/:id/error-path`. Trace-id-scoped queries (no resource CTE). Service runs longest-path DAG traversal + error-chain walk.
- [servicemap/](internal/modules/traces/servicemap/) — `GET /traces/:id/service-map`, `/traces/:id/errors`. Trace-id-scoped. Service folds spans into service-map nodes/edges and groups errors by exception_type.
- [shape/](internal/modules/traces/shape/) — `GET /traces/:id/flamegraph`, `/traces/:id/span-kind-breakdown`. Trace-id-scoped. Service builds flamegraph trees with self-time computation.
- [suggest/](internal/modules/traces/suggest/) — `POST /traces/suggest`. Autocomplete: `IsScalarField` validates the canonical scalar-field name set up front; `fetch` dispatches `@`-prefix → `SuggestAttribute` (JSONExtractString from `attributes`) vs scalar → `SuggestScalar` (closed-set column expression). Both queries PREWHERE `(team_id, ts_bucket)`.
- [filter/](internal/modules/traces/filter/) — typed `Filters` (`Services`/`Operations`/`SpanKinds`/`HTTPMethods`/`HTTPStatuses`/`Statuses`/`Environments`/`PeerServices`/`TraceID`/`MinDurationNs`/`MaxDurationNs`/`HasError` + `Exclude*` + `Attributes []AttrFilter`) + `AttrFilter` + `Validate()` + `BuildClauses(f)`. Mirrors `logs/filter`, shared by `explorer` / `span_query`. (Span-side error analytics live in [services/errors](internal/modules/services/errors/) — there is no separate traces/errors module.)

Trace-id-scoped queries match with a plain `trace_id = @traceID` row predicate; handlers reject an empty `:traceId` path param with 400, and ingestion canonicalizes storage (`hex.EncodeToString` for case, `zeroOut` collapses the all-zero sentinel to `""`). Each repo binds args via a local `traceIDArgs(teamID, traceID)` helper at the bottom of `repository.go` (no shared package).

#### Metrics read path ([internal/modules/metrics/](internal/modules/metrics/))

- [explorer/](internal/modules/metrics/explorer/) — `GET /metrics/names`, `GET /metrics/:metricName/tags`, `POST /metrics/explorer/query`. Reads `observability.metrics_1m` with the `metrics_resource` CTE; `ListMetricNames` uses a `WITH names AS (... metrics_resource …)` candidate-narrowing CTE then projects metric metadata. Aggregation expressions emit via `buildAggExpr` (rate divisor uses `strconv.FormatInt`, not `fmt.Sprintf`). Service `convertFEQuery` maps the FE `[{key, operator, value}]` filter shape into `filter.Filters`, validates, hands off to repo; `buildColumnarResult` folds flat per-bucket rows into the columnar `{timestamps, series}` wire shape.
- [filter/](internal/modules/metrics/filter/) — typed `Filters{Tags []TagFilter, Aggregation, …}` + `TagFilter{Key, Operator, Value}` + `Validate()` (≤30-day clamp, `validAggregations`), `Canonical()` (alias map: `service.name`→`service`, `host.name`→`host`, `deployment.environment`→`environment`, `k8s.namespace.name`→`k8s_namespace`), `ResourceColumn()` (the 4 flat columns on `metrics_resource`), `AttrColumn()` (sanitized `attributes.\`<key>\`::String` JSON-path on `metrics_1m`), `GroupByColumn()`, `SanitizeKey()`, `BuildClauses(f) → (resourceWhere, where, args)`. Stable bind names per canonical resource key; arbitrary attribute filters use indexed `mf0`/`mf1`/… binds. (Deviates from the per-module-helper-at-the-bottom-of-repository.go convention logs/traces follow — chosen for metrics so canonicalization, sanitization, and clause emission share one home.)

#### AI observability read path ([internal/modules/aiobservability/](internal/modules/aiobservability/))

- `GET /api/v1/ai/llm/{request-rate-by-model,latency-by-model,error-rate-by-model,token-usage-by-model,token-usage-by-provider,cost-by-model,cost-by-provider,top-expensive-calls,top-slow-calls}`. Explicit GenAI LLM panels only, no generic summary or timeseries endpoint. Request-rate, latency, error-rate, and token-usage reads use existing `observability.metrics_1m` for `gen_ai.client.operation.duration` and `gen_ai.client.token.usage`; cost and top-call endpoints read raw `observability.spans` because cost and trace identity live in span attributes.
- `GET /api/v1/ai/prompts/{usage-by-prompt,usage-by-version,latency-by-version,token-usage-by-version,cost-by-version,traces}`. Reads raw spans and extracts Datadog prompt-tracking JSON (`_dd.ml_obs.prompt_tracking`) plus generic `gen_ai.prompt.*` / `prompt.*` fallbacks. It does not project prompt or response body content.
- `GET /api/v1/ai/agents/{runs-by-agent,tool-calls-by-tool,tool-errors-by-tool,tool-latency-by-tool}` and `/api/v1/ai/retrieval/{request-rate-by-store,latency-by-store,errors-by-store}`. Reads raw spans filtered by `gen_ai.operation.name` (`invoke_agent`, `execute_tool`, `retrieval`) and groups by explicit agent, tool, or store dimensions.
- `GET /api/v1/ai/traces/query` and `/api/v1/ai/facets`. List and facet GenAI span views over raw `observability.spans`; limit is capped by `httputil.MaxPageSize`.

#### Services read path ([internal/modules/services/](internal/modules/services/))

- [deployments/](internal/modules/services/deployments/) — `GET /deployments/latest-by-service`, `/deployments/list`, `/deployments/compare`, `/deployments/timeline`, `/deployments/impact`, `/deployments/active-version`. Reads version traffic, timeline, and impact directly from `observability.spans_1m` based on `service_version` changes.
- [errors/](internal/modules/services/errors/) — `GET /errors/{service-error-rate,error-volume,latency-during-error-windows,groups}`, `/errors/groups/:groupId{,/traces,/timeseries}`, `/spans/{exception-rate-by-type,error-hotspot,http-5xx-by-route}`, `/errors/fingerprints{,/trend}`. 12 repo methods, each with `*All` and `*ByService` siblings; reads `observability.spans` with `spans_resource` CTE. `service-error-rate` and `latency-during-error-windows` share the same `ServiceErrorRateRows*` repo query — service.go discards zero-error rows for the latency view.
- [latency/](internal/modules/services/latency/) — `GET /services/latency/histogram`, `/services/latency/heatmap`. Reads `observability.spans` with `spans_resource` CTE; heatmap is 2D bucketing of (display_bucket × duration_bucket).
- [redmetrics/](internal/modules/services/redmetrics/) — RED summary + Apdex + per-route + breakdowns. 11 repo methods. Latency-percentile methods (`GetSummary`, `GetTopSlowOperations`, `GetP95LatencyTimeSeries`) read `observability.spans_1m` and project `quantileTimingMerge(q)(latency_state)`; Apdex / error-by-route / span-kind / etc. read raw `observability.spans` (Apdex computes exact `countIf(duration_nano <= @satisfiedNs)` / `countIf(duration_nano > @satisfiedNs AND duration_nano <= @toleratingNs)` buckets, which require raw-span granularity). `*ByService` split (no inline `query += ...`). The service-level error-rate timeseries lives at `/errors/service-error-rate` (services/errors), not under `/spans/red/`.
- [topology/](internal/modules/services/topology/) — `GET /api/v1/services/topology[?service=<focus>]`. Two queries (`GetNodes`, `GetEdges`) fanned via `errgroup`. Both read `observability.spans_1m` with `spans_resource` CTE. `GetEdges` derives directed edges from `kind_string = 'Client'` spans where `service` and `peer_service` are non-empty and distinct. P50/P95/P99 (and P50/P95 for edges) project `quantileTimingMerge(q)(latency_state)` server-side. Service classifies node health (`unhealthyErrorRate=0.05`, `degradedErrorRate=0.01` → `healthy`/`degraded`/`unhealthy`), `addMissingEdgeNodes` injects pure-CLIENT producers as nodes, optional 1-hop `filterNeighborhood` for the focus filter.

#### Infrastructure read paths ([internal/modules/infrastructure/](internal/modules/infrastructure/))

Apm-style across 9 submodules. Seven modules (`connpool`, `cpu`, `disk`, `jvm`, `kubernetes`, `memory`, `network`) read `observability.metrics_1m` with the `metrics_resource` CTE. Two modules (`nodes`, `fleet`) read `observability.spans_1m` with the `spans_resource` CTE for per-host / per-pod RED aggregates with P95 via `quantileTimingMerge(0.95)(latency_state)`.

- **Resolver narrowing**: `WITH active_fps AS (SELECT fingerprint FROM observability.metrics_resource WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name [= @metricName | IN @metricNames])`. Main query PREWHEREs `(team_id, ts_bucket, fingerprint IN active_fps, metric_name)`.
- **Repository contract**: queries only, **one CH call per method**. Each module declares its own `metricArgs` / `withMetricName{,s}` / `spanArgs` / `spanBucketBounds` helpers locally. Reads OTel semconv 1.30+ canonical attribute paths inline (e.g., `attributes.'system.cpu.state'::String`, `attributes.'jvm.memory.pool.name'::String`, `attributes.'k8s.container.name'::String`).
- **Service derivation**: counter→rate computed server-side via `sum(...) / @bucketGrainSec` per `timebucket.DisplayGrainSQL` bucket; gauge avg via SQL-side `avg(val_sum / val_count)` per display bucket; JVM GC duration P50/P95/P99 via server-side `quantilesPrometheusHistogramMerge` on `metrics_1m.latency_state`; multi-metric percentage blends (cpu 3-metric, memory 4-metric, disk 3-metric, connpool 5-metric); `≤1.0 → *100` percentage normalization; NaN scrub via `utils.SanitizeFloat`; RED `error_rate = errCount × 100 / reqCount` and `avg_latency_ms = durationMsSum / reqCount` for `nodes` / `fleet` (computed from `spans_1m`'s `request_count`, `error_count`, `duration_ms_sum` SimpleAggregateFunction columns).
- [infraconsts/](internal/modules/infrastructure/infraconsts/) — shared OTel metric-name + column-name constants. Not a routable module.

Panels assume OTel semconv 1.30+ canonical attribute names. Older instrumentations that emit non-canonical attribute spellings will return empty for those panels until upgraded.

#### Kafka saturation read path ([internal/modules/saturation/kafka/](internal/modules/saturation/kafka/))

Apm-style across 3 actor-aligned panel submodules — `producer`, `consumer`, `client` — plus a 9-route `explorer/` composer. All 18 panel routes live under `/api/v1/saturation/kafka/...` and read `observability.metrics_1m` via the `metrics_resource` CTE — no panel touches `observability.spans`. Each repo method binds its OTel metric-name set as a closed const internally and pulls shared bind helpers (`MetricArgs` / `WithMetricName{,s}` / `WithOpAliases`) from [filter/](internal/modules/saturation/kafka/filter/).

- [filter/](internal/modules/saturation/kafka/filter/) — shared SQL/OTel/fold helpers. `args.go` (`MetricArgs`, `WithMetricName{,s}`, `WithOpAliases`, `MetricBucketBounds`); `otel.go` (canonical metric constants + `Producer/Consumer/Process/ConsumerLag/Rebalance/Duration` alias arrays + `Publish/Receive/Process` operation aliases); `fold.go` (`FoldCounterRateByDim`, `FoldErrorRateByPair`, `SecondsToMs`, `FormatTime`).
- [internal/shared/](internal/modules/saturation/kafka/internal/shared/) — HTTP-side helpers: `routes.go` exposes `SaturationRoutePrefix = "/saturation/kafka"` + `RegisterGET`/`RegisterGroup`; `handler.go` exposes `HandleRangeQuery(c, getTenant, errMessage, query)` — the closure-based range-query helper free-function form, reused by every submodule's handler.
- [producer/](internal/modules/saturation/kafka/producer/) — 3 routes (`/produce-rate-by-topic`, `/publish-latency-by-topic`, `/publish-errors`). Repo: `QueryPublishRateByTopic`, `QueryPublishLatencyByTopic`, `QueryPublishErrorsByTopic`. Latency reader OR-matches `messaging.client.operation.duration` filtered by `messaging.operation.name IN @opAliases` so instrumentations emitting only the unified op-duration metric still light up.
- [consumer/](internal/modules/saturation/kafka/consumer/) — 10 routes (`/consume-rate-by-topic`, `/receive-latency-by-topic`, `/consume-rate-by-group`, `/process-rate-by-group`, `/process-latency-by-group`, `/consume-errors`, `/process-errors`, `/consumer-lag-by-group`, `/lag-per-partition`, `/rebalance-signals`). Repo: 10 endpoint-named methods. `partitionLagTopN = 200` cap lives at the top of `repository.go` (only consumer/ uses it). Rebalance fold switches on `metric_name` to populate the 6 fields of `RebalancePoint`. Owns the exported `PartitionLag` DTO that `explorer/` re-imports for its two stubbed partition handlers.
- [client/](internal/modules/saturation/kafka/client/) — 5 routes (`/summary-stats`, `/e2e-latency`, `/broker-connections`, `/client-op-duration`, `/client-op-errors`). `summary-stats` issues 5 repo calls in series; `e2e-latency` issues one multi-metric repo call (`metric_name IN [publish, receive, process].duration`) and folds to `(display_bucket, topic) → {publishP95, receiveP95, processP95}` triples.
- [explorer/](internal/modules/saturation/kafka/explorer/) — 9 catalog routes (`/saturation/kafka/{summary, topics, groups, topic/overview, topic/groups, topic/partitions, group/overview, group/topics, group/partitions}`). Owns its own `repository.go` with two `argMax(value, timestamp)`-shape readers (`QueryTopicMetricSamples` / `QueryConsumerMetricSamples`) that collapse each metric time series down to its latest value per `(topic|group, metric_name)` tuple — the right shape for table-style catalog views vs the panel modules' time-series shape. Reads the JVM `kafka.consumer.*` metric family (different namespace from the OTel `messaging.*` family the panels use). Service does the wide-pivot Go-side, folding flat `(dim, metric_name, value)` rows into `KafkaTopicRow{BytesPerSec, Lag, Lead, …}` / `KafkaGroupRow{AssignedPartitions, CommitRate, …}`. Mirrors the database-explorer placement pattern.

Service derivations: rate = sum(value) ÷ `timebucket.DisplayGrain(window).Seconds()` per (display_bucket, dim); histogram P50/P95/P99 via server-side `quantilesPrometheusHistogramMerge(0.5,0.95,0.99)(latency_state)` per (display_bucket, dim) — service multiplies by 1000 to convert seconds → ms; gauge averages via custom multi-key folds; partition-lag is server-side `argMax(value, timestamp)` + DESC LIMIT 200; rebalance fold switches on `metric_name` to populate the 6 fields of `RebalancePoint`. Composite endpoints: `summary-stats` issues 5 repo calls in series; `e2e-latency` and `rebalance-signals` each use one multi-metric repo call with `metric_name` projected as a row dim.

Error-rate panels depend on OTel semconv 1.30+ `error.type` data-point attribute. Older kafka instrumentations don't emit it; those panels return empty for affected services.

#### DB saturation read path ([internal/modules/saturation/database/](internal/modules/saturation/database/))

Apm-style across 9 submodules — `volume`, `errors`, `latency`, `collection`, `system` (single-DB scoped), `systems` (cross-system list), `summary` (top-level KPIs), `slowqueries`, `connections` — plus `explorer` (composer over the others, no repo of its own).

- [filter/](internal/modules/saturation/database/filter/) — typed `Filters` (DBSystem/Collection/Namespace/Server) + OTel constants (`AttrDB*` / `MetricDB*`) + `BuildSpanClauses(f)` + `BuildMetricClauses(f)` + `SpanArgs/SpanBucketBounds` (1-min) + `MetricArgs/MetricArgsMulti/MetricBucketBounds` (1-min). Latency percentiles + per-second rates + display-grain bucketing are all computed server-side (`timebucket.DisplayGrainSQL` + `WithBucketGrainSec`).
- **Source-table choice (per OTel semconv 1.30+)**:
  - **Spans-side (8 submodules)** — `volume / errors / latency / collection / system / systems / summary / slowqueries`: each DB call is one span. Non-percentile reads (counts / errors / read-vs-write / heatmaps) and `db_statement`-grouped slowqueries panels (`GetSlowQueryPatterns`, `GetP99ByQueryText`, `GetSlowQueryRate` with its dynamic threshold, `collection.GetCollectionQueryTexts`) read raw `observability.spans` with the `spans_resource` CTE. The free-text-grouped readers compute percentiles inline server-side via `quantileTiming` / `quantilesTiming(...)(duration_nano / 1e6)` (no `-Merge`, no rollup state). **Rollup percentile reads** (`GetLatencyBy{System,Operation,Collection,Namespace,Server}`, `GetCollectionLatency`, `GetSystemLatency`, `GetSystemTopCollectionsBy{Latency,Volume}`, `GetMainStats` p95/p99, `GetSystemSummariesRaw` p95, `GetSlowestCollections` p99) read `observability.spans_1m` and project `quantileTimingMerge(q)(latency_state)`. Filter helpers split accordingly: `BuildSpanClauses` / `SpanGroupColumn` for raw-spans queries, `BuildSpans1mClauses` / `Spans1mGroupColumn` for `spans_1m`.
  - **Metrics-side (`connections` only)** — the OTel `db.client.connection.*` family is instrumentation-side gauges/counters/histograms, not per-call spans. Reads `observability.metrics_1m` with `metrics_resource` CTE. 8 methods: count / max / idle.max / idle.min / pending_requests (gauges, raw `value`), timeouts (counter → service folds + per-second rate), wait_time / create_time / use_time (OTel histograms; CH computes per-(display_bucket, pool.name) p50/p95/p99 server-side via `quantilesPrometheusHistogramMerge` on `latency_state`, service multiplies by 1000 for seconds→ms). The `summary` and `systems` submodules are two-phase composites that fan out spans aggregates + metrics-side active-connection gauge in parallel via errgroup, joining on `db_system`.
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

Treat the manifest and package contents as the source of truth.

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

- **services**: `deployments`, `errors`, `redmetrics`, `latency`, `topology`
- **traces**: `explorer`, `detail`, `span_query`, `facets`, `paths`, `servicemap`, `shape`, `suggest`, `trend` (+ `filter` shared package)
- **logs**: `explorer`, `logdetail` (registered as `log_detail`), `log_facets`, `log_trends`, `trace_logs` (+ `filter` and `shared/models` packages)
- **metrics**: `explorer` (+ `filter` shared package)
- **ai_observability**: explicit GenAI routes under `/api/v1/ai/{llm,prompts,agents,retrieval,traces,facets}`; existing `metrics_1m` + raw spans, no dedicated AI rollup table
- **infrastructure**: `connpool`, `cpu`, `disk`, `jvm`, `kubernetes`, `memory`, `network`, `fleet`, `nodes` (+ `infraconsts` shared package)
- **saturation**: `kafka/{producer, consumer, client, explorer}` (+ `kafka/filter` and `kafka/internal/shared` packages); `database/{collection, connections, errors, explorer, latency, slowqueries, summary, system, systems, volume}` (+ `database/filter` and `database/internal` packages)
- **user**: `auth`, `team`, `user`
- **ingestion** (background runners + gRPC registrars): spans, logs, metrics

53 modules total.

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
| [internal/shared/](internal/shared/) | Shared contracts and helpers: contracts, errorcode, httputil. |
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

### Alerting read+write path ([internal/modules/alerting/](internal/modules/alerting/))

The alerting platform owns monitor CRUD, notification channels/policies/templates, the in-process evaluator, and Slack dispatch. The package is **flat-vs-nested compliant**: `alerting/` holds only subdirectories.

- [monitors/](internal/modules/alerting/monitors/) — `GET|POST /api/v1/monitors`, `GET|PUT|DELETE /api/v1/monitors/:id`, `POST /api/v1/monitors/:id/{ack,mute,unmute,test}`, `GET /api/v1/monitors/:id/{events,series,status-timeline}`, `GET /api/v1/monitors/activity`. Files split by responsibility: `repository.go` (CRUD) + `repository_events.go` (event history + state actions), `service.go` (validation + JSON marshaling) + `service_actions.go` (ack/mute/test/series/status-timeline/events/activity), `handler.go` (CRUD endpoints) + `handler_actions.go` (state + read endpoints), `models.go` + `models_events.go`. The `test` endpoint runs `query.Scalar` once via the injected `query.Registry` and returns `{value, has_data, would_decide_as, threshold}` without persisting state. Series queries call `query.Backend.Series` for bucketed timeseries; status-timeline derives bands from `monitor_events`.
- [notifications/](internal/modules/alerting/notifications/) — `GET|POST /api/v1/notifications/{channels,policies,templates}`, `GET|PUT|DELETE /api/v1/notifications/channels/:id`, `POST /api/v1/notifications/channels/:id/test`, `GET /api/v1/notifications/integrations`. Channel deletion returns 409 when a monitor's `notify_json` still references it (`JSON_TABLE` join). Integrations catalog is static — Slack is "connected", every other type is "not_connected". `Service.TestChannel` synthesizes a Payload mirroring a real "Alerting" trigger and dispatches through the shared `dispatch.Dispatcher`.
- [evaluator/](internal/modules/alerting/evaluator/) — `registry.BackgroundRunner`. `Module.Start()` spawns one goroutine, 10s ticker; per tick: `repo.LoadDue(now, 500)` (single MySQL hit, ordered by `next_evaluation_at`), `errgroup` fan-out with `SetLimit(16)` per-eval, `query.Scalar` against CH under `database.DashboardCtx` (3s budget), `expr.Decide` returns the new status, CAS-checked `UPDATE monitor_state` to avoid clobbering acks. On transitions (`ok->alert`/`warn`, `alert/warn->ok`) writes `monitor_events` + dispatches synchronously. Muted monitors evaluate but skip dispatch. Failures log via `slog` and never bubble up — one bad monitor can't kill the tick.
- [dispatch/](internal/modules/alerting/dispatch/) — `Transport` interface + `SlackWebhook` impl + `Stub` impl. `DefaultDispatcher` selects by `channel.Type`: Slack uses the live HTTP transport (Slack legacy attachment format, color by severity, 5s timeout, 1 retry on 5xx); everything else falls to Stub (logs + succeeds).
- [shared/](internal/modules/alerting/shared/) — only subdirectories:
  - `models/` — DB row types (`MonitorRow`, `MonitorStateRow`, `MonitorEventRow`, `ChannelRow`, `PolicyRow`, `TemplateRow`), wire types (`Scope`, `Conditions`, `MonitorQuery`, `NotifyTargets`, `SlackWebhookConfig`), enum lists.
  - `query/` — typed monitor query → CH SQL, one file per type. `Registry{Metric, APM, Log}` dispatches by monitor type. Every backend exposes `Scalar` (single-value eval, used by evaluator + `monitors.Test`) and `Series` (bucketed timeseries, used by `monitors.Series`). `metric.go` reads `observability.metrics_1m` via `metrics_resource` CTE; `apm.go` reads `spans_1m` via `spans_resource` CTE; `log.go` reads raw `observability.logs` with `hasToken` body search.
  - `expr/decide.go` — pure decide function with hysteresis (`RecoveryThreshold`), `NoDataAs` disposition, renotify-interval check.
  - `template/template.go` — single-pass mustache subset for `{{value}}`, `{{threshold}}`, `{{service.name}}` and status sections (`{{#is_alert}}…{{/is_alert}}`, etc.). No external dep.

MySQL schema appended to [db/mysql/mysql.sql](db/mysql/mysql.sql): `monitors`, `monitor_state`, `monitor_events`, `notification_channels`, `notification_policies`, `notification_templates`. All idempotent `CREATE TABLE IF NOT EXISTS` under `observability.*`. No ClickHouse schema changes.

Wired in [internal/app/server/modules_manifest.go](internal/app/server/modules_manifest.go) via three constructors: `alerting_monitors.NewModule(sqlDB, getTenant, nativeQuerier)`, `alerting_notifications.NewModule(sqlDB, getTenant)`, `alerting_evaluator.NewModule(sqlDB, nativeQuerier)`. The evaluator is a `BackgroundRunner` picked up automatically by the existing `app.startBackgroundModules` loop.

**Scope limits (v1):** Anomaly + Synthetic monitor types are not implemented (the UI also omits them). Non-Slack transports ship as "Install" stubs (PagerDuty / Opsgenie / MS Teams / Twilio / Jira / Email / Generic-webhook UI cards render but route through `dispatch.Stub`). Routing policies CRUD ships but the evaluator does not consult them — it dispatches directly to a monitor's configured channels.

## Related docs

- Overview: [README.md](README.md)
- Flow diagrams: [docs/flows/](docs/flows/) — ingestion, auth, http-request, trace-assembly
- Endpoint latency audit: [docs/endpoint_latency_audit.md](docs/endpoint_latency_audit.md) — per-route latency-class buckets (A/B/C/D/E) derived from budget tier + table + amplifiers
- JSON serialization audit: [docs/json_serialization_audit.md](docs/json_serialization_audit.md) — every `toJSONString(attributes)` site, the per-row cost, and the recommendation
- Ingest dashboard: [monitoring/grafana/dashboards/optikk_ingest.json](monitoring/grafana/dashboards/optikk_ingest.json)
- ClickHouse schema overview: [db/clickhouse/README.md](db/clickhouse/README.md)
