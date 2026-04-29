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

`internal/ingestion/` owns OTLP ingest for spans, logs, and metrics. The pipeline is intentionally minimal — **no app-side accumulator, no per-partition workers, no Dispatcher/Worker generics, no pause/resume backpressure, no retry loop**. The mental model is the canonical six-stage flow:

```
gRPC  →  otlp_proto  →  project_proto  →  kafka publish  →  kafka consume  →  CH write
```

Producer-side batching is delegated to franz-go (`ProducerLinger` + `ProducerBatchMaxBytes`); consumer batching is just whatever a single `PollFetches` returns; ClickHouse `async_insert=1, wait_for_async_insert=1` provides server-side coalescing on top.

Each signal is a flat 7-file Go package — no `ingress/`, `mapper/`, `enrich/`, `consumer/`, `dlq/`, `module/` subdirectories. The only subdir is `schema/` which holds the protoc-generated proto + pb.go.

```
internal/ingestion/{spans,logs,metrics}/
  schema/         span_row.proto + span_row.pb.go (generated wire format)
  handler.go      gRPC OTLP Export — auth, mapper, producer.Publish
  mapper.go       OTLP → []*schema.Row (folds the previous mapper_*.go + enrich/*.go)
  producer.go     ProducerConfig{Topic, Partitions, Replicas, RetentionHours} + Producer
  consumer.go     ConsumerConfig{Topic, ConsumerGroup} + Consumer (decodes records → writer)
  writer.go       chTable + chColumns + chValues + Insert (CH async_insert=1, wait=1)
  dlq.go          DLQ — republishes original record bytes to optikk.dlq.{signal}
  module.go       registry.Module wiring (RegisterGRPC + Start/Stop the consumer goroutine)
```

**Hot path (gRPC → Kafka)**: handler resolves teamID from ctx → `mapRequest` walks ResourceX → ScopeX → record level → produces `[]*schema.Row` (resource fingerprint via `internal/infra/fingerprint`, attribute caps via `internal/infra/otlp.TypedAttrs` / `CapStringMap`, hot-attribute promotion to dedicated CH columns) → `producer.Publish(ctx, rows)` marshals every row to proto, builds N `*kgo.Record` (key = teamID for sticky per-team partitioning, topic = `optikk.ingest.{signal}`), submits as one async batch, waits for all acks via `kafka.Producer.PublishBatch`. Failure to publish surfaces to the OTLP caller as `codes.Unavailable`.

**Cold path (Kafka → ClickHouse)**: consumer.Run delegates to `kafka.Consumer.Run(ctx, handle)` which `PollFetches` in a loop and hands every polled batch to `handle`. `handle` decodes each `*kgo.Record` into `*schema.Row` (malformed records are logged once and dropped, not DLQ'd — they can't be replayed usefully), passes the slice to `writer.Insert`. Writer prepares one CH batch, appends N rows, sends under `clickhouse.WithSettings(async_insert=1, wait_for_async_insert=1)`. **On any failure** (`prepare`/`append`/`send`) the consumer fires `dlq.PublishAll(ctx, recs, err)` which republishes the original `kgo.Record.Value` bytes verbatim to `optikk.dlq.{signal}` (with `x-dlq-reason` + `x-dlq-signal` headers), then returns nil so offsets commit and the partition keeps moving — **single failure → DLQ, no retry loop**.

**Shared kafka primitives** (`internal/infra/kafka/`, flat — no subpackages):
- `client.go` — `Config{Brokers, LingerMs, BatchMaxBytes, Compression}`, `NewProducerClient`, `NewConsumerClient`. Producer opts: `ProducerLinger` (default 20ms), `ProducerBatchMaxBytes` (default 1 MiB), `MaxBufferedRecords(1<<18)`, `RequiredAcks(AllISRAcks)`, `StickyKeyPartitioner`, ZSTD compression. Consumer opts: `ConsumerGroup`, `ConsumeTopics`, `DisableAutoCommit`, `CooperativeStickyBalancer`, `FetchMaxWait(2s)`.
- `producer.go` — `Producer{client *kgo.Client}`. `PublishBatch` (async fan-out + WaitGroup, first-error returned), `PublishSync` (one record, used by DLQ fast paths if needed), `Flush`, `Close`, `Client`.
- `consumer.go` — `Consumer{client *kgo.Client}`. `Run(ctx, RecordHandler)` is the sole entry point: PollFetches → drain → call handler → on nil error CommitRecords; on error log + leave uncommitted (handler does its own DLQ-and-return-nil for at-least-once semantics).
- `topics.go` — `TopicSpec{Name, Partitions, Replicas, RetentionHours}` + `EnsureTopics(ctx, brokers, specs)`. Idempotent `kadm.CreateTopics` (TopicAlreadyExists = success). Called once at app boot for every signal + DLQ topic. Helpers: `IngestTopic(prefix, signal)`, `DLQTopic(dlqPrefix, signal)`. Signal-name constants `SignalSpans`/`SignalLogs`/`SignalMetrics`.
- `observability.go` — kgo `Hooks()` (produce/fetch counter, broker-connect, group-manage-error) + `LagPoller` (15s ticker, `optikk_kafka_consumer_lag_records` via raw `kmsg` MetadataRequest + OffsetFetchRequest + ListOffsetsRequest). One LagPoller per ingest consumer, started by app.Start as run.Group actors.

**Topology config** (`internal/config/ingestion.go`):
```go
type IngestionConfig struct {
    Spans   SignalConfig
    Logs    SignalConfig
    Metrics SignalConfig
}
type SignalConfig struct {
    Partitions     int    // default 8
    Replicas       int    // default 1 (dev) / 3 (prod)
    RetentionHours int    // default 24
    ConsumerGroup  string // default "optikk-ingest.{signal}.consumer"
}
```
`Config.IngestSignal(name)` returns the merged config (zero values inherit `SignalDefaults`). YAML lives under `ingestion.{spans,logs,metrics}.*`. Topic names derive from `kafka.topic_prefix` (default `optikk.ingest`) + signal; DLQ from `kafka.dlq_prefix` (default `optikk.dlq`) + signal.

**Producer/Consumer wiring** (`internal/app/server/infra.go::buildIngest`): one shared `*kgo.Client` for production (so all three signals share one TCP pool to brokers), one `*kgo.Client` per consumer group. `kafka.EnsureTopics` runs once at boot for the 6 topics (3 ingest + 3 DLQ). For each signal, the bootstrap constructs Producer/Writer/DLQ/Consumer/Handler explicitly and passes them via `signal.Deps{Handler, Consumer}` into `signal.NewModule`. The module's `Start` launches `consumer.Run` in a goroutine; `Stop` cancels the consumer ctx and joins.

**Approx LOC**: Hand-written Go is ~1,900 LOC (~600 spans + ~440 logs + ~470 metrics + ~400 infra/kafka), down from ~5,200 in the previous Dispatcher/Worker/Accumulator architecture (~64% reduction). Generated `*_row.pb.go` files contribute ~1,200 LOC, untouched.

**Scale envelope** (single backend instance, 3-broker Redpanda + single-node ClickHouse):
- gRPC OTLP `Export` ingress: ~50K req/s (proto unmarshal + mapper allocation bound)
- Mapper → Row: ~300K rows/s (proto marshal + map flatten bound)
- `producer.PublishBatch`: ~500K rec/s (kgo limit; batching hides linger latency under concurrency)
- Kafka cluster: ~1M rec/s/topic (broker disk + replication bound)
- Consumer per partition: ~50K rec/s (network + proto decode bound)
- CH `async_insert=1, wait=1`: ~200K-1M rows/s aggregate (merge tree write + MV propagation bound)

**Realistic E2E ceiling per backend node: ~150–300K rows/s/signal**, dominated by mapper allocation + CH insert latency. Scales horizontally by adding backend replicas (each joins the same consumer group → partitions rebalance via cooperative-sticky). Spans is the tightest signal (~1.5KB/row); metrics the loosest (~300B/row).

Shared OTLP helpers in `internal/infra/otlp/`:
- `protoconv.go` — `AttrsToMap`, `ResourceFingerprint`, `AnyValueString`, `BytesToHex`.
- `typed_attrs.go` — single-pass `TypedAttrs(kvs, maxStringKeys)` → (str,num,bool,dropped) + `CapStringMap(m, max)`. Dropped counts fan into `optikk_ingest_mapper_attrs_dropped_total{signal}`.

Local development uses Redpanda from [docker-compose.yml](docker-compose.yml).

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

`monitoring/` ships a local Prometheus + Grafana pair for ingest-pipeline dashboards:

- **Ports (bespoke — chosen to avoid colliding with the main compose)**: Prometheus `:19091`, Grafana `:13001`.
- **Bring-up**: `docker compose -f monitoring/stack/docker-compose.yml up -d`.
- **Scrape target**: `host.docker.internal:19090` (the Go backend's `/metrics`, 5s interval).
- **Directory layout** (flat-vs-nested rule — `monitoring/` holds only subdirs):
  - `monitoring/stack/` — `docker-compose.yml`, `prometheus.yml`.
  - `monitoring/grafana/dashboards/optikk_ingest.json` — starter dashboard (ingest rate, handler/worker/CH latency, queue depth, paused partitions, flush reasons, DLQ rate, consumer lag, attr drops).
  - `monitoring/grafana/provisioning/{datasources,dashboards}/*.yml` — auto-registration so no UI setup is needed.

This is the primary monitoring stack for local development — all ingest metrics flow here via Prometheus scrape.

### Observability / monitoring stack

Self-telemetry is Prometheus-only. The customer-facing OTLP receiver on `:4317` is unrelated — it ingests external spans/metrics/logs from clients and has its own pipeline (`internal/ingestion/`).

**Metrics** — `promauto` collectors exposed on `/metrics`, scraped by the local Prometheus + Grafana stack (`monitoring/stack/`).

- `internal/infra/metrics/` — `http.go`, `grpc.go`, `db.go`, `kafka.go`, `auth.go`, `ingest.go` — all `promauto`-registered collectors.
- `internal/infra/redis/metrics_hook.go` — Redis-specific collectors via a `goredis.Hook`.
- `internal/infra/middleware/metrics.go` — `HTTPMetricsMiddleware` populates `optikk_http_*` (route label = Gin `FullPath()` template, method, status class).
- `internal/app/server/grpc_metrics.go` — unary + stream interceptors populate `optikk_grpc_*` (full method, canonical gRPC code).
- `internal/infra/kafka/observability/observability.go` — franz-go `kgo.Hooks` (produce/fetch/broker/group-error) + `LagPoller` (`optikk_kafka_consumer_lag_records` via raw `kmsg` calls). One `LagPoller.Run(ctx)` per ingest consumer, started by `app.Start`.
- `internal/infra/database/{clickhouse,mysql}_instrument.go` + `instrument_common.go` — `SelectCH/QueryCH/ExecCH` + `SelectSQL/GetSQL/ExecSQL` seam that emits `optikk_db_*` for every query.

**Logs** — `slog.InfoContext/…Context` records fan out through `internal/shared/slogx.FanoutHandler` so additional sinks (file, syslog) can be added without touching call sites. Currently the fanout wraps a single stdout leg (tint for local dev, JSON when `LOG_FORMAT=json`).

**Grafana dashboards** (`monitoring/grafana/dashboards/`) — provisioned read-only, one file per concern:
- `optikk_overview.json` — service-wide health tiles + top-level timeseries (start here).
- `optikk_http_api.json` — **per-API** HTTP dashboard: QPS/latency/error rate with route + method template variables; top-10 slowest and error-prone routes.
- `optikk_grpc.json` — per-method gRPC dashboard (OTLP ingest surface + any other gRPC).
- `optikk_db.json` — MySQL + ClickHouse query rate/latency/errors split by `system` + `op`.
- `optikk_redis.json` — Redis commands/sec, latency, error ratio (from the metrics hook).
- `optikk_kafka.json` — produce/consume rate, produce latency, consumer lag per partition, rebalances, broker connects.
- `optikk_ingest.json` — OTLP ingest pipeline (records/sec by signal, handler/worker/CH latency, queue depth, paused partitions, DLQ rate, attr drops).

### Traces read path (split into sibling submodules)

The trace read surface used to live in two monoliths (`explorer` + `tracedetail`); it is now split by concern so each submodule owns its own `dto/handler/models/module/repository/service` bundle:

- `internal/modules/traces/explorer/` — core list + single: `POST /api/v1/traces/query`, `GET /api/v1/traces/:traceId`. Reads `observability.signoz_index_v3` (with `is_root = 1` + column aliases that shape rows into a per-trace summary). The `traces_index` table referenced in older docs is not implemented; the read path falls through to raw spans.
- `internal/modules/traces/trace_analytics/` — `POST /api/v1/traces/analytics` (group-by + aggregations over traces_index).
- `internal/modules/traces/span_query/` — `POST /api/v1/spans/query` (span-level explorer view over `observability.spans`).
- `internal/modules/traces/tracedetail/` — per-span drill-downs: `/traces/:id/span-events`, `/traces/:id/spans/:spanId/attributes`, `/traces/:id/logs`, `/traces/:id/related`, plus the spans list/tree (`/traces/:id/spans`, `/spans/:id/tree`).
- `internal/modules/traces/trace_shape/` — "shape" of a trace: `/traces/:id/flamegraph`, `/traces/:id/span-kind-breakdown`. Self-times are computed on the frontend from the span list returned by `/traces/:id/bundle`; there is no server-side `/traces/:id/span-self-times` endpoint.
- `internal/modules/traces/trace_paths/` — chain analysis: `/traces/:id/critical-path`, `/traces/:id/error-path`.
- `internal/modules/traces/trace_servicemap/` — per-trace aggregates: `/traces/:id/service-map`, `/traces/:id/errors`.
- `internal/modules/traces/trace_suggest/` — DSL autocomplete: `POST /traces/suggest` for field/attribute value completion on the traces query bar.
- `internal/modules/traces/filter/filter.go` — `Filters` (typed shape: `Services / Operations / SpanKinds / HTTPMethods / HTTPStatuses / Statuses / Environments / PeerServices / TraceID / Search / SearchMode / Attributes []AttrFilter / MinDurationNs / MaxDurationNs / HasError *bool` + `Exclude*` siblings + `TeamID/StartMs/EndMs`) and `AttrFilter` (`Key / Op / Value`). `Validate()` clamps the window to ≤ 30 days, defaults `SearchMode` to `"ngram"`, rejects missing `startTime`. Decoded off the wire directly (frontend sends typed JSON; no parse step). `BuildClauses(f) → (resourceWhere, where, args)` is the single shared SQL emitter consumed by explorer / span_query / errors — stable bind names (`services` / `excServices` / `httpStatuses` / `peerServices` / etc.) so identical predicate sets produce byte-identical SQL — CH plan cache can hit. Mirrors `internal/modules/metrics/filter/` and `internal/modules/logs/filter/`.
- The previous `internal/modules/traces/querycompiler/`, `internal/modules/traces/shared/resource/`, `internal/modules/traces/shared/traceidmatch/`, and `internal/modules/traces/shared/rootspan/` packages were all deleted. The 5 trace-ID-scoped submodules (`tracedetail`, `trace_paths`, `trace_servicemap`, `trace_shape`, `trace_suggest`) are now apm-style: every repo method is `const query = `…``, every method PREWHEREs `team_id = @teamID` (and additional PK slots — `span_id` for `GetSpanLogs`/`GetSpanAttributes`/`Subtree`'s start CTE; `service` + `name` for `GetRelatedTraces`), and the `traceIDMatchPredicate` SQL fragment that handles OTLP empty/all-zero-hex/case-insensitive forms is **inlined as a const at the bottom of each repository.go** (with a per-file `traceIDArgs(teamID, traceID)` helper). Service owns derivation: `trace_paths` runs longest-path DAG traversal + error-chain walk; `trace_shape` builds flamegraph trees with self-time computation; `trace_servicemap` folds spans into service-map nodes/edges and groups errors; `tracedetail` parses events+exceptions (`splitEventRows` + `parseEventJSON`), normalizes `db.statement` literals (`normalizeDBStatement`), Subtree-walks the parent chain (`filterSubtree`); `trace_suggest.IsScalarField` validates the canonical scalar-field name set up front so the repo can string-concat the closed-set column expression safely. `tracedetail.GetRelatedTraces` is the only window-scoped query of the five — it gets the apm `WITH active_fps AS (... spans_resource PREWHERE team_id, ts_bucket, service ...)` CTE plus main-scan PREWHERE on `(team_id, ts_bucket, fingerprint IN active_fps, service, name)`. **No `fmt.Sprintf`, no `%s`, no shared SQL-fragment helpers** anywhere in these 5 submodules.
- `internal/modules/services/errors/` no longer routes through this resolver. Each repository method that accepts `serviceName` writes its own inline `WITH active_fps AS (SELECT fingerprint FROM observability.spans_resource WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND service = @serviceName)` CTE and adds `s.fingerprint IN active_fps` to PREWHERE. Repo is queries-only — derivations (error rate, avg latency, status-bucket → code, group-ID hash resolution) live in `service.go`. Methods are split `*All` vs `*ByService`, no single function contains two queries, and the prior 60s `groupResolveStore` cache is gone.

### Bucket consistency invariant

Every bucket value used in a PREWHERE — `ts_bucket_start` for spans/logs, `bucket_ts` for metrics tier dictionaries, `bucket_ts_hour` / `_6hr` / `_day` for raw `observability.metrics`, and the display-time time-bucket column emitted by every reader — is computed Go-side via [internal/infra/timebucket/](internal/infra/timebucket/). ClickHouse never computes a bucket itself: no `toStartOfHour`, `toStartOfInterval`, `toStartOfDay`, `toStartOfMinute`, or `toStartOfFiveMinutes` appears in any SQL we emit or any schema we define. Writers compute the bucket and write it to a column; readers compute the same bucket from the same Go helper to derive PREWHERE bounds; cross-language drift cannot break the contract because the cross-language path doesn't exist. Round-trip enforced by [internal/infra/timebucket/consistency_test.go](internal/infra/timebucket/consistency_test.go). Changing a constant (`BucketSeconds`, `BucketSeconds`) is a breaking schema change requiring a table rebuild — runbook lives in the package doc.

### Spans schema (`observability.signoz_index_v3`)

Hot resource/span attributes are JSON typed-path subcolumns inside `attributes JSON(\`service.name\` LowCardinality(String), \`host.name\` LowCardinality(String), …, max_dynamic_paths=100)`. Type-hinted paths are stored as native subcolumns with column-equivalent read performance ([CH docs](https://clickhouse.com/docs/best-practices/use-json-where-appropriate)) — no JSON parse per row, no MATERIALIZED column tax at insert. Reader-facing names live as zero-cost `ALIAS` columns (`service_name`, `host_name`, `pod_name`, `service_version`, `deployment_environment`, `peer_service`, `db_system`, `db_name`, `db_statement`, `http_route`, `attr_exception_type`). `resource_fingerprint` is a real top-level column written directly by the Go mapper.

Resource-side skip indexes (service/host/pod/version/environment) are intentionally absent — the resolver pre-filters those into a `resource_fingerprint IN (...)` PREWHERE that hits the third PK slot directly. Bloom filters on the remaining span-attribute paths (`http.route`, `db.system`, `db.name`, `peer.service`, `exception.type`) target the JSON typed subcolumns directly (CH ≥ 24.12).

**No rollup tables exist.** `spans_red_rollup`, `spans_peer_rollup`, `spans_by_version_rollup`, `spans_error_fingerprint`, `signoz_index_v3_rollup_{1m,5m,1h}`, `signoz_index_v3_host_rollup_*`, `db_histograms_rollup`, `root_spans_index`, `traces_index`, and `observability.resources` were planned but never built — comments referencing them are documentation drift. All "aggregation panel" readers go to raw `signoz_index_v3` and rely on resolver narrowing for performance.

### Logs read path (split into sibling submodules)

The logs read surface used to live in a single `explorer/` package; it is
now split by responsibility, mirroring the traces decomposition. Each
sibling owns its own handler/service/repository/dto bundle.

- `internal/modules/logs/explorer/` — `POST /api/v1/logs/query`. Orchestrator: list (own repo) + optional `summary` / `facets` / `trend` includes fanned in parallel via `errgroup` against sibling services through `FacetsClient` / `TrendsClient` interfaces. Response = max of all branches.
- `internal/modules/logs/logdetail/` — `GET /api/v1/logs/:id` (single-log deep link).
- `internal/modules/logs/log_analytics/` — `POST /api/v1/logs/analytics` (group-by grid + agg projections over raw logs).
- `internal/modules/logs/log_facets/` — `POST /api/v1/logs/facets` (top-N per dim: severity, service, host, pod, environment).
- `internal/modules/logs/log_trends/` — `POST /api/v1/logs/trends` (Summary KPIs + severity-bucketed time-series).
- See [internal/modules/logs/filter/](internal/modules/logs/filter/) above for the typed `Filters` + `AttrFilter` + `Validate()` + `BuildClauses(f)` shape consumed by every filter-bearing logs repo. The previous `internal/modules/logs/querycompiler/` package was deleted along with `Compiled`/`Target`/`FromStructured`/`DroppedClauses` — every dynamic-DSL repo method consumes `filter.Filters` directly.
- `internal/modules/logs/filter/` — `Filters` + `AttrFilter` + `Validate()` (typed wire shape, ≤30-day clamp, `searchMode="ngram"` default) plus `BuildClauses(f) → (resourceWhere, where, args)`, the single shared SQL emitter consumed by `explorer` / `log_facets` / `log_trends`. Stable bind names (`services` / `excServices` / `hosts` / `excHosts` / `pods` / `containers` / `environments` / `severities` / `excSeverities` / `traceID` / `spanID` / `search` / `akey_N` / `aval_N`) so identical predicate sets produce byte-identical SQL — CH plan cache can hit. Mirrors `internal/modules/metrics/filter/` and `internal/modules/traces/filter/`.
- `internal/modules/logs/shared/models/` — output models: `Log`, `LogRow`, `Cursor`, `FacetValue`, `Facets`, `Summary`, `TrendBucket`, `PageInfo`, `AnalyticsRow`, `Aggregation`, plus `EncodeLogID` / `ParseLogID` / `MapLog(s)` and the `RawLogsTable` + `LogColumns` constants every reader projects into. (Filter shape and `BuildClauses` moved out to `logs/filter/`.)
- `internal/modules/logs/shared/analytics/` — agg spec builders (`BuildAggsRaw` / `BuildAggsRollupVolume` / `BuildAggsRollupFacets`) + dynamic CH row helpers (`ScanAnyRow`, `MapRow`, `AliasSet`, `ToString`, `ToFloat`).

**Apm-style discipline**: Each filter-bearing repo method takes `models.Filters` directly and calls a private `buildFilterClauses(f models.Filters) (resourceWhere, where, args)` helper at the bottom of its `repository.go` (one per module — explorer, log_facets, log_trends — no shared helper code). The helper returns the inline-CTE fragment + main-WHERE fragment + named bindings. The query is assembled by **plain string concatenation** (`+`) — no `fmt.Sprintf`, no `%s` placeholders. Each method runs **one query** with an inline `WITH active_fps AS (SELECT DISTINCT fingerprint FROM observability.logs_resource PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd <resourceWhere>)` CTE; main scan PREWHEREs `(team_id, ts_bucket, fingerprint IN active_fps)`. All values bind via `clickhouse.Named()` — user input never enters the SQL string. Repos hold queries only — derivations live in service: `log_facets` folds raw rows into per-dim top-N maps; `log_trends.Summary` reads SQL-side `count() / countIf(severity_bucket >= 4) / countIf(severity_bucket = 3)` directly; `log_trends.Trend` does step resolution + `formatBucket` + map fold + sort (`resolveStepMinutes` / `formatBucket` live as private functions inside `log_trends/service.go`). `logdetail.GetByID` is fully `const` (no CTE; the `(team_id, trace_id, span_id)` PREWHERE is already maximal).

### Service topology read path

Apm-style. One route — `GET /api/v1/services/topology[?service=<focus>]`. Repository holds queries only; service computes percentiles, error-rates, and health classification. **No quantile aggregate runs in ClickHouse** — both queries emit a fixed-bucket histogram array, and Go-side `quantile.FromHistogram` interpolates P50/P95/P99 from it.

- [internal/modules/services/topology/repository.go](internal/modules/services/topology/repository.go) — two `const query` methods, each with inline `WITH active_fps AS (SELECT fingerprint FROM observability.spans_resource PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd)` CTE; main scan PREWHEREs `(team_id, ts_bucket, fingerprint IN active_fps)` for full PK granule pruning.
    - `GetNodes`: per-service RED — `count()`, `countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)`, plus a 13-element `Array(UInt64)` latency histogram emitted as a chain of `countIf(duration_nano …)` clauses aligned with `latencyBucketBoundsMs` in `service.go`.
    - `GetEdges`: derives directed edges from `kind_string = 'Client'` spans where `service` and `peer_service` are non-empty and distinct, emitting the same RED + histogram shape per `(source, target)`. Edge latency = client-side call duration (network + remote service time).
  Repo-local `spanArgs(teamID, startMs, endMs)` and `spanBucketBounds(startMs, endMs)` helpers mirror the per-module pattern in `services/latency`. **No `fmt.Sprintf`, no phantom-rollup column references**: the previous query referenced `latency_ms_digest` / `client_service` / `server_service` which don't exist in this schema (no rollup tables) — same broken-against-non-existent-rollup problem `services/latency` had pre-refactor.
- [internal/modules/services/topology/service.go](internal/modules/services/topology/service.go) — `latencyBucketBoundsMs = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 1e18]` ms upper bounds, kept in lockstep with the repository's `countIf` chain. `buildNodes` / `buildEdges` interpolate P50/P95/P99 via `quantile.FromHistogram(latencyBucketBoundsMs, row.Buckets, q)`, derive error-rate (`error_count / count`), classify node health (`unhealthyErrorRate=0.05` / `degradedErrorRate=0.01` thresholds → `healthy` / `degraded` / `unhealthy`). `GetTopology` orchestrates: errgroup fan-out of `GetNodes` + `GetEdges`, `addMissingEdgeNodes` to inject pure-CLIENT producers, and optional 1-hop `filterNeighborhood` for the `?service=<focus>` query param.
- [internal/modules/services/topology/dto.go](internal/modules/services/topology/dto.go) — `nodeAggRow` and `edgeAggRow` carry the service identifiers + `Buckets []uint64 \`ch:"bucket_counts"\``; no percentile fields on the scan structs (computed Go-side).
- Spans schema: PK = `(team_id, ts_bucket, fingerprint, service, name, timestamp, trace_id, span_id)` per [db/clickhouse/01_spans.sql](db/clickhouse/01_spans.sql); `spans_resource` PK = `(team_id, ts_bucket, fingerprint)` per [db/clickhouse/04_resources.sql](db/clickhouse/04_resources.sql). `ts_bucket` is `UInt64` 5-minute-aligned via [timebucket.BucketStart](internal/infra/timebucket/timebucket.go).

### Metrics explorer read path

Apm-style, structured like logs/traces. The `explorer` submodule (`internal/modules/metrics/explorer/`) owns the full Module (handler/service/repo/dto/models/module.go); `internal/modules/metrics/filter/` is a sibling subpackage. Three routes — `GET /metrics/names` (autocomplete), `GET /metrics/:metricName/tags` (key+value autocomplete), `POST /metrics/explorer/query` (main aggregation). Repository exposes `ListMetricNames`, `ListTagKeys`, `ListTagValues`, `QueryTimeseries`. **Every query PREWHEREs the PK** — `(team_id, ts_bucket)` on raw `metrics` and `(team_id, ts_bucket)` on `metrics_resource`. Wire format keeps the **dynamic** `where: [{key, operator, value}]` shape — metrics is a generic open-dimension explorer (any tag, any operator), so promoting some keys to typed top-level fields would create an asymmetric API.

- [internal/modules/metrics/filter/filter.go](internal/modules/metrics/filter/filter.go) — typed `Filters` + `TagFilter` + `Validate()` (clamps to ≤30 days, validates aggregation), `Canonical()` (alias map: `service.name`→`service`, `host.name`→`host`, `deployment.environment`→`environment`, `k8s.namespace.name`→`k8s_namespace`), `ResourceColumn()` (4 flat columns on `metrics_resource`), `AttrColumn()` (sanitized `attributes.\`<key>\`::String` JSON-path on raw `metrics`), `GroupByColumn()`, `SanitizeKey()`, and `BuildClauses(f) → (resourceWhere, where, args)` which partitions `f.Tags` into the resource-CTE fragment and the row-side WHERE fragment. **Stable bind names**: `services` / `excServices` / `hosts` / `excHosts` / `environments` / `excEnvironments` / `k8sNamespaces` / `excK8sNamespaces` for the 4 canonical resource keys; `mf0`/`mf1`/… (indexed) for arbitrary attribute filters. Resource clauses are emitted in fixed canonical order, so identical filter shapes produce byte-identical SQL — CH plan cache can hit. (Deviates from the per-module-helper-at-the-bottom-of-repository.go convention logs/traces follow — chosen for metrics so canonicalization, sanitization, and clause emission share one home.)
- [internal/modules/metrics/explorer/repository.go](internal/modules/metrics/explorer/repository.go) — every query is `const query = \`…\``, every query PREWHEREs the PK:
    - `ListMetricNames`: single statement with `WITH names AS (SELECT DISTINCT metric_name FROM observability.metrics_resource PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd WHERE metric_name ILIKE @search)` candidate-narrowing CTE → outer SELECT PREWHEREs raw `metrics` on `(team_id, ts_bucket, metric_name IN names)` for type/unit/description lookup. Two-phase but one round-trip; the dictionary CTE is tiny.
    - `ListTagKeys`: PREWHEREs raw metrics on `(team_id, ts_bucket, metric_name)`, then `mapKeys(JSONAllPathsWithTypes(attributes))` for dynamic-keys side; UNION ALL with constant resource-keys list.
    - `ListTagValues` resource path: PREWHEREs `metrics_resource` on `(team_id, ts_bucket, metric_name)`, projects the 4 flat LowCardinality columns.
    - `ListTagValues` data-point path: PREWHEREs raw metrics on `(team_id, ts_bucket, metric_name)`, projects sanitized `attributes.\`<key>\`::String`.
    - `QueryTimeseries`: inline `WITH active_fps AS (SELECT fingerprint FROM observability.metrics_resource PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name = @metricName <resourceWhere>)` CTE → outer SELECT PREWHEREs raw metrics on `(team_id, ts_bucket, fingerprint IN active_fps, metric_name)`. One round-trip per filtered query.
  Aggregation expressions emit via `buildAggExpr` (rate divisor uses `strconv.FormatInt`, not `fmt.Sprintf`). Repo-local `metricBucketBounds(startMs, endMs)` and `metricArgs(f filter.Filters)` helpers produce hour-aligned `[bucketStart, bucketEnd)` and the 6 base bind values. **No `fmt.Sprintf`, no `%s`, no preflight round-trip** — `internal/modules/metrics/shared/resource/` was deleted.
- [internal/modules/metrics/explorer/service.go](internal/modules/metrics/explorer/service.go) — `convertFEQuery` maps the FE `[{key, operator, value}]` filter shape into `filter.Filters`, `Validate()`s it, hands off to repo. `buildColumnarResult` folds flat per-bucket rows into the columnar `{timestamps, series}` wire shape the frontend expects. `ListTags` orchestrates `ListTagKeys` + `ListTagValues` per key into the `[{key, values}]` autocomplete response.
- Raw schema: [db/clickhouse/03_metrics.sql](db/clickhouse/03_metrics.sql) — `observability.metrics` PK = `(team_id, ts_bucket, fingerprint, metric_name, temporality, timestamp)`; carries a separate `resource JSON(max_dynamic_paths=100)` column (resource attributes only) alongside `attributes JSON(max_dynamic_paths=100)` (data-point attributes only) and flat top-level LowCardinality columns for the canonical resource keys (`service`, `host`, `environment`, `k8s_namespace`, `http_method`, `http_status_code`). The mapper at [internal/ingestion/metrics/mapper/mapper_points.go](internal/ingestion/metrics/mapper/mapper_points.go) writes them separately — there is no merge; resource attributes do not appear in `attributes`.
- Resource dictionary: [db/clickhouse/04_resources.sql](db/clickhouse/04_resources.sql) — `metrics_resource` PK = `(team_id, ts_bucket, metric_name, fingerprint)`, populated by MV from raw `observability.metrics`, with the canonical resource keys flattened to top-level `LowCardinality(String)` columns plus a stable `fingerprint` per (team, metric_name, resource-tuple). Hour-aligned `ts_bucket` via [timebucket.BucketStart](internal/infra/timebucket/timebucket.go).

### Infrastructure read paths (`infrastructure/*`)

Apm-style across all 10 submodules. Six modules (`network`, `cpu`, `disk`, `memory`, `jvm`, `kubernetes`) plus the direct query in `resourceutil` read `observability.metrics` via inline `metrics_resource` CTE; two modules (`nodes`, `fleet`) read `observability.spans` for RED-style aggregates via `spans_resource` CTE; `connpool` was already correct on `observability.metrics`.

- **Resolver narrowing**: every query opens with `WITH active_fps AS (SELECT fingerprint FROM observability.metrics_resource WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name [= @metricName | IN @metricNames])`. Main query PREWHEREs `(team_id, ts_bucket, fingerprint IN active_fps)` — first three slots of the metrics PK. Hour-aligned bounds via [timebucket.BucketStart](internal/infra/timebucket/timebucket.go). Spans-backed reads use the same shape with `spans_resource` and `timebucket.BucketStart` (5-minute aligned), mirroring [httpmetrics.QueryRouteAgg](internal/modules/services/httpmetrics/repository.go#L143-L167).
- **Repository contract**: queries only, **one CH call per method**. Each method's body: a `const query = \`...\`` string + named-param binding via local `metricArgs` / `withMetricName{,s}` helpers (each module declares its own — no shared helper) + `dbutil.SelectCH` / `QueryRowCH`. No `fmt.Sprintf`, no phantom-rollup column references (`value_sum`, `value_avg_num`, `sample_count`, `state_dim`), no `coalesce`/`nullIf` attribute alias chains. SQL reads OTel semconv 1.30+ canonical attribute paths inline (e.g., `attributes.'system.cpu.state'::String`, `attributes.'jvm.memory.pool.name'::String`, `attributes.'k8s.container.name'::String`).
- **Service derivation**: every fold/normalization/percentile that previously lived in the repo moved to the service. Counter→rate via [displaybucket.SumByTimeAndKey](internal/shared/displaybucket/displaybucket.go); gauge averages via custom multi-key folds; JVM GC duration P50/P95/P99 via [quantile.FromHistogram](internal/shared/quantile/quantile.go) merged across `(display_bucket, gc_name)`; multi-metric percentage blends (cpu 3-metric `system.cpu.utilization` + `system.cpu.usage` + `process.cpu.usage`; memory 4-metric `system.memory.utilization` + `system.memory.usage` + `jvm.memory.used` + `jvm.memory.max`; disk 3-metric `system.disk.utilization` + `disk.free` + `disk.total`; connpool 5-metric DB/Hikari/JDBC); `≤1.0 → *100` percentage normalization for utilization metrics; NaN scrubbing; RED `error_rate = errCount × 100 / reqCount` and `avg_latency_ms = durationMsSum / reqCount` derivation for `nodes` / `fleet`.
- **`resourceutil`** composite consumes `*Service` (not `Repository`) from cpu/memory/disk/network/connpool — wired in `resourceutil/module.go` via `cpu.NewService(cpu.NewRepository(db))` etc. The one direct CH query (`GetResourceUsageByService`) reads `observability.metrics` with `metric_name IN @metricNames` over `infraconsts.AllResourceMetrics` and folds per-service across the 5 utilization families.
- **Constraint**: panels assume OTel semconv 1.30+ canonical attribute names. Older instrumentations that emit non-canonical attribute spellings will return empty for those panels until upgraded — same contract as httpmetrics and kafka.

### Kafka saturation read path (`saturation/kafka`)

Apm-style. 19 routes under `/api/v1/saturation/kafka/...` (summary, produce/consume/process rate by topic/group, publish/receive/process latency, consumer lag, partition lag, rebalance signals, e2e latency, error rates, broker connections, client-op duration) all read [observability.metrics](db/clickhouse/03_metrics.sql). No panel touches `observability.spans` — the kafka surface is pure OTel-messaging-metric aggregation.

- **Resource resolver**: every query opens with `WITH active_fps AS (SELECT fingerprint FROM observability.metrics_resource WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name [IN @metricNames | = @metricName])`. Main query PREWHEREs `(team_id, ts_bucket, fingerprint IN active_fps)` — first three slots of the metrics PK — before any JSON-path read. Hour-aligned bounds come from [timebucket.BucketStart](internal/infra/timebucket/timebucket.go) (no CH-side `toStartOfHour`).
- **Repository contract** ([internal/modules/saturation/kafka/repository.go](internal/modules/saturation/kafka/repository.go)): queries only, **one CH call per method**. SQL templates `Sprintf`'d at package init so JSON-path expressions for topic / consumer_group / operation / partition / server.address / error.type live in one place. ~16 methods covering: counter time-series by topic/group, counter+error_type series, histogram time-series by topic/group/operation/(metric+topic), histogram aggregate (`sum(hist_sum)` + `sumForEach(hist_counts)`), counter aggregate, gauge time-series + max + per-(topic,partition,group) `argMax` snapshot, multi-metric rebalance projection, and consumer/topic sample queries.
- **Multi-name OTel aliases** preserved via `metric_name IN @metricNames` — `producerMetricAliases`, `consumerMetricAliases`, `consumerLagMetricAliases`, `rebalanceMetricAliases`, `processMetricAliases` from [otel_conventions.go](internal/modules/saturation/kafka/otel_conventions.go). Latency-by-topic / -by-group additionally OR-match `messaging.client.operation.duration` filtered by `messaging.operation.name IN @opAliases` so instrumentations that emit only the unified op-duration metric still light up the panels.
- **Service derivations** ([internal/modules/saturation/kafka/service.go](internal/modules/saturation/kafka/service.go)): rate = sum(value) ÷ `timebucket.DisplayGrain(window).Seconds()` per (display_bucket, dim); histogram P50/P95/P99 via `quantile.FromHistogram(buckets, counts, q)` after merging aligned `hist_buckets`/`hist_counts` per (display_bucket, dim); gauge averages via custom multi-key folds; partition-lag is server-side `argMax(value, timestamp)` + DESC LIMIT 200; rebalance fold switches on `metric_name` to populate the 6 fields of `RebalancePoint`. Composite endpoints: `summary-stats` issues 5 repo calls (CounterAgg×2, HistogramAgg×2, GaugeMax×1) in series; `e2e-latency` and `rebalance-signals` each use one multi-metric repo call with `metric_name` projected as a row dim.
- **Constraint**: error-rate panels depend on OTel semconv 1.30+ `error.type` data-point attribute. Older kafka instrumentations don't emit it; those panels return empty for affected services.

### DB saturation read path (`saturation/database`)

Apm-style across all 9 submodules — `volume`, `errors`, `latency`, `collection`, `system`, `systems`, `summary`, `slowqueries`, `connections` — plus `explorer` (composer over the others, no repo of its own). Replaces the previous broken implementation that pointed every query at a phantom `db_histograms_rollup` table referencing columns (`latency_ms_digest`, `value_sum`, `sample_count`, `db_operation`, `pool_name`, `db_connection_state`) that never existed on either raw table.

- [internal/modules/saturation/database/filter/filter.go](internal/modules/saturation/database/filter/filter.go) — typed `Filters` (DBSystem/Collection/Namespace/Server) + OTel constants (`AttrDB*` / `MetricDB*`) + `BuildSpanClauses(f)` + `BuildMetricClauses(f)` + `SpanArgs/SpanBucketBounds` (5-min) + `MetricArgs/MetricArgsMulti/MetricBucketBounds` (hour) + `BucketWidthSeconds` + `LatencyBucketBoundsMs` (`1/5/10/25/50/100/250/500/1000/2500/5000/10000/30000/+Inf` ms) + `LatencyBucketCountsSQL()` (closed-set `[countIf(duration_nano …) …]` array emitter). Mirrors `internal/modules/metrics/filter/`.
- **Source-table choice (per OTel semconv 1.30+)**:
  - **Spans-side (8 submodules)** — `volume / errors / latency / collection / system / systems / summary / slowqueries`: each DB call is one span on `observability.spans` with `duration_nano`, `has_error`, `response_status_code`, flat `db_system` / `db_name` / `db_statement` columns, plus `attributes.'db.operation.name' / 'db.collection.name' / 'db.namespace' / 'server.address' / 'error.type'::String` typed-paths. Every method runs `WITH active_fps AS (SELECT fingerprint FROM observability.spans_resource PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd)` then PREWHEREs the main scan on `(team_id, ts_bucket, fingerprint IN active_fps)`. Latency-bearing methods emit a fixed-bucket histogram `Array(UInt64)` via `filter.LatencyBucketCountsSQL()`; service.go interpolates p50/p95/p99 via `quantile.FromHistogram(filter.LatencyBucketBoundsMs, …)`. Counts → per-second rate via `filter.BucketWidthSeconds`.
  - **Metrics-side (`connections` only)** — the OTel `db.client.connection.*` family is instrumentation-side gauges/counters/histograms, not per-call spans. Reads `observability.metrics` with `WITH active_fps AS (SELECT fingerprint FROM observability.metrics_resource PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name [= @metricName | IN @metricNames])` + main scan PREWHERE on `(team_id, ts_bucket, fingerprint IN active_fps, metric_name)`. 8 methods cover: count/max/idle.max/idle.min/pending_requests (gauges, raw `value`), timeouts (counter → service folds + per-second rate), wait_time/create_time/use_time (OTel native histograms with `hist_buckets` + `hist_counts`; service merges per `(display_bucket, pool.name)` and computes p50/p95/p99 via `quantile.FromHistogram`).
- **Service derivations** — count→rate, error_count/total → ratio percent, gauge avg via custom `(bucket, pool, state)` multi-key folds, top-N sorting + trimming Go-side, two-phase composites (`systems.GetSystemSummaries` fans out spans aggregates + metrics-side active-connection gauge in parallel via errgroup, joins on `db_system`).
- **Repos clean** — every method is `const query = \`…\`` (or string-concat with the closed-set `LatencyBucketCountsSQL()`). **No `fmt.Sprintf`, no `%s`, no phantom-rollup column references.** Old `internal/shared/{query.go, rollup.go, otel.go}` were deleted; the surviving `internal/shared/{http.go, routes.go}` carry only HTTP-layer helpers (`ParseFilters` / `ParseLimit` / `ParseThreshold` / `RequireCollection` / `RequireDBSystem` / `RegisterDualGET` / `RegisterDualGroup`) which are explicitly handler-side.
- **Spans schema**: PK = `(team_id, ts_bucket, fingerprint, service, name, timestamp, trace_id, span_id)` per [db/clickhouse/01_spans.sql](db/clickhouse/01_spans.sql).
- **Metrics schema**: PK = `(team_id, ts_bucket, fingerprint, metric_name, temporality, timestamp)` per [db/clickhouse/03_metrics.sql](db/clickhouse/03_metrics.sql); `metrics_resource` PK = `(team_id, ts_bucket, metric_name, fingerprint)` per [db/clickhouse/04_resources.sql](db/clickhouse/04_resources.sql).

### HTTP metrics read path (`services/httpmetrics`)

Mirrors the apm module: queries-only repository, derivation in service, single canonical source per endpoint (no fallbacks). 17 endpoints under `/api/v1/http/...`, served by 6 repository methods.

- **Metrics-backed endpoints (10)** — read [observability.metrics](db/clickhouse/03_metrics.sql) via the apm `WITH active_fps AS (SELECT fingerprint FROM observability.metrics_resource WHERE …)` CTE. Histogram percentiles are computed Go-side via [internal/shared/quantile/quantile.go](internal/shared/quantile/quantile.go) `FromHistogram(buckets, counts, q)`; SQL never calls `quantile()` on this path.
  - `GetRequestRate`, `GetStatusDistribution`, `GetErrorTimeseries` — `QueryStatusHistogramSeries(http.server.request.duration)` (status-keyed `hist_count` time series).
  - `GetRequestDuration`, `GetRequestBodySize`, `GetResponseBodySize`, `GetClientDuration`, `GetDNSDuration`, `GetTLSDuration` — `QueryHistogramAgg(<metric>)` (sums `hist_sum`/`hist_count`, returns merged `hist_buckets`/`hist_counts`).
  - `GetActiveRequests` — `QueryMetricSeries(http.server.active_requests)` (gauge series).
- **Spans-backed endpoints (7)** — read [observability.spans](db/clickhouse/01_spans.sql) via `WITH active_fps AS (SELECT DISTINCT fingerprint FROM observability.spans_resource WHERE …)` CTE plus `ts_bucket BETWEEN @bucketStart AND @bucketEnd` PREWHERE for granule pruning aligned to the spans sort key. P95 is computed in SQL via ClickHouse's native `quantile(0.95)(duration_nano)` aggregator.
  - `GetTopRoutesByVolume`, `GetTopRoutesByLatency`, `GetRouteErrorRate` — single `QueryRouteAgg` call (`route, req_count, p95_ms, err_count`); service sorts/slices for each top-N panel.
  - `GetRouteErrorTimeseries` — `QueryRouteErrorSeries`, pre-aggregated by `ts_bucket × route` in SQL; service rebuckets to display grain.
  - `GetTopExternalHosts`, `GetExternalHostLatency`, `GetExternalHostErrorRate` — single `QueryExternalHostAgg` call (`peer_service`, `req_count`, `p95_ms`, `err_count`); service sorts/slices.
- Service uses [internal/shared/displaybucket](internal/shared/displaybucket/displaybucket.go) (`SumByTimeAndKey`, `AvgByTime`) for time-bucket folding and `slices.SortFunc` for top-N selection. Bucket bounds via [internal/infra/timebucket](internal/infra/timebucket/timebucket.go) (`BucketStart` for metrics, `BucketStart` for spans).

### RED metrics read path (`services/redmetrics`)

Apm-style: 10 repository methods, queries-only, all on raw `observability.spans` via the `WITH active_fps AS (SELECT DISTINCT fingerprint FROM observability.spans_resource WHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd)` CTE; main scan PREWHEREs `(team_id, ts_bucket, fingerprint IN active_fps)`. Span-side P95/P50/P99 use `quantileTiming(q)(duration_nano / 1000000.0)` (matching the post-quantileTiming-swap pattern). Apdex computes exact `countIf(duration_nano <= @satisfiedNs)` / `countIf(duration_nano > @satisfiedNs AND duration_nano <= @toleratingNs)` buckets in SQL — no Go-side percentile interpolation. Errors-by-route resolves the OTel canonical `attributes.'http.route'::String` and falls back to `operation_name` only when empty (single inline `if(...)`, not two queries). `GetTopSlowOperations` uses a two-step CTE: candidates picked by `count()` first, percentiles computed only over the bounded set. Service-name scoping uses the `*ByService` split (no inline `query += ...`); for Apdex this means `GetApdex` (all) and `GetApdexByService`. Service holds derivations: `RPS = count / BucketSeconds`, `error_pct = errors*100/total`, NaN scrub via `utils.SanitizeFloat`, latency-breakdown pct-of-total, Apdex score `(satisfied + tolerating*0.5) / total` and `frustrated = total - satisfied - tolerating`. Local helpers: `spanArgs` and `spanBucketBounds`.

### SLO read path (`services/slo`)

Apm-style: 8 repository methods (4 `*All` + 4 `*ByService` pairs) plus a `WindowCountsRow` shape for `errorRateForWindow`. All on raw `observability.spans` via the `spans_resource` CTE. P95 latency uses `quantileTiming(0.95)(duration_nano / 1000000.0)`. The `serviceName` filter lives **inside** the CTE (`AND service = @serviceName`) and the main `WHERE` — no `query += serviceNameFilter` inline concat. Repo never calls `time.Now()`; the clock lives on `SLOService.now` (defaulted to `time.Now`, swappable for tests). Service computes availability (`(total-errs)*100/total`), avg latency (`durationMsSum/total`), cumulative error-budget burn over per-bucket rows, the 3-way `errgroup` for burn-rate (fast 5m + slow 1h + summary). Burn-rate window timestamps are derived in service from the injected clock and passed as `(sinceMs, untilMs)` args. Local helpers: `spanArgs` and `spanBucketBounds`.

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
- traces: `explorer`, `trace_analytics`, `span_query`, `tracedetail`, `trace_shape`, `trace_paths`, `trace_servicemap`, `trace_suggest`, `errors`, `filter`
- logs: `explorer`, `logdetail`, `log_analytics`, `log_facets`, `log_trends`, `shared/{models,analytics}`
- metrics
- services: `topology`, `deployments`
- infrastructure: `connpool`, `cpu`, `disk`, `fleet`, `jvm`, `kubernetes`, `memory`, `network`, `nodes`, `resourceutil`, `infraconsts` (shared constants, not a routable module)
- saturation: `kafka`, `database/{filter, collection, connections, errors, explorer, latency, slowqueries, summary, system, systems, volume}`
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
- Ingest dashboard: [monitoring/grafana/dashboards/optikk_ingest.json](/Users/ramantayal/Desktop/pro/optikk-backend/monitoring/grafana/dashboards/optikk_ingest.json)
- Frontend sibling repo: [../optikk-frontend/README.md](/Users/ramantayal/Desktop/pro/optikk-frontend/README.md)
