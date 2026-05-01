# Optik Backend — Claude Code Instructions

## Before any task

1. Read **`CODEBASE_INDEX.md`** (repo root) — the canonical map of modules, ingestion, and architecture.
2. **Plan first.** For non-trivial work (anything beyond a one-line fix), write a plan via `EnterPlanMode` and wait for approval before editing.

## After every iteration

After completing any task — no matter how small — review and update the following if anything changed:

1. **`CODEBASE_INDEX.md`** — new modules, endpoints, helpers, or schema.
2. **This file (`CLAUDE.md`)** — new quick-reference paths or principles.
3. **`db/clickhouse/README.md`** — new migration files or schema-level facts.

This is **mandatory**. Documentation must always reflect the current architecture.

## Quick reference

### Stack and entry points

- **Stack**: Go, Gin, gRPC, ClickHouse, MySQL, Redis, Kafka/Redpanda. OTLP gRPC ingest surface only — self-telemetry is Prometheus on `/metrics`.
- **Server entry**: [cmd/server/main.go](cmd/server/main.go) → [internal/app/server/app.go](internal/app/server/app.go).
- **Module registration**: [internal/app/server/modules_manifest.go](internal/app/server/modules_manifest.go) → `configuredModules()`. Single `/api/v1` route group with `TenantMiddleware` — no cached/uncached split, no response-cache middleware.

### Shared helpers (use these, don't reinvent)

- **HTTP**: [internal/shared/httputil/base.go](internal/shared/httputil/base.go) — `RespondOK`, `RespondError`, `RespondErrorWithCause`, `ParseRequiredRange`, `ParseRange`, `ParseComparisonRange`, `WithComparison`, `MaxPageSize=200`.
- **Error codes**: [internal/shared/errorcode/codes.go](internal/shared/errorcode/codes.go).
- **ClickHouse query budgets**: [internal/infra/database/clickhouse.go](internal/infra/database/clickhouse.go) — three tiers via `DashboardCtx` (3s, 1 GiB, priority 1), `OverviewCtx` (15s, 2 GiB, priority 5), `ExplorerCtx` (60s, 8 GiB, priority 10). All three enable CH per-shard query cache (60s TTL, per-user). Pick the lowest budget that fits — repeat queries within 60s come from cache regardless of tier.
- **DB instrumentation seam**: `dbutil.SelectCH/QueryCH/ExecCH` (and `SelectSQL/GetSQL/ExecSQL`) emit `optikk_db_{queries_total,query_duration_seconds}` with the `op` label inferred from the call site — pass a meaningful `op` (e.g. `"logsDetail.GetByID"`).
- **Quantiles**: [internal/shared/quantile/quantile.go](internal/shared/quantile/quantile.go) — `FromHistogram(buckets, counts, q)` for percentile interpolation from fixed-bucket histograms.
- **Display-time bucketing**: [internal/shared/displaybucket/displaybucket.go](internal/shared/displaybucket/displaybucket.go) — `SumByTime`, `AvgByTime`, `SumByTimeAndKey`, `AvgByTimeAndKey`.

### Time bucketing — single source of truth

[internal/infra/timebucket/timebucket.go](internal/infra/timebucket/timebucket.go) owns all bucket math.

- `BucketSeconds = 300` — **5-minute grain**. `BucketStart(unixSeconds) → uint32` truncates to a 5-minute boundary; all three signals (spans/logs/metrics) store `ts_bucket UInt32` Unix-seconds at this grain.
- `DisplayBucket(rowUnixSeconds, windowMs)` / `DisplayGrain(windowMs)` are window-adaptive: ≤3h → 1m, ≤24h → 5m, ≤7d → 1h, else 1d. Display-time aggregation is a separate concept from partition-prune buckets.
- **ClickHouse never computes a bucket itself** in any reader SQL — no `toStartOfHour`, `toStartOfInterval`, `toStartOfDay`, `toStartOfMinute`, `toStartOfFiveMinutes`. The single exception is `metrics_1m_mv` in `07_metrics_1m.sql`, which derives `ts_bucket` server-side at MV evaluation; the value still matches `timebucket.BucketStart`.
- Display-time aggregation in `SELECT` / `GROUP BY` (e.g. `toStartOfInterval(timestamp, INTERVAL @stepMin MINUTE)`) is permitted — no row is being matched against a bucket value, so no cross-language drift risk.
- Changing `BucketSeconds` is a breaking schema change — drop the `observability` database on dev clusters and let migrations re-run.

### ClickHouse schema (8 migration files, no rollup tiers)

See [db/clickhouse/README.md](db/clickhouse/README.md) for the per-file table.

- `observability.spans` ([01_spans.sql](db/clickhouse/01_spans.sql)) — raw OTLP spans. PK `(team_id, ts_bucket, fingerprint, service, name, timestamp, trace_id, span_id)`. `attributes JSON(max_dynamic_paths=100)` typed-paths; ALIAS columns for hot reader names. **No rollup tables exist for spans** — readers go to raw spans and rely on `spans_resource` to narrow `fingerprint IN (...)`.
- `observability.logs` ([02_logs.sql](db/clickhouse/02_logs.sql)) — raw OTLP logs. PK `(team_id, ts_bucket, fingerprint, timestamp)`. `log_id String` column + `idx_log_id` bloom-filter skip-index. The `log_id` deep-link id is a stable FNV-64a hex of `(trace_id, timestamp_ns, body, fingerprint)` computed in the ingestion mapper. Trace-id-keyed lookups go through `observability.trace_index`.
- `observability.metrics` ([03_metrics.sql](db/clickhouse/03_metrics.sql)) — raw OTLP metrics. PK `(team_id, ts_bucket, fingerprint, metric_name, timestamp)`. One row per OTel data point; histograms inline in `hist_buckets Array(Float64)` + `hist_counts Array(UInt64)`.
- `observability.{spans,logs,metrics}_resource` ([04_resources.sql](db/clickhouse/04_resources.sql)) — `ReplacingMergeTree` dictionaries populated by MV from raw, used by reader CTEs to narrow `fingerprint IN (...)` before the main scan.
- `observability.trace_index` ([08_trace_index.sql](db/clickhouse/08_trace_index.sql)) — `MergeTree` reverse-key projection of `observability.logs`, leading PK = `trace_id`. Populated by `logs_to_trace_index` MV. Used by [trace_logs](internal/modules/logs/trace_logs/) to resolve `(team_id, trace_id) → (ts_bucket bounds, fingerprint set, log_id list)` in O(one granule) before scanning the raw logs table.
- `observability.deployments` ([05_deployments.sql](db/clickhouse/05_deployments.sql)) — VCS metadata per (service, version, environment), populated by MV from spans where `is_root = 1` and `vcs.*` resource attributes are present.
- `observability.metrics_1m` ([07_metrics_1m.sql](db/clickhouse/07_metrics_1m.sql)) — 1-minute `AggregatingMergeTree` rollup from `observability.metrics` via `metrics_1m_mv`. Five `SimpleAggregateFunction` scalar columns (`val_min`/`val_max`/`val_sum`/`val_count`/`val_last`); histogram cols `hist_buckets` (max), `hist_sum`/`hist_count` (sum) are `SimpleAggregateFunction`; `hist_counts AggregateFunction(sumForEach, Array(UInt64))` requires `-Merge` on read. `attr_hash UInt64 = cityHash64(toJSONString(attributes))` discriminates attribute combos in PK. **Every metrics-bearing reader queries `metrics_1m`, not raw `observability.metrics`** — interpolate p50/p95/p99 Go-side via [quantile.FromHistogram](internal/shared/quantile/quantile.go).

### Ingestion

[internal/ingestion/{spans,logs,metrics}/](internal/ingestion/) — flat 7-file layout per signal: `schema/`, `handler.go` → `mapper.go` → `producer.go` → `consumer.go` → `writer.go` + `dlq.go` + `module.go`. Pipeline: gRPC OTLP `Export` → mapper → `[]*schema.Row` → `producer.PublishBatch` (one Kafka record per row, key=teamID) → `consumer.PollFetches` → decode → `writer.Insert` (`async_insert=1, wait_for_async_insert=1`). On any CH insert failure, publish original record bytes verbatim to `optikk.dlq.{signal}` and commit — **no retry loop**.

Shared kafka primitives in [internal/infra/kafka/](internal/infra/kafka/) (flat — `client.go`, `producer.go`, `consumer.go`, `topics.go`, `observability.go`). Topics auto-created at app boot via `kafka.EnsureTopics` (idempotent). Topology config: [internal/config/ingestion.go](internal/config/ingestion.go) — `SignalConfig{Partitions, Replicas, RetentionHours, ConsumerGroup}` per signal.

### Read paths (one line each)

Every reader follows the apm pattern: queries-only `repository.go` (each query `const`, all values bound via `clickhouse.Named()` — **no `fmt.Sprintf` in any SQL, no phantom rollup table refs**); derivations in `service.go`. Filter-bearing modules emit an inline `WITH active_fps AS (... <signal>_resource ...)` CTE; trace-id-scoped point lookups skip the resource CTE because the trace_id+span_id PREWHERE is already maximal.

- **Logs** ([internal/modules/logs/](internal/modules/logs/)) — `explorer`, `logdetail`, `log_facets`, `log_trends`, `trace_logs`. `explorer` / `log_trends` read `observability.logs` + `logs_resource` via shared `filter.BuildClauses(f)` in [internal/modules/logs/filter/](internal/modules/logs/filter/). `log_facets` is **one** CH query — a 4-arm `UNION ALL` on `logs_resource` covering service/host/pod/environment; severity is a static label list (`models.SeverityLabels`), no DB call. `logdetail` is a single `const` query keyed on `log_id` (FNV-64a hex), pruned by `idx_log_id` bloom-filter. `trace_logs` (`GET /api/v1/logs/trace/:traceID`) is two `const` queries: step 1 reads `observability.trace_index` (PREWHERE leads on `trace_id`) to resolve `(min/max ts_bucket, fingerprint set)`; step 2 PREWHEREs `observability.logs` on three PK slots `(team_id, ts_bucket BETWEEN, fingerprint IN @fps)` plus the trace_id row check.
- **Traces** ([internal/modules/traces/](internal/modules/traces/)) — `explorer`, `span_query` read `observability.spans` + `spans_resource` via shared `filter.BuildClauses(f)` in [internal/modules/traces/filter/](internal/modules/traces/filter/). Trace-id-scoped submodules (`tracedetail` mostly, `trace_paths`, `trace_servicemap`, `trace_shape`, `trace_suggest`) skip the resource CTE — `traceIDMatchPredicate` SQL fragment + `traceIDArgs` helper inlined at the bottom of each `repository.go`. Span-side error analytics live in [services/errors](internal/modules/services/errors/), not in a separate traces/errors module.
- **Metrics** ([internal/modules/metrics/explorer/](internal/modules/metrics/explorer/)) — reads `observability.metrics_1m` + `metrics_resource`. Filter shape and emitter live in [internal/modules/metrics/filter/](internal/modules/metrics/filter/) (canonicalization, sanitization, and clause emission share one home — deviates from the per-module-helper convention).
- **Services** ([internal/modules/services/](internal/modules/services/)) — `apm`, `httpmetrics` (mixed), `redmetrics`, `slo`, `latency`, `topology`, `errors`, `deployments`. APM-style metrics reads on `metrics_1m`; spans reads on `observability.spans`; `httpmetrics` mixes both per panel; `topology` interpolates P50/P95/P99 Go-side from a `countIf` bucket array; `redmetrics` uses SQL `quantileTiming(q)(duration_nano / 1e6)`; `deployments` reads its own `observability.deployments` table joined to `observability.spans`.
- **Infrastructure** ([internal/modules/infrastructure/](internal/modules/infrastructure/)) — 7 metrics modules (`connpool`, `cpu`, `disk`, `jvm`, `kubernetes`, `memory`, `network`) read `metrics_1m` + `metrics_resource`; 2 spans modules (`nodes`, `fleet`) read `observability.spans` + `spans_resource`. `infraconsts` holds shared OTel metric-name constants. Panels assume OTel semconv 1.30+ canonical attribute paths.
- **Saturation / kafka** ([internal/modules/saturation/kafka/](internal/modules/saturation/kafka/)) — 19 routes, all reads on `metrics_1m` + `metrics_resource`. Multi-name OTel aliases via `metric_name IN @metricNames`. Histogram percentiles Go-side; partition-lag is server-side `argMax(value, timestamp)` + DESC LIMIT 200.
- **Saturation / database** ([internal/modules/saturation/database/](internal/modules/saturation/database/)) — 8 spans-side submodules (`volume`, `errors`, `latency`, `collection`, `system`, `systems`, `summary`, `slowqueries`) read `observability.spans` + `spans_resource`; `connections` reads `metrics_1m` + `metrics_resource` (the `db.client.connection.*` family is metrics-only). `summary` and `systems` are two-phase composites that fan span aggregates + active-connection gauge in parallel via errgroup. Shared filter + bucket helpers in [internal/modules/saturation/database/filter/](internal/modules/saturation/database/filter/) including `LatencyBucketBoundsMs` (closed set: 1/5/10/25/50/100/250/500/1000/2500/5000/10000/30000/+Inf ms) and `LatencyBucketCountsSQL()`.

### Middleware and session

[internal/infra/middleware/](internal/infra/middleware/) — two files: `middleware.go` (CORS, ErrorRecovery, BodyLimit 10 MiB, TenantMiddleware, RequireRole, GetTenant) and `metrics.go` (HTTPMetricsMiddleware with route-templated labels). Public-prefix list (auth bypass): `/api/v1/auth/login`, `/otlp/`, `/health`. Public-POST list: `/api/v1/auth/forgot-password`, `/api/v1/users`, `/api/v1/teams`. Session manager: [internal/infra/session/manager.go](internal/infra/session/manager.go).

### Local monitoring

[monitoring/](monitoring/) ships a Prometheus + Grafana pair. Ports: Prometheus `:19091`, Grafana `:13001`. Bring-up: `docker compose -f monitoring/stack/docker-compose.yml up -d`. Dashboards in `monitoring/grafana/dashboards/`: `optikk_overview`, `optikk_http_api`, `optikk_grpc`, `optikk_db`, `optikk_redis`, `optikk_kafka`, `optikk_ingest`. All Prometheus-sourced — no OTel collector, no Tempo.

### Load test (query-side)

`make loadtest-smoke` for CI sanity, `make loadtest-all` for full sweep. Scenarios in [loadtest/scenarios/](loadtest/scenarios/), entrypoints in [loadtest/entrypoints/](loadtest/entrypoints/). See [loadtest/docs/README.md](loadtest/docs/README.md).

### Schema migrations

[db/clickhouse/*.sql](db/clickhouse/) applied via [internal/infra/database](internal/infra/database) at boot, lexically. Embedded via [db/clickhouse/embed.go](db/clickhouse/embed.go) (`//go:embed *.sql`). `observability.schema_migrations.version` records each applied filename.

## Engineering principles

- **Module Architecture**: Strict 6-file pattern (`handler.go`, `service.go`, `repository.go`, `module.go`, `dto.go`, `models.go`). All repository methods stay in `repository.go`.
- **Apm-style discipline**: Every reader is queries-only repository + derivation in service. SQL is `const`; values bound via `clickhouse.Named()`. **No `fmt.Sprintf` in queries. No phantom rollup tables.** Inline `WITH active_fps AS (... _resource ...)` CTE for partition pruning.
- **SOLID & DRY**: Factor shared behavior when a pattern appears more than once.
- **Quality**: Leave the code clearer or simpler with every change.
- **No unsolicited tests**: Do not add tests unless explicitly asked.
