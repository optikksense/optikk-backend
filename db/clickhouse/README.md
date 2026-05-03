# ClickHouse schema

Migrations apply in lexical order at server boot. The migrator is wired into [internal/infra/database](../../internal/infra/database) and embedded SQL ships via [embed.go](embed.go) (`//go:embed *.sql`).

All tables live in the `observability` database. Two rollup tiers exist: `metrics_1m` (for OTel metrics) and `spans_1m` (for spans, with a `quantileTiming` aggregate state for latency percentiles).

## Layout

| File | Contents |
|---|---|
| [`00_database.sql`](00_database.sql) | `CREATE DATABASE observability`. |
| [`01_spans.sql`](01_spans.sql) | Raw OTLP spans. PK `(team_id, ts_bucket, fingerprint, service, name, timestamp, trace_id, span_id)`. `attributes JSON(max_dynamic_paths=100)` typed-paths; ALIAS columns for hot reader names (`operation_name`, `start_time`, `duration_ms`, `status`, `http_status_code`, `is_error`, `is_root`). 30-day TTL. |
| [`02_logs.sql`](02_logs.sql) | Raw OTLP logs. PK `(team_id, ts_bucket, fingerprint, timestamp)`. `attributes_{string,number,bool}` Map columns + `resource JSON(max_dynamic_paths=100)`. `log_id String` column (FNV-64a hex of `(trace_id, timestamp_ns, body, fingerprint)` computed in [internal/ingestion/logs/mapper.go](../../internal/ingestion/logs/mapper.go)::`computeLogID`). Skip-indexes: `idx_log_id` bloom-filter on `log_id`; `idx_body_text` native text (inverted) index on `body` (CH 26.2 GA — `tokenizer='splitByNonAlpha'` + `preprocessor='lowerUTF8(str)'`; consulted by `hasToken(body, lower(@search))` and the case-insensitive `lower(body) LIKE` exact-mode predicate). 30-day TTL. (No `idx_trace_id` — trace-id-keyed lookups go through `observability.trace_index` instead.) |
| [`03_metrics.sql`](03_metrics.sql) | Raw OTLP metrics. PK `(team_id, ts_bucket, fingerprint, metric_name, timestamp)`. One row per OTel data point; histograms inline in `hist_buckets Array(Float64)` + `hist_counts Array(UInt64)`. Six flat resource columns (`service`, `host`, `environment`, `k8s_namespace`, `http_method`, `http_status_code`); `resource` and `attributes` JSON typed-path columns. 30-day TTL. |
| [`04_resources.sql`](04_resources.sql) | Three `_resource` `ReplacingMergeTree` dictionaries — `spans_resource`, `logs_resource`, `metrics_resource` — populated by MV from the raw tables. Used by reader CTEs to narrow `fingerprint IN (...)` before the main scan. 90-day TTL. |
| [`05_deployments.sql`](05_deployments.sql) | Deployment dimension table populated by `spans_to_deployments` MV from spans where `is_root = 1` and the `vcs.*` resource attributes are present. PK `(team_id, service, service_version, environment)`. 180-day TTL. |
| [`07_metrics_1m.sql`](07_metrics_1m.sql) | 1-minute `AggregatingMergeTree` rollup from `observability.metrics`. PK `(team_id, ts_bucket, metric_name, fingerprint, attr_hash, timestamp)`. `SimpleAggregateFunction` columns for `val_min`/`val_max`/`val_sum`/`val_count`/`val_last`, `hist_buckets` (max), `hist_sum`/`hist_count` (sum); `hist_counts` is `AggregateFunction(sumForEach, Array(UInt64))` and requires `-Merge` on read. 90-day TTL. Every metrics-bearing reader queries this rollup, **not** raw `observability.metrics`. |
| [`08_trace_index.sql`](08_trace_index.sql) | **Spans-fed** reverse-key projection (was logs-fed). `MergeTree`, leading PK = `trace_id`. Populated by `spans_to_trace_index` MV from `observability.spans`. Used by both [trace_logs](../../internal/modules/logs/trace_logs/) and [traces/explorer.GetByID](../../internal/modules/traces/explorer/) to resolve `(team_id, trace_id) → (ts_bucket bounds, fingerprint set)` in O(one granule), then narrow the raw scan. `log_id` is fetched directly from `observability.logs` (trace_index no longer carries it). 30-day TTL. |
| [`09_spans_1m.sql`](09_spans_1m.sql) | 1-minute `AggregatingMergeTree` rollup from `observability.spans` via `spans_1m_mv`. Holds `latency_state AggregateFunction(quantileTiming, Float64)` plus `SimpleAggregateFunction` columns for `request_count`, `error_count`, `duration_ms_sum`, `duration_ms_max`, and three `any()` sample fields (`sample_status_message`, `sample_trace_id`, `sample_exception_stacktrace`). PK leads with `(team_id, ts_bucket, fingerprint, service, name, kind_string, exception_type, status_message_hash)` so error-group queries hit a tight contiguous range. Wide column set covers HTTP semantics, service identity, DB attributes, error-analytics columns; `status_message_hash UInt64 = cityHash64(status_message)` provides the stable error-group identity used by `services/errors`, `services/deployments`. Span-side aggregate / count / percentile readers project from this rollup; only DETAIL queries (per-trace, per-span, free-text grouping, dynamic threshold, heatmap, attribute typeahead) stay on raw `observability.spans`. 30-day TTL. |
| [`10_logs_text_index.sql`](10_logs_text_index.sql) | Migrates `observability.logs.body` from `tokenbf_v1` to the native CH 26.2 `text` (inverted) index — drops `idx_body_tokens`, adds `idx_body_text` with `tokenizer='splitByNonAlpha'` + `preprocessor='lowerUTF8(str)'`, then `MATERIALIZE INDEX` over historical parts. Body-search predicates in [internal/modules/logs/filter](../../internal/modules/logs/filter/) switch from `hasTokenCaseInsensitive` / `positionCaseInsensitive` to `hasToken(body, lower(@search))` / `lower(body) LIKE …` to match the index's supported function list. |

(`06_*.sql` is intentionally absent — file slot reserved.)

## Bucket invariant

Every `ts_bucket` value is computed Go-side via [internal/infra/timebucket](../../internal/infra/timebucket). ClickHouse never computes a bucket itself — no `toStartOfInterval`, `toStartOfHour`, `toStartOfDay`, `toStartOfMinute`, or `toStartOfFiveMinutes` appears in any reader SQL. The single exception is the `metrics_1m_mv` materialized view in `07_metrics_1m.sql`, which derives `ts_bucket` from `timestamp` server-side because the MV runs inside ClickHouse — the value still matches what `timebucket.BucketStart` would produce for the same row.

## Apply

Migrations run automatically on every server boot — the migrator scans `observability.schema_migrations`, applies pending files in lexical order, and the server proceeds to start. There is no separate flag or binary; just:

```sh
go run ./cmd/server
```

`observability.schema_migrations.version` stores each file's basename (e.g. `01_spans.sql`). Files already recorded as applied are skipped.
