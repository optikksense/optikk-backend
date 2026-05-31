# ClickHouse schema

Migrations apply in lexical order at server boot. The migrator is wired into [internal/infra/database](../../internal/infra/database) and embedded SQL ships via [embed.go](embed.go) (`//go:embed *.sql`).

All tables live in the `observability` database. Two rollup tiers exist: `metrics_1m` (for OTel metrics, with a `quantilesPrometheusHistogram(0.5, 0.95, 0.99)` aggregate state for OTel histogram percentiles) and `spans_1m` (for spans, with a `quantileTiming` aggregate state for latency percentiles).

## Layout

| File | Contents |
|---|---|
| [`00_database.sql`](00_database.sql) | `CREATE DATABASE observability`. |
| [`01_spans.sql`](01_spans.sql) | Raw OTLP spans. PK `(team_id, ts_bucket, fingerprint, service, name, timestamp, trace_id, span_id)`. `attributes JSON(max_dynamic_paths=100)` typed-paths; ALIAS columns for hot reader names (`operation_name`, `start_time`, `duration_ms`, `status`, `http_status_code`, `is_error`, `is_root`). 30-day TTL. |
| [`02_logs.sql`](02_logs.sql) | Raw OTLP logs. PK `(team_id, ts_bucket, fingerprint, timestamp)`. `attributes_{string,number,bool}` Map columns + `resource JSON(max_dynamic_paths=100)`. Skip-indexes: `idx_log_id` bloom-filter on `log_id`; `idx_body_text` native text (inverted) index on `body`. 30-day TTL. |
| [`03_metrics.sql`](03_metrics.sql) | Raw OTLP metrics. PK `(team_id, ts_bucket, fingerprint, metric_name, timestamp)`. One row per OTel data point; histograms inline in `hist_buckets Array(Float64)` + `hist_counts Array(UInt64)`. 30-day TTL. |
| [`04_spans_resource.sql`](04_spans_resource.sql) | `spans_resource` `ReplacingMergeTree` dictionary + `spans_to_spans_resource` MV. Resolves fingerprint before raw-table scan. 90-day TTL. |
| [`05_logs_resource.sql`](05_logs_resource.sql) | `logs_resource` `ReplacingMergeTree` dictionary + `logs_to_logs_resource` MV. Resolves fingerprint before raw-table scan. 90-day TTL. |
| [`06_metrics_resource.sql`](06_metrics_resource.sql) | `metrics_resource` `ReplacingMergeTree` dictionary + `metrics_to_metrics_resource` MV. Resolves fingerprint before raw-table scan. 90-day TTL. |
| [`07_trace_index.sql`](07_trace_index.sql) | **Spans-fed** reverse-key projection. `MergeTree`, leading PK = `trace_id`. Populated by `spans_to_trace_index` MV. Used by trace_logs and traces/explorer to resolve `(team_id, trace_id) → (ts_bucket bounds, fingerprint set)`. 30-day TTL. |
| [`08_metrics_1m.sql`](08_metrics_1m.sql) | 1-minute `AggregatingMergeTree` rollup from `observability.metrics` via `metrics_1m_mv`. Carries `metric_type`, `unit`, `description` metadata, and `latency_state AggregateFunction(quantilesPrometheusHistogram(0.5, 0.95, 0.99), Float64, UInt64)` for server-side percentiles. 90-day TTL. |
| [`09_spans_1m.sql`](09_spans_1m.sql) | 1-minute `AggregatingMergeTree` rollup from `observability.spans` via `spans_1m_mv`. Carries `latency_state AggregateFunction(quantileTiming, Float64)` plus count/sum aggregates. Wide column set covers HTTP, DB, error-analytics dimensions. 30-day TTL. |

## Bucket invariant

Every `ts_bucket` value is computed Go-side via [internal/infra/timebucket](../../internal/infra/timebucket). ClickHouse never computes a bucket itself — no `toStartOfInterval`, `toStartOfHour`, `toStartOfDay`, `toStartOfMinute`, or `toStartOfFiveMinutes` appears in any reader SQL. The single exception is the `metrics_1m_mv` and `spans_1m_mv` materialized views, which derive `ts_bucket` from `timestamp` server-side because the MV runs inside ClickHouse — the value still matches what `timebucket.BucketStart` would produce for the same row.

## Apply

Migrations run automatically on every server boot — the migrator scans `observability.schema_migrations`, applies pending files in lexical order, and the server proceeds to start. There is no separate flag or binary; just:

```sh
go run ./cmd/server
```

`observability.schema_migrations.version` stores each file's basename (e.g. `01_spans.sql`). Files already recorded as applied are skipped.
