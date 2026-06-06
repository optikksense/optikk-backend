# ClickHouse schema

Migrations apply in lexical order at server boot. The migrator is wired into [internal/infra/database](../../internal/infra/database) and embedded SQL ships via [embed.go](embed.go) (`//go:embed *.sql`).

All tables live in the `observability` database. Rollup tiers exist for both signals. Metrics are **normalized** across five tables: a scalar rollup (`metrics_1m`), a histogram rollup (`metrics_hist_1m`, with a `quantilesPrometheusHistogram(0.5, 0.95, 0.99)` aggregate state for OTel histogram percentiles), and three dictionaries that keep dimensions off the hot rollup rows — `metrics_resource` (resource dims by fingerprint), `metrics_attr` (data-point labels by attr_hash), and `metrics_meta` (type/unit/description by metric_name). Spans use a single `spans_1m` rollup (with a `quantileTiming` aggregate state for latency percentiles).

The scalar/histogram split removes the empty-state tax (91.5% of rows are scalar and previously carried an empty histogram state); the dictionaries let readers resolve a label like `db.name` or a resource dim like `service` by joining `metrics_attr` (on `attr_hash`) or `metrics_resource` (on `fingerprint`) — the same CTE-join shape used for resource filters — instead of carrying the JSON/flat columns on every minute's rollup row.

## Layout

| File | Contents |
|---|---|
| [`00_database.sql`](00_database.sql) | `CREATE DATABASE observability`. |
| [`01_spans.sql`](01_spans.sql) | Raw OTLP spans. PK `(team_id, ts_bucket, fingerprint, service, name, timestamp, trace_id, span_id)`. `attributes JSON(max_dynamic_paths=100)` typed-paths; ALIAS columns for hot reader names (`operation_name`, `start_time`, `duration_ms`, `status`, `http_status_code`, `is_error`, `is_root`). 30-day TTL. |
| [`02_logs.sql`](02_logs.sql) | Raw OTLP logs. PK `(team_id, ts_bucket, fingerprint, timestamp)`. `attributes_{string,number,bool}` Map columns + `resource JSON(max_dynamic_paths=100)`. Skip-indexes: `idx_log_id` bloom-filter on `log_id`; `idx_body_text` native text (inverted) index on `body`. 30-day TTL. |
| [`03_metrics.sql`](03_metrics.sql) | Raw OTLP metrics. PK `(team_id, ts_bucket, fingerprint, metric_name, timestamp)`. One row per OTel data point; histograms inline in `hist_buckets Array(Float64)` + `hist_counts Array(UInt64)`. 30-day TTL. |
| [`04_spans_resource.sql`](04_spans_resource.sql) | `spans_resource` `ReplacingMergeTree` dictionary + `spans_to_spans_resource` MV. Resolves fingerprint before raw-table scan. 90-day TTL. |
| [`05_logs_resource.sql`](05_logs_resource.sql) | `logs_resource` `ReplacingMergeTree` dictionary + `logs_to_logs_resource` MV. Resolves fingerprint before raw-table scan. 90-day TTL. |
| [`06_metrics_resource.sql`](06_metrics_resource.sql) | `metrics_resource` **pure resource** `ReplacingMergeTree` dictionary (`service`, `host`, `environment`, `k8s_namespace`, `pod`, `container`) keyed `(team_id, fingerprint)` + `metrics_to_metrics_resource` MV. Carries **no `metric_name`** — readers narrow by metric on the rollups instead, so the dict stays one row per fingerprint. Resolves/projects resource dims by joining the rollup on `fingerprint` for resource filters and group-bys. 90-day TTL. |
| [`07_trace_index.sql`](07_trace_index.sql) | **Spans-fed** reverse-key projection. `MergeTree`, leading PK = `trace_id`. Populated by `spans_to_trace_index` MV. Used by trace_logs and traces/explorer to resolve `(team_id, trace_id) → (ts_bucket bounds, fingerprint set)`. 30-day TTL. |
| [`08_metrics_1m.sql`](08_metrics_1m.sql) | 1-minute **scalar (Gauge/Sum)** `AggregatingMergeTree` rollup from `observability.metrics` via `metrics_1m_mv` (MV gates on `metric_type IN ('Gauge','Sum')`). PK `(team_id, metric_name, ts_bucket, fingerprint, attr_hash, timestamp)` — **`metric_name` leads the key** so metric-scoped scans hit a contiguous range. Carries only series identity + `val_min/val_max/val_sum/val_count`; dimensions/metadata live in the dictionaries. 90-day TTL. |
| [`09_spans_1m.sql`](09_spans_1m.sql) | 1-minute `AggregatingMergeTree` rollup from `observability.spans` via `spans_1m_mv`. Carries `latency_state AggregateFunction(quantileTiming, Float64)` plus count/sum aggregates (`request_count`, `error_count`, `duration_ms_sum`). Wide column set covers HTTP, DB, error-analytics dimensions. 30-day TTL. |
| [`10_metrics_attr.sql`](10_metrics_attr.sql) | `metrics_attr` `ReplacingMergeTree` label dictionary keyed `(team_id, metric_name, attr_hash)` carrying the per-data-point `attributes JSON` + `metrics_to_metrics_attr` MV. Stores each label set once per metric instead of on every rollup row; backs tag faceting and `attr_hash`-joined label group-bys/filters. 90-day TTL. |
| [`11_metrics_meta.sql`](11_metrics_meta.sql) | `metrics_meta` `ReplacingMergeTree` metadata dictionary keyed `(team_id, metric_name)` carrying `metric_type`, `unit`, `description` + `metrics_to_metrics_meta` MV. One row per metric; backs the metric catalog/list view. No partition/TTL (tiny). |
| [`12_metrics_hist_1m.sql`](12_metrics_hist_1m.sql) | 1-minute **histogram** `AggregatingMergeTree` rollup from `observability.metrics` via `metrics_hist_1m_mv` (MV gates on `metric_type = 'Histogram'`). Same PK as `metrics_1m`; carries `hist_sum`, `hist_count`, and `latency_state AggregateFunction(quantilesPrometheusHistogram(0.5, 0.95, 0.99), Float64, UInt64)` for server-side percentiles. Split from `metrics_1m` so scalar rows carry no empty histogram state and vice-versa. 90-day TTL. |

## Bucket invariant

Every `ts_bucket` value is computed Go-side via [internal/infra/timebucket](../../internal/infra/timebucket). ClickHouse never computes a bucket itself — no `toStartOfInterval`, `toStartOfHour`, `toStartOfDay`, `toStartOfMinute`, or `toStartOfFiveMinutes` appears in any reader SQL. The single exception is the `metrics_1m_mv`, `metrics_hist_1m_mv`, and `spans_1m_mv` materialized views, which derive `ts_bucket` from `timestamp` server-side because the MV runs inside ClickHouse — the value still matches what `timebucket.BucketStart` would produce for the same row.

## Apply

Migrations run automatically on every server boot — the migrator scans `observability.schema_migrations`, applies pending files in lexical order, and the server proceeds to start. There is no separate flag or binary; just:

```sh
go run ./cmd/server
```

`observability.schema_migrations.version` stores each file's basename (e.g. `01_spans.sql`). Files already recorded as applied are skipped.
