# ClickHouse schema

DDL is split one-file-per-object so each file is reviewable on its own. Load files in lexical order — the numeric prefix enforces the sequence (raw tables → rollups, base tier before cascade within each rollup file).

| File | Contents |
|---|---|
| `00_database.sql` | `CREATE DATABASE observability`. |
| `01_spans.sql` | `observability.spans` — raw OTLP spans + materialized attribute columns + bloom-filter indexes. 1 h raw TTL. |
| `02_logs.sql` | `observability.logs` — raw OTLP logs. 1 h raw TTL. |
| `03_metrics.sql` | `observability.metrics` — raw OTLP metrics. 1 h raw TTL. |
| `04_alert_events.sql` | `observability.alert_events` — alerting audit log. |
| `10_rollup_spans.sql` | `spans_rollup_{1m,5m,1h}` + MVs — root-span RED per (service, operation, endpoint, method). |
| `11_rollup_metrics_histograms.sql` | `metrics_histograms_rollup_{1m,5m,1h}` + MVs — generic histogram metric latency. |
| `12_rollup_logs.sql` | `logs_rollup_{1m,5m,1h}` + MVs — log volume + error counts. |
| `14_rollup_spans_error_fingerprint.sql` | `spans_error_fingerprint_{1m,5m,1h}` + MVs — grouped error spans. |
| `15_rollup_spans_host.sql` | `spans_host_rollup_{1m,5m,1h}` + MVs — RED per (host, pod, service). |
| `16_rollup_spans_by_version.sql` | `spans_by_version_{1m,5m,1h}` + MVs — root-span RED per (service, version, env). |
| `17_rollup_metrics_gauges.sql` | `metrics_gauges_rollup_{1m,5m,1h}` + MVs — unified gauge / counter rollup with extended `state_dim` (network state+direction fallback, filesystem.mountpoint, jvm pool+type, db.client.connections.state, …). |
| `18_rollup_metrics_gauges_by_status.sql` | `metrics_gauges_by_status_rollup_{1m,5m,1h}` + MVs — HTTP count by status. |
| `19_rollup_db_histograms.sql` | `db_histograms_rollup_{1m,5m,1h}` + MVs — unified db/pool histogram + gauge rows; keys include `db_connection_state` and `db_response_status_code`. |
| `20_rollup_messaging_histograms.sql` | `messaging_histograms_rollup_{1m,5m,1h}` + MVs — messaging.* histogram latency. |
| `21_rollup_spans_topology.sql` | `spans_topology_rollup_{1m,5m,1h}` + MVs — service-to-service edges. |
| `22_rollup_metrics_k8s.sql` | `metrics_k8s_rollup_{1m,5m,1h}` + MVs — K8s-scoped metrics with first-class `container` + `namespace` keys + pod-phase state. |
| `23_rollup_messaging_counters.sql` | `messaging_counters_rollup_{1m,5m,1h}` + MVs — messaging counter / gauge metrics (rates, lag, rebalance, broker connections) — keys include broker + partition + error_type. |
| `24_rollup_spans_peer.sql` | `spans_peer_rollup_{1m,5m,1h}` + MVs — external-host / peer-service CLIENT-span aggregates with http_status_bucket. |
| `25_rollup_spans_kind.sql` | `spans_kind_rollup_{1m,5m,1h}` + MVs — span-kind breakdown (5–6 kinds total). |
| `26_drop_legacy_metrics_gauges_v2_objects.sql` | Drops obsolete `metrics_gauges_rollup_v2_*` MVs/tables left from pre-merge gauge rollup naming (safe `IF EXISTS`). |
| `27_drop_legacy_db_histograms_and_ai_spans_v2_objects.sql` | Drops obsolete `db_histograms_rollup_v2_*` MVs/tables (safe `IF EXISTS`). |
| `29_drop_ai_spans_rollup_objects.sql` | Drops all `ai_spans_rollup_*` MVs/tables (GenAI rollup not used; raw spans stay in `observability.spans`). |

Each `*_rollup_*.sql` file is self-contained — all three tiers (`_1m`, `_5m`, `_1h`) plus their MVs live together.

## Applying the schema

```sh
go run ./cmd/migrate up       # apply all pending migrations
go run ./cmd/migrate status   # list applied / pending files
```

The migrator embeds every `db/clickhouse/*.sql` file into the Go binary, applies them in lexical order, and records each file's basename in `observability.schema_migrations`. Re-runs are idempotent — already-applied files are skipped.

First-time setup after `docker-compose up -d clickhouse` (or any fresh cluster):

```sh
go run ./cmd/migrate up
```

For ad-hoc one-shot applies without the CLI (e.g. debugging on a remote host), the flat `cat | clickhouse-client` form still works because every `CREATE` uses `IF NOT EXISTS`, but it won't update `schema_migrations` — prefer the CLI.

## Adding a new rollup

Drop a new `NN_rollup_<name>.sql` at the next available slot. Include all three tiers + their MVs in the single file. Don't retcon earlier files — treat them as append-only migrations. On next `migrate up` the new file is picked up automatically.

## Schema tracker

`observability.schema_migrations` is created by the runner. Schema:

```sql
CREATE TABLE observability.schema_migrations (
    version    String,                   -- file basename, e.g. "10_rollup_spans.sql"
    applied_at DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY version
```

Query with:

```sh
clickhouse-client -q "SELECT version, applied_at FROM observability.schema_migrations ORDER BY version"
```
