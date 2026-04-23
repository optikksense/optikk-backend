# ClickHouse schema (local / greenfield)

Twenty-two migrations (`00`–`21`), lexical order. No legacy cutover files, no `*_v2` table names, no historical `DROP` migrations for objects this tree never created.

| File | Contents |
|---|---|
| `00_database.sql` | `CREATE DATABASE observability`. |
| `01_spans.sql` | `observability.spans` — OTLP spans. |
| `02_logs.sql` | `observability.logs` — OTLP logs. |
| `03_metrics.sql` | `observability.metrics` — OTLP metrics. |
| `04_rollup_metrics_histograms.sql` | `metrics_histograms_rollup_*` + MVs. |
| `05_rollup_spans_error_fingerprint.sql` | `spans_error_fingerprint_*` + MVs. |
| `06_rollup_spans_host.sql` | `spans_host_rollup_*` + MVs. |
| `07_rollup_spans_by_version.sql` | `spans_by_version_*` + MVs. |
| `08_rollup_metrics_gauges.sql` | `metrics_gauges_rollup_*` + MVs. |
| `09_rollup_metrics_gauges_by_status.sql` | `metrics_gauges_by_status_rollup_*` + MVs. |
| `10_rollup_db_histograms.sql` | `db_histograms_rollup_*` + MVs. |
| `11_rollup_messaging_histograms.sql` | `messaging_histograms_rollup_*` + MVs. |
| `12_rollup_spans_topology.sql` | `spans_topology_rollup_*` + MVs. |
| `13_rollup_metrics_k8s.sql` | `metrics_k8s_rollup_*` + MVs. |
| `14_rollup_messaging_counters.sql` | `messaging_counters_rollup_*` + MVs. |
| `15_rollup_spans_peer.sql` | `spans_peer_rollup_*` + MVs. |
| `16_rollup_spans_kind.sql` | `spans_kind_rollup_*` + MVs. |
| `17_rollup_logs.sql` | `logs_rollup_{1m,5m,1h}` + MVs from `observability.logs`. |
| `18_rollup_logs_facets.sql` | `logs_facets_rollup_5m` + MV. |
| `19_traces_index.sql` | `traces_index` (span indexer / explorer). |
| `20_rollup_spans_red.sql` | `spans_rollup_{1m,5m,1h}` + MVs from `traces_index`. |
| `21_rollup_traces_facets.sql` | `traces_facets_rollup_5m` + MV. |

## Apply

```sh
go run ./cmd/migrate up
```

Fresh local DB: optionally `TRUNCATE observability.schema_migrations` first, then `migrate up`.

## Tracker

`observability.schema_migrations.version` stores each file’s basename (e.g. `01_spans.sql`).
