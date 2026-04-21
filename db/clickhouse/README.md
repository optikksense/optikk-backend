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
| `13_rollup_ai_spans.sql` | `ai_spans_rollup_{1m,5m,1h}` + MVs — GenAI / LLM spans. |
| `14_rollup_spans_error_fingerprint.sql` | `spans_error_fingerprint_{1m,5m,1h}` + MVs — grouped error spans. |
| `15_rollup_spans_host.sql` | `spans_host_rollup_{1m,5m,1h}` + MVs — RED per (host, pod, service). |
| `16_rollup_spans_by_version.sql` | `spans_by_version_{1m,5m,1h}` + MVs — root-span RED per (service, version, env). |
| `17_rollup_metrics_gauges.sql` | `metrics_gauges_rollup_{1m,5m,1h}` + MVs — generic gauge / counter metrics with state_dim. |
| `18_rollup_metrics_gauges_by_status.sql` | `metrics_gauges_by_status_rollup_{1m,5m,1h}` + MVs — HTTP count by status. |
| `19_rollup_db_histograms.sql` | `db_histograms_rollup_{1m,5m,1h}` + MVs — db.* histogram latency. |
| `20_rollup_messaging_histograms.sql` | `messaging_histograms_rollup_{1m,5m,1h}` + MVs — messaging.* histogram latency. |
| `21_rollup_spans_topology.sql` | `spans_topology_rollup_{1m,5m,1h}` + MVs — service-to-service edges. |

Each `*_rollup_*.sql` file is self-contained — all three tiers (`_1m`, `_5m`, `_1h`) plus their MVs live together.

## Applying the schema

```sh
cat db/clickhouse/*.sql | clickhouse-client --multiquery
```

The glob expands in lexical order, so the numeric prefixes drive the right sequence.

## Adding a new rollup

Drop a new `NN_rollup_<name>.sql` at the next available slot. Include all three tiers + their MVs in the single file. Don't retcon earlier files — treat them as append-only migrations.
