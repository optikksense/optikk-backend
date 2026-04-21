# ClickHouse schema

DDL is split by table / phase so each file is reviewable on its own. Load files in lexical order (the numeric prefix enforces the sequence: base tables → Phase-5 rollups → Phase-6 base + cascade → Phase-7 base + cascade).

| File | Contents |
|---|---|
| `00_database.sql` | `CREATE DATABASE observability`. |
| `01_spans.sql` | `observability.spans` — raw OTLP spans with materialized attribute columns (`mat_http_route`, `mat_db_system`, `mat_peer_service`, `mat_host_name`, `mat_k8s_pod_name`, `mat_service_version`, `mat_deployment_environment`, …) + bloom-filter indexes on hot lookup columns (trace_id, span_id, service_name, …). 1 h raw TTL. |
| `02_logs.sql` | `observability.logs` — raw OTLP logs; materialized `service`, `host`, `pod`; bloom/ngram indexes. 1 h raw TTL. |
| `03_metrics.sql` | `observability.metrics` — raw OTLP metrics; materialized `service`, `host`, `environment`, `k8s_namespace`, `http_method`, `http_status_code`, `has_error`. 1 h raw TTL. |
| `04_alert_events.sql` | `observability.alert_events` — alerting audit log. |
| `10_phase5_rollups.sql` | Phase 5 base rollups: `spans_rollup_1m` + MV, `metrics_histograms_rollup_1m` + MV. AggregatingMergeTree, 90 d TTL. |
| `20_phase6_rollups_base.sql` | Phase 6 base rollups: `logs_rollup_1m`, `ai_spans_rollup_1m`, `spans_error_fingerprint_1m`, `spans_host_rollup_1m`, `spans_by_version_1m` + MVs. |
| `21_phase6_rollups_cascade.sql` | Phase 6 cascade tiers (`_5m` + `_1h`) for all seven Phase-5/6 rollups, via `-MergeState` combinator MVs. |
| `30_phase7_rollups.sql` | Phase 7 base + cascade: `metrics_gauges_rollup_*`, `metrics_gauges_by_status_rollup_*`, `db_histograms_rollup_*`, `messaging_histograms_rollup_*`, `spans_topology_rollup_*`. |

## Applying the schema

### Fresh cluster (local dev / CI)

```sh
cat db/clickhouse/*.sql | clickhouse-client --multiquery
```

The glob expands in lexical order, so the numeric prefixes drive the right sequence.

### Adding a new phase

Drop a new file at the appropriate prefix slot (e.g. `40_phase8_rollups.sql`). Keep one logical group per file; split base tables from cascade tiers if the phase adds >200 lines. Don't retcon earlier files — treat them as append-only migrations.

### Single-file compat

`db/clickhouse_local.sql` remains as the flat concatenation for tooling that expects a single artifact. Regenerate with:

```sh
cat db/clickhouse/*.sql > db/clickhouse_local.sql
```

CI should verify the two are in sync.
