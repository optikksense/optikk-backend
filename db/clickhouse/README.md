# ClickHouse schema

Migrations apply in lexical order. Tables live in the `observability` database.

## Layout

| File | Contents |
|---|---|
| `00_database.sql` | `CREATE DATABASE observability`. |
| `01_spans.sql` | Raw OTLP spans with `mat_*` MATERIALIZED attributes and `http_status_bucket`. |
| `02_logs.sql` | Raw OTLP logs with `severity_bucket` MATERIALIZED. |
| `03_metrics.sql` | Raw OTLP metrics with `state_dim` MATERIALIZED (feeds metrics_gauges + metrics_k8s rollups). |
| `04_traces_index.sql` | Per-trace summary (one row per trace) with `http_status_bucket` MATERIALIZED. |
| `05_dim_deployments.sql` | Dimension table holding VCS metadata per (service, version, env). `spans_version` rollup joins here for display fields. |
| `06_spans_red.sql` | **Canonical APM stats rollup.** Service × operation × http_status_bucket. |
| `07_spans_kind.sql` | Service × kind_string latency + rate. |
| `08_spans_host.sql` | Service × host/pod (infrastructure-lens). |
| `09_spans_topology.sql` | Service-graph edges (CLIENT-side spans with peer.service). |
| `10_spans_peer.sql` | CLIENT-side breakdown by peer/host/status. PK leads with (service, http_status_bucket). |
| `11_spans_version.sql` | Per-deployment RED stats; joins to `deployments` for VCS metadata. |
| `12_spans_errors.sql` | Error-fingerprint rollup with sampled trace_id / status_message. |
| `13_metrics_gauges.sql` | Unified gauge + sum metric rollup. MV body is pure GROUP BY + state. |
| `14_metrics_gauges_by_status.sql` | HTTP request counters bucketed by status. |
| `15_metrics_hist.sql` | OTLP Histogram percentiles (t-digest). |
| `16_metrics_k8s.sql` | Kubernetes-specific gauges. PK leads with namespace. |
| `17_db_saturation.sql` | All db.* / pool.* metrics. PK leads with db_system. |
| `18_messaging.sql` | Unified messaging rollup (counters + histograms merged). |
| `19_logs_volume.sql` | Log-count + error-count by severity and resource. Pod consistent across tiers. |
| `20_logs_facets.sql` | HLL facet sketches for the logs explorer (3 tiers). |
| `21_traces_facets.sql` | HLL facet sketches for the traces explorer (3 tiers). |
| `22_spans_by_trace_index.sql` | Per-trace span projection for drill-down queries. |
| `23_root_spans_index.sql` | Root-span-only projection for "Related traces". |
| `24_trace_attribute_values_5m.sql` | Attribute-value autocomplete index. |
| `25_logs_by_trace_index.sql` | Per-trace log projection. |

The `traces_index.by_start_desc` projection is declared inline inside
`04_traces_index.sql`'s `CREATE TABLE` — no separate ALTER migration.

## Rollup tier policy

Every rollup family has three tiers. All measures use `AggregateFunction` states; cascade is `raw → _1m → _5m → _1h` via `*MergeState`.

| Tier | TTL | Purpose |
|---|---|---|
| `_1m` | 72 hours | Live dashboards and "today/yesterday" minute-resolution. |
| `_5m` | 14 days | Medium-resolution historical (past 2 weeks). |
| `_1h` | 90 days | Long-range historical. |

`PickTier` is TTL-aware: the returned tier is the finest whose retention still covers `startMs`. Readers go through `internal/infra/rollup`.

## Design principles

1. **Materialized columns live on raw tables** — any derived dim with fan-in ≥ 2 (shared across rollup families) is declared MATERIALIZED on the raw fact table. MV SELECTs stay pure `GROUP BY + state(…)` wherever possible.
2. **Consistent columns across tiers** — every family's 1m/5m/1h siblings share identical columns and PRIMARY KEY.
3. **PK reflects dominant WHERE shape** — e.g. `db_saturation` leads with `db_system`, `metrics_k8s` leads with `namespace`, `spans_peer` leads with `(service_name, http_status_bucket)`.
4. **No MV → MV → MV compute.** Only the `raw → _1m` MV ever evaluates anything non-trivial; subsequent tier hops are `*MergeState` merges of pre-aggregated state.
5. **Canonical APM stats** — `spans_red` is the default rollup for service-level RED metrics. New spans-derived rollups must justify the additional family against cardinality and maintenance cost.

## Apply

Migrations run automatically on every server boot — the migrator scans
`schema_migrations`, applies pending files in lexical order, and the server
proceeds to start. There is no separate flag or binary; just:

```sh
go run ./cmd/server
```

`observability.schema_migrations.version` stores each file's basename (e.g.
`01_spans.sql`). Files already recorded as applied are skipped.
