# Observability — Self-telemetry pipeline

This doc describes how the Optikk backend observes itself. It does NOT
cover the customer-facing OTLP ingest; that's a separate pipeline on
`:4317` which persists to ClickHouse.

## Architecture

```
Go backend ──OTLP gRPC :14317 (traces + logs)──┐
                                               │
Go backend ──HTTP /metrics :19090 (scrape)────┐│
                                              ↓↓
                                   otel-collector (docker-compose)
                                              │
                                              ▼
                                        Grafana Cloud
                                     (Tempo · Loki · Mimir)
```

Three signals, one collector:

1. **Traces** — emitted by `internal/infra/otel` via `otelgin` (HTTP),
   `otelgrpc` (gRPC), `redisotel` (Redis) and our custom
   `dbutil.SelectCH/QueryCH/ExecCH` (ClickHouse) + `SelectSQL/GetSQL/ExecSQL`
   (MariaDB) wrappers. Shipped OTLP-gRPC to the collector.
2. **Logs** — emitted by `slog.InfoContext/WarnContext/…`. The root
   handler (see `cmd/server/logger.go`) fans to stdout (tint / JSON) AND
   through `contrib/bridges/otelslog` → collector → Loki. Every record
   that has an active OTel context is decorated with `trace_id`/`span_id`
   by `internal/shared/slogx.TraceAttrHandler`.
3. **Metrics** — Prometheus `promauto` collectors exposed at `/metrics`;
   the collector's `prometheus` receiver scrapes them and forwards to
   Grafana Cloud Mimir. No OTel metrics SDK — we deliberately single-source.

The `.env` file (gitignored) carries the Grafana Cloud credentials; see
`.env.example`.

## Metric catalogue

All metrics share the `optikk_` namespace. Registered collectors live in
`internal/infra/metrics/` (one file per subsystem) except for Redis,
which owns its own collectors inside `internal/infra/redis/metrics_hook.go`
to avoid a go-redis dep leak into the metrics package.

### HTTP (`internal/infra/middleware/observability.go`)

| Metric | Type | Labels | Notes |
|---|---|---|---|
| `optikk_http_requests_total` | counter | `route`, `method`, `status_class` | `status_class` ∈ {1xx,2xx,3xx,4xx,5xx} to bound cardinality. |
| `optikk_http_request_duration_seconds` | histogram | `route`, `method` | Buckets 5 ms → 5 s. |
| `optikk_http_in_flight_requests` | gauge | — | Incremented on entry, decremented on exit. |

`route` is Gin's `FullPath()` template (e.g. `/api/v1/traces/:traceId`),
not the raw request path. Unmatched → `"unmatched"`.

### gRPC (`internal/infra/grpcutil/observability.go`)

| Metric | Type | Labels |
|---|---|---|
| `optikk_grpc_started_total` | counter | `method` |
| `optikk_grpc_handled_total` | counter | `method`, `code` |
| `optikk_grpc_handling_duration_seconds` | histogram | `method` |

Plus the standard otelgrpc span recorded per RPC by the `grpc.StatsHandler`.

### DB (`internal/infra/database/{clickhouse,mysql}_instrument.go`)

| Metric | Type | Labels |
|---|---|---|
| `optikk_db_query_duration_seconds` | histogram | `system` (clickhouse/mysql), `op` |
| `optikk_db_queries_total` | counter | `system`, `op`, `result` (ok/err) |

`op` label convention: `<module>.<MethodName>`, e.g. `"logs.ListLogs"`,
`"auth.FindActiveUserByID"`. Every call site in
`internal/modules/**/repository.go` routes through the seam so this
label is always populated.

### Kafka (`internal/infra/kafka/observability.go`)

| Metric | Type | Labels |
|---|---|---|
| `optikk_kafka_produced_total` | counter | `topic`, `result` (ok/err) |
| `optikk_kafka_produce_duration_seconds` | histogram | `topic` |
| `optikk_kafka_consumed_total` | counter | `topic` |
| `optikk_kafka_consumer_lag_records` | gauge | `topic`, `partition`, `group` |
| `optikk_kafka_rebalances_total` | counter | `reason` |
| `optikk_kafka_broker_connects_total` | counter | `result` |

Wired via `kgo.WithHooks(obsHooks{})` at client construction (both
producer and consumer, see `internal/infra/kafka/client.go`). The
consumer-lag gauge is populated by `LagPoller` on a 15 s ticker using
raw `kmsg` requests (`MetadataRequest` → `OffsetFetchRequest` →
`ListOffsetsRequest`) issued against the existing consumer client — no
`kadm` top-level dep. One poller runs per ingest consumer (logs,
metrics, spans); they're spawned as `run.Group` actors from `app.Start`
so graceful shutdown kills them.

### Kafka trace-context propagation (`internal/infra/kafka/tracecontext.go`)

Producer side: every call to `Producer.Produce` / `Producer.PublishBatch`
runs `InjectTraceContext(ctx, record)` before the record is handed to
`kgo.Client.Produce`, which writes `traceparent` (+ optional `tracestate`)
into `record.Headers` via the W3C TextMap propagator installed by
`internal/infra/otel/provider.go`.

Consumer side: `internal/infra/kafka_ingest/dispatcher.go` calls
`ExtractTraceContext(ctx, r.Headers)` for every fetched record and opens
a short-lived `kafka.consume <signal>` span on that parent ctx before
invoking the decoder. The span ends immediately after decode; the batch
pipeline (Worker → Writer) still produces its DB spans as a separate
trace root, but Grafana Tempo correlates producer ↔ consumer spans by
matching `trace_id`. Attributes: `messaging.system`, `messaging.destination`,
`messaging.kafka.partition`, `messaging.kafka.offset`.

### Redis (`internal/infra/redis/metrics_hook.go`)

| Metric | Type | Labels |
|---|---|---|
| `optikk_redis_commands_total` | counter | `cmd`, `result` |
| `optikk_redis_command_duration_seconds` | histogram | `cmd` |

Redis also gets spans via `redisotel.InstrumentTracing`. Pipelines
collapse under `cmd="pipeline"` to cap cardinality.

### Auth (`internal/infra/middleware/middleware.go`)

| Metric | Type | Labels |
|---|---|---|
| `optikk_auth_denied_total` | counter | `reason` (unauthorized/missing_team/forbidden_team/forbidden_role) |
| `optikk_auth_authenticated_total` | counter | — |

### Ingest handler (`internal/ingestion/{spans,logs,metrics}/handler.go` + `internal/infra/metrics/ingest.go`)

| Metric | Type | Labels |
|---|---|---|
| `optikk_ingest_records_total` | counter | `signal` (spans/logs/metrics), `result` |
| `optikk_ingest_record_bytes_total` | counter | `signal` |
| `optikk_ingest_handler_publish_duration_seconds` | histogram | `signal`, `result` |
| `optikk_ingest_mapper_duration_seconds` | histogram | `signal` |
| `optikk_ingest_mapper_rows_per_request` | histogram | `signal` |
| `optikk_ingest_mapper_attrs_dropped_total` | counter | `signal` |

### Ingest pipeline (`internal/infra/kafka_ingest/metrics.go`)

All three signals share the generic `Dispatcher[T] → Worker[T] → Writer[T]` pipeline (metrics migrated off the legacy single-goroutine consumer in the ingest refactor round). Every metric below carries a `signal` label so the Grafana dashboard can slice per-signal.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `optikk_ingest_worker_flush_duration_seconds` | histogram | `signal`, `result` | `Writer.Write` end-to-end wall clock per flushed batch |
| `optikk_ingest_worker_flush_rows` | histogram | `signal`, `reason` (size/bytes/time/stop) | Rows per flushed batch, labeled by the accumulator trigger |
| `optikk_ingest_worker_flush_bytes` | histogram | `signal` | Accumulated bytes per flushed batch |
| `optikk_ingest_worker_queue_depth` | gauge | `signal`, `partition` | Per-partition inbox depth, sampled every 100ms tick |
| `optikk_ingest_worker_paused_partitions` | gauge | `signal` | Count of partitions paused for backpressure |
| `optikk_ingest_writer_ch_insert_duration_seconds` | histogram | `signal`, `result` | `PrepareBatch → Append → Send` wall clock per attempt |
| `optikk_ingest_writer_ch_rows_total` | counter | `signal`, `result` | Rows handed to ClickHouse per attempt |
| `optikk_ingest_writer_retry_attempts_total` | counter | `signal` | Retries beyond the first attempt |
| `optikk_ingest_writer_dlq_sent_total` | counter | `signal`, `reason_code` | Batches routed to DLQ after retry exhaustion |
| `optikk_ingest_writer_dlq_publish_failed_total` | counter | `signal` | DLQ publish errors (rate-limited-logged) |

Flush-reason cardinality is capped at 4 (`size | bytes | time | stop`). DLQ reason-code cardinality at 4 (`timeout | canceled | other | ok`). Both deliberately low-cardinality so the counters are Prometheus-safe.

## Logging contract

- Every log line flows through `internal/shared/slogx.TraceAttrHandler`
  → `FanoutHandler{stdout, otelBridge}`. Stdout shape depends on
  `LOG_FORMAT` (`json` for prod, tint-coloured text otherwise).
- Records emitted with `*Context(ctx, …)` variants automatically carry
  `trace_id` + `span_id` when the ctx has an OTel span. Use `slog.*Context`
  at every boundary — the sweep already migrated the major call sites.
- HTTP middleware reserves these fields on the request log line:
  `method, route, status, duration_ms, remote_ip, user_agent, team_id,
  trace_id, span_id`.
- gRPC interceptor reserves: `method, code, duration_ms, trace_id, span_id`.
- DB instrument helpers log one `db query failed` (ClickHouse) /
  `mysql query failed` line per error with `op, duration_s, error`.

## Local monitoring stack (opt-in)

Alongside the Grafana Cloud pipeline, `deploy/monitoring/` ships a standalone Prometheus + Grafana pair for sub-minute-resolution ingest dashboards without the Grafana Cloud ingest cost.

Bring up:

```bash
docker compose -f deploy/monitoring/stack/docker-compose.yml up -d
# Grafana    → http://localhost:13001    (admin / admin unless overridden via .env)
# Prometheus → http://localhost:19091
```

Non-default host ports (`13001`, `19091`) avoid colliding with the root `docker-compose.yml` — which already binds backend `:19090` (`/metrics`), MariaDB `:13306`, ClickHouse `:19000`, otel-collector `:14317/:14318`, Redis `:16379`, Redpanda `:19092`.

Provisioning is automatic:

- `deploy/monitoring/grafana/provisioning/datasources/prometheus.yml` registers the Prometheus datasource.
- `deploy/monitoring/grafana/provisioning/dashboards/dashboards.yml` loads JSON dashboards from `/var/lib/grafana/dashboards`.
- `deploy/monitoring/grafana/dashboards/optikk_ingest.json` is the starter dashboard — 10 panels covering ingest rate, handler publish latency, worker flush latency, CH insert latency, queue depth (per-partition), paused partitions, flush reason distribution, DLQ rate, Kafka consumer lag, and attribute drops. A `signal` multi-select var lets operators slice any panel across logs / spans / metrics.

Directory layout respects the flat-vs-nested rule (`monitoring/` holds only subdirs):

```
deploy/monitoring/
├── stack/                          ← files (compose + prometheus.yml)
│   ├── docker-compose.yml
│   └── prometheus.yml
└── grafana/                        ← subdirs
    ├── dashboards/
    │   └── optikk_ingest.json
    └── provisioning/
        ├── dashboards/dashboards.yml
        └── datasources/prometheus.yml
```

Independently revertible: `docker compose -f deploy/monitoring/stack/docker-compose.yml down -v` cleans up volumes and containers with no effect on the root stack or Grafana Cloud pipeline.

## Configuration

`config.yml` → `telemetry.otel`:

```yaml
telemetry:
  otel:
    enabled: true
    endpoint: 127.0.0.1:14317
    sample_ratio: 1.0
    service_name: optikk-backend
```

`LOG_LEVEL` env var controls both the stdout handler and the bridge
handler's floor. `LOG_FORMAT=json` switches stdout to structured.

### Ingest pipeline tuning

`config.yml` → `ingestion.pipeline.{logs,spans,metrics}` tunes the generic Kafka → Accumulator → Writer pipeline per signal. All fields optional — zero values inherit `config.DefaultIngestPipelineConfig` (tuned for ~200k records/s/instance):

```yaml
ingestion:
  pipeline:
    logs:
      max_rows: 10000                 # accumulator: rows trigger
      max_bytes: 16777216             # accumulator: bytes trigger (16 MiB)
      max_age_ms: 250                 # accumulator: age trigger
      worker_queue_size: 4096         # per-partition channel cap
      pause_depth_ratio: 0.8          # PauseFetchPartitions when depth ≥ 80%
      resume_depth_ratio: 0.4         # ResumeFetchPartitions when depth ≤ 40%
      writer_max_attempts: 5
      writer_base_backoff_ms: 100
      writer_max_backoff_ms: 5000
      writer_attempt_timeout_ms: 30000
      async_insert: true              # SETTINGS async_insert=1, wait_for_async_insert=1
    spans: {}                         # inherits defaults
    metrics: {}                       # inherits defaults (and joins the generic pipeline — legacy consumer retired)
```

`async_insert` is the single largest lever — CH batches across connections server-side, trading a few ms of write latency for multi-x throughput under merge pressure. `wait_for_async_insert=1` preserves at-least-once.

## Outbound HTTP clients

The backend makes no outbound HTTP calls today (verified by a repo-wide
grep for `http.Client`, `http.NewRequest`, `http.Get`, `http.Post`,
`http.RoundTripper`, `http.Transport` under `internal/` + `cmd/`). If
any land in the future, the convention is:

```go
client := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
```

`otelhttp` is already an indirect dep via `otelgin`, so no new
top-level dep is required when that time comes.

## Out of scope

- Threading the `kafka.consume` span ctx into the Worker batch pipeline
  so DB writes appear as children of the consume span. Requires
  `Item.Ctx` + `Worker.Push` signature changes — deferred to a later
  round.
- Per-batch (rather than per-record) consume spans, if span volume
  becomes a Grafana Cloud ingest concern.
- `redigo` session pool — only the `go-redis/v9` client is hooked;
  redigo callers stay unmeasured until migrated.
- MySQL slow-query log integration.
- Dashboards + alert rules — those live in Grafana Cloud, provisioned
  there rather than in-repo.
