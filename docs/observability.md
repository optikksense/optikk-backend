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
producer and consumer, see `internal/infra/kafka/client.go`). Consumer
lag gauge is published by `LagPoller` on a 15 s ticker.

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

### Ingest (`internal/ingestion/{spans,logs,metrics}/handler.go`)

| Metric | Type | Labels |
|---|---|---|
| `optikk_ingest_records_total` | counter | `signal` (spans/logs/metrics), `result` |
| `optikk_ingest_record_bytes_total` | counter | `signal` |

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

## Out of scope

- `redigo` session pool — only the `go-redis/v9` client is hooked;
  redigo callers stay unmeasured until migrated.
- MySQL slow-query log integration.
- Dashboards + alert rules — those live in Grafana Cloud, provisioned
  there rather than in-repo.
