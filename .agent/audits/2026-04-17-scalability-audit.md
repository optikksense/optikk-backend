---
date: 2026-04-17
scope: existing features only (no new product surface)
goal: identify architectural gaps that prevent Datadog-scale operation
---

# Scalability Audit — Datadog-grade gaps

Audit is feature-preserving: every recommendation targets an **existing** subsystem and a specific file/path in this repo. No new product verticals are proposed.

## Executive summary

Optikk is a modular Go monolith fronting ClickHouse (spans/logs/metrics), MySQL (tenants, alerts), Redis (sessions, cache, live-tail fan-out optional), and Kafka (OTLP ingest queue). The hot data plane is correct and type-safe, but single-replica assumptions, chatty JSON-over-Kafka, a dual-writer evaluator loop, and query-time fan-out to raw `observability.spans` are the main blockers to Datadog-class scale (Mpoints/s, multi-region, 10k tenants, per-query SLO < 2 s over days of data).

Ordered by leverage (biggest ROI first):

| # | Area | Today | Gap at DD scale | Recommended change |
|---|------|-------|-----------------|--------------------|
| 1 | Ingest serialization | `KafkaDispatcher` JSON-marshals full `TelemetryBatch[T]` per record | JSON doubles CPU & bytes vs protobuf/msgpack; batches > 1 MB risk broker rejection | Switch to proto (reuse OTLP wire types) or msgpack; enable LZ4/zstd producer compression |
| 2 | Query layer on raw spans | Every `/overview/*`, `/spans/red/*`, `/errors/*`, `/http/*` scans `observability.spans` | Linear in row count; no rollup | Introduce ClickHouse **AggregatingMergeTree** MVs per domain: `spans_red_1m`, `spans_red_1h`, `http_routes_1m`, `errors_fingerprint_1m`. Route overview/RED/HTTP queries to rollups by bucket size |
| 3 | CH writer path | Single `CHFlusher` per signal, single goroutine | Head-of-line blocking, no backpressure | Sharded flushers keyed by `team_id % N`; retry with exponential backoff + DLQ topic |
| 4 | Kafka consumer commit-ordering | `consumeLoop` commits each record before CH write completes | Process crash = permanent data loss between commit and CH flush | **SHIPPED Phase 1**: ack-gated commit via `AckableBatch` |
| 5 | Query fan-out & cache | 30 s response cache is blind; no singleflight | Thundering herd on cache expiry | Singleflight on cache miss; per-tenant concurrency limiter around `NativeQuerier` |
| 6 | Alerting evaluator | Single `EvaluatorLoop` goroutine ticks every 30 s, serially evaluates every enabled rule | At 10k rules × O(CH query each) = minutes of lag; no HA | **Partially SHIPPED Phase 3**: Redis lease per rule. Leased-worker pool (M=8) not yet done |
| 7 | Dispatcher durability | Alert Dispatcher was in-memory bounded channel | Notifications lost on crash | **SHIPPED Phase 3**: MySQL outbox + relay with backoff |
| 8 | Session/auth | `scs/v2` with Redis single node, team list loaded per request | Redis single point; team resolution CH round-trip per call | Add local LRU (e.g. 30 s) in `TenantMiddleware`; enable Redis Cluster / Sentinel |
| 9 | Live tail | `livetail.NewHub()` was in-process | Client connected to pod A never sees spans ingested on pod B | **SHIPPED Phase 2**: Redis Streams hub — cross-pod fan-out |
| 10 | Rate limiting | Handled at gateway, not in-process | Per-pod not per-tenant | Token bucket per `team_id` stored in Redis with Lua |
| 11 | Config & multi-region | Single `config.yml`; `AppConfig.Region` exists but no region-pinning | DD runs regional cells | Add `region` column to `teams`; `TenantMiddleware` rejects cross-region; per-region CH cluster |
| 12 | Schema partition key | `PARTITION BY toYYYYMM(timestamp)` on spans | Monthly partitions at DD scale are huge; merge amplification | Switch to `toYYYYMMDD(timestamp)` for hot tables |
| 13 | Observability of self | Uses `slog` + OTLP; no RED metrics on Optikk itself | Can't alert on own ingest lag/drop | **Partially SHIPPED Phase 5**: OTel Collector + Prometheus in compose. SDK emitter in main.go TBD |
| 14 | Ingest auth cache | `auth.NewAuthenticator` uses Redis cache with TTL | OK — but team lookup still MySQL on miss | Add process-local cache ahead of Redis |
| 15 | Health readiness | `healthReady` pings MySQL/CH/Redis synchronously on every request | Probe storms can DoS CH | Cache result for 5 s; background prober goroutine |

## Concrete refactor plan (ordered)

### Phase 1 — stop silent data loss — **SHIPPED**
- Removed `if err == nil` guards in workers.
- `kafka_dispatcher.go` ack-gated commit; `produceToDLQ` on flush failure.
- Added `insert_deduplication_token = sha1(record.Value)` to ClickHouse inserts.

### Phase 2 — Live-tail Redis-only — **SHIPPED**
- Deleted `LocalHub`; `redis_hub.go` is the sole `Hub` impl.
- Redis Streams keyed by `optikk:livetail:<teamID>`.

### Phase 3 — Alerting HA + decomposition — **SHIPPED**
- Redis lease per rule evaluation (`SET NX PX 60000`).
- MySQL `alert_outbox` + `OutboxRelay` with exponential backoff.
- Subpackage decomposition: `alerting/{rules,incidents,silences,slack,engine,shared,factory}`.
- Leased-worker pool (M=8) — DEFERRED.

### Phase 4 — CH query profiles — **SHIPPED**
- `NativeQuerier.SelectOverview` (15 s / 100M / 2GB) + `SelectExplorer` (60 s / 1B / 8GB).
- Explorer paths migrated: `logs/explorer`, `traces/explorer`, `tracedetail`, `ai/explorer`, `metrics` timeseries.
- Legacy `Select` / `QueryRow` kept as deprecated Overview aliases.

### Phase 5 — External observability stack — **PARTIALLY SHIPPED**
- OTel Collector + Prometheus in docker-compose (Grafana removed due to TLS issue).
- OTel SDK init in `cmd/server/main.go` — TBD.

### Phase 6 — AI module decomposition — **SHIPPED**
- `ai/{overview,runs,analytics,explorer,shared,factory}` subpackage split.
- Parent `ai/` directory-only.

### Remaining high-leverage work
1. **Rollup MVs** (Phase 2 of original plan) — 10× overview query lift
2. **Sharded flushers** — N× linear ingest
3. **Leased-worker pool for evaluator** — 8× rule capacity
4. **Per-tenant CH concurrency cap** — noisy-neighbor isolation
5. **Multi-region cells** — gated by customer demand

## Non-goals (explicitly out of scope)

- New telemetry signals (profiles, RUM, synthetics).
- New UI surface.
- Auth/SSO feature additions.
- Anomaly / ML alert conditions.
