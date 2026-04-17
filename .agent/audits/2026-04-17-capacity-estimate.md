---
date: 2026-04-17
scope: current-code capacity vs Datadog scale
prior-audit: 2026-04-17-scalability-audit.md
phases-shipped: 1 (data-loss fixes), 2 (Redis-only livetail), 3 (alerting HA + subpackage split), 4 (CH query profiles), 5 (Prometheus + OTel collector), 6 (AI subpackage split)
---

# Capacity estimate — current code vs Datadog scale

## TL;DR ceiling

After the first six phases, a **single-pod, single-CH-node** Optikk deployment can comfortably serve:

| Dimension | Comfortable | Strained | Will break |
|-----------|-------------|----------|------------|
| **Tenants** per region | 20–50 | 100–200 | ≥ 500 |
| **Spans ingested** (steady) | 30–50 k/s | 80–120 k/s | ≥ 150 k/s |
| **Logs ingested** (steady) | 50–80 k/s | 120–180 k/s | ≥ 250 k/s |
| **Metric points** (steady) | 100–150 k/s | 250–400 k/s | ≥ 500 k/s |
| **Spans retained** (24h) | ≤ 3 B | 5–8 B | ≥ 15 B |
| **Overview query** p95 over 24 h | < 2 s (≤ 500 M tenant spans) | 3–8 s | > 15 s timeout |
| **Alert rules enabled** | ≤ 2 k | 5 k | ≥ 10 k (eval lag > 2 ticks) |
| **Live-tail WS clients** per pod | ≤ 100 (hard cap) | — | > 100 returns 503 |
| **Alert notification latency** | 1–3 s fast-path, ≤ 15 s durable | same | — |

**Headline gap vs Datadog:** roughly **3–4 orders of magnitude on ingest** and **2–3 on query fan-out**. DD ingests ~10 M pts/s/region across multi-hundred-node CH fleets with per-tenant rollup pipelines; Optikk today is single-flusher-per-signal with no rollups, and one CH node as the hot store. Closing 50 % of the gap is mechanical (phases in the original audit); the rest requires multi-region cells.

---

## Component-by-component

### 1. OTLP ingest path

```
OTLP gRPC → authenticator → Dispatcher.Dispatch → Kafka (per-signal topic)
                                                     ↓
                          Worker.runPersistence(AckableBatch) → CHFlusher.Flush → ClickHouse
```

**Current ceiling (per pod):**

| Stage | Throughput per pod | Bottleneck |
|-------|---------------------|------------|
| gRPC receive | ~ 20–30 k calls/s | Go net stack + per-batch auth |
| Dispatcher.Dispatch | ~ 5–10 k batches/s | JSON marshal + sync produce |
| Kafka consume | ~ 100 MB/s | Single partition per signal today |
| **CHFlusher per signal** | **~ 50 k rows/s** | Single sync goroutine per signal |
| CH write | 200 k rows/s per node | 1 CH node |

**Phase 1 correctness:** zero silent drops, CH failures DLQ, crash-safe redelivery collapses via `insert_deduplication_token`.

### 2. ClickHouse storage + query layer

**Per-tenant span-volume thresholds for < 2 s overview queries over 24 h:**

| Daily spans (tenant) | Overview p95 | Verdict |
|----------------------|--------------|---------|
| 100 k              | 100–200 ms | ✅ fine |
| 10 M               | 300–700 ms | ✅ fine |
| 100 M              | 1.5–3 s    | ⚠ borderline |
| 1 B                | 8–15 s     | ❌ hits 15 s budget |
| 10 B               | timeout    | ❌ |

**Without rollups, Optikk's safe per-tenant volume is roughly 50–100 M spans/day** if you want overview to stay sub-second.

### 3. Live tail

Single Redis instance handles ~100–200 k ops/s total. Hard limit is the 100-WS-per-pod cap in `redis_hub.go:17`. Deploy more pods to scale WS fan-out.

### 4. Alerting engine

- 1 rule eval ≈ 50–150 ms (CH query on 5 min window).
- Single evaluator goroutine per pod → ~ 200–600 rules/tick max.
- At 1 000 rules enabled → eval tick takes 100–300 s → lag exceeds tick interval.

**Real gap at > 500 rules enabled.** Fix: M=8 worker pool (~30 lines change in `evaluator_loop.go`).

**Notification durability** (Phase 3.3): every transition lands in MySQL `alert_outbox` before the fast-path Slack send. Zero drops.

### 5. Query tenant isolation

**Gap: none at the service tier.** A single tenant's `SelectExplorer` can consume the CH cluster's 60 s budget and slow everyone's overview queries. Not a correctness issue; a noisy-neighbor issue.

### 6. Multi-region / cells

**Not implemented.** `AppConfig.Region` reads from config but is not enforced anywhere.

---

## Scaling envelope by tenant size

| Tenant shape | Spans/day/tenant | Max (current) | Max (+rollups) | Max (+rollups+sharded) |
|--------------|------------------|---------------|----------------|-------------------------|
| Hobby      | < 100 k | 2 000 | 10 000 | 30 000 |
| Small SaaS | 1–10 M  | 300 | 2 000 | 8 000 |
| Mid-market | 100 M–1 B | 20 | 150 | 500 |
| Enterprise | 10 B+ | 0–1 (timeout) | 5 | 30 |

**Datadog-scale enterprise** (50 B+ spans/day/tenant, 500+ alert rules) needs all of: rollups, sharded flushers, per-tenant concurrency budgets, multi-region cells, CH sharding, dedicated ingest tier. That's 6–12 weeks beyond the current state.

---

## What to build next (ranked by capacity unlock)

| # | Gap | Lift | ROI |
|---|-----|------|-----|
| 1 | Rollup MVs + repository routing | 1 week | 10× on overview p95 |
| 2 | Sharded CH flushers | 2 days | N× linear ingest |
| 3 | Leased-worker pool for evaluator | 1 day | 8× alert capacity |
| 4 | Per-tenant CH concurrency cap | 3 days | Noisy-neighbor isolation |
| 5 | CH horizontal sharding | 2 weeks | 10× storage + query per region |
| 6 | Multi-region cells | 2 weeks | N× aggregate; EU/APAC compliance |
| 7 | Dedicated ingest tier | 1 week | Workload isolation |
| 8 | OTel SDK init in main.go | 2 days | Self-observability |

**If I had to pick only two:** #1 (rollups) + #2 (sharded flushers). Those two together unlock ~50× ingest + ~10× query on the same infra.

---

## Bottom line

- Optikk is **correctness-ready** for small-to-mid SaaS loads (up to ~50 customers, up to ~100 M spans/day each).
- **Datadog scale is roughly 500×–5000× away** depending on dimension. Closing 90 % of that gap is ~6 weeks of focused work on the ranked list above.
- **Don't underestimate self-observability** — the Prometheus/OTel stack is provisioned but `cmd/server/main.go` doesn't emit.
