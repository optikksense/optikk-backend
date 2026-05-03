# Trace Assembly Flow

The Trace Assembler is a spans-only background component that aggregates raw span writes into a single summary row per trace in `observability.traces_index`.

**Location:** `internal/ingestion/spans/indexer/`

---

## Why it exists

ClickHouse `observability.spans` stores every span row individually. The traces explorer (`/api/v1/traces/query`) needs per-trace aggregates — service set, error count, root operation, duration — without scanning millions of span rows on every query. The assembler materialises these aggregates incrementally at ingest time and writes them to `observability.traces_index` (one row per trace).

---

## State machine diagram

```mermaid
stateDiagram-v2
    [*] --> Pending : first span for (teamID, traceID) observed

    Pending --> Pending : mergeSpan — accumulate fields\n(spanCount, errors, services, time bounds, root fields)

    Pending --> Emitting : rootSeen = true\nAND now − lastSeen ≥ 10 s (quiet window)
    Pending --> Emitting : now − firstSeen ≥ 60 s (hard timeout)\ntruncated = true

    Emitting --> [*] : CHEmitter.Emit → observability.traces_index\nstate evicted from LRU map

    Pending --> Emitting : LRU capacity exceeded (100 000 traces)\ntruncated = true

    note right of Pending
        Fields accumulated per trace:
        - spanCount, errorCount, errorFp
        - services (set), peerServices (set)
        - startMs, endMs
        - rootService, rootOperation
        - rootStatus, rootHTTPMethod, rootHTTPStatus
        - environment
        - firstSeen, lastSeen (for window checks)
    end note
```

---

## Component flow

```mermaid
flowchart TD
    WRITE_OK["consumer/writer.go\nWriter.Write success\n─\nrowToSpan(row) → indexer.Span"]
    WRITE_OK --> OBSERVE

    subgraph ASSEMBLER ["indexer/assembler.go — Assembler"]
        OBSERVE["Assembler.Observe(ctx, span)\n─\nstate.Upsert(teamID, traceID, now, tsBucketStart)"]

        OBSERVE -->|new entry| PENDING["pending trace created\nfirstSeen = now\nlastSeen = now"]
        OBSERVE -->|existing entry| MERGE
        OBSERVE -->|LRU eviction| EVICT_EMIT["emit(ctx, evicted, truncated=true)"]

        MERGE["mergeSpan(pending, span)\n─\nspanCount++\nif IsError: errorCount++, set errorFp\nservices.add(serviceName)\npeers.add(peerService)\nstartMs = min(...)\nendMs   = max(...)\nif IsRoot: capture root_* fields\nlastSeen = now"]
        MERGE --> PENDING_STATE[("pending state\nLRU map\n≤ 100 000 traces")]
    end

    subgraph SWEEP ["Sweep goroutine (every 5 s)"]
        TICKER["5 s ticker\nassembler.go:Start()"]
        TICKER --> SWEEP_FN["state.Sweep(now, quietWindow, hardTimeout)\n─\nfor each pending trace:\n  rootSeen AND now−lastSeen ≥ 10s → complete\n  now−firstSeen ≥ 60s            → timeout"]
        SWEEP_FN -->|complete| EMIT_OK["emit(ctx, pending, truncated=false)"]
        SWEEP_FN -->|timeout| EMIT_TRUNC["emit(ctx, pending, truncated=true)"]
    end

    EMIT_OK --> TO_ROW
    EMIT_TRUNC --> TO_ROW
    EVICT_EMIT --> TO_ROW

    TO_ROW["pending.ToRow(truncated)\n─\nbuild TraceIndexRow struct"]
    TO_ROW --> EMITTER

    subgraph EMITTER_PKG ["indexer/emitter.go — CHEmitter"]
        EMITTER["CHEmitter.Emit(ctx, row)\n─\nch.PrepareBatch\nbatch.Append(rowValues(row)...)\nbatch.Send()"]
    end

    EMITTER --> CH[("ClickHouse\nobservability.traces_index")]
```

---

## TraceIndexRow columns

| Column | Type | Source |
|--------|------|--------|
| `team_id` | UInt32 | from span |
| `ts_bucket_start` | UInt64 | earliest bucket seen |
| `trace_id` | String | from span |
| `start_ms` | Int64 | min of all span start times |
| `end_ms` | Int64 | max of all span end times |
| `duration_ns` | Int64 | `(end_ms − start_ms) × 1e6` |
| `root_service` | String | root span `service.name` |
| `root_operation` | String | root span name |
| `root_status` | String | root span status code |
| `root_http_method` | String | root span HTTP method |
| `root_http_status` | UInt16 | root span HTTP status |
| `span_count` | Int64 | total spans seen |
| `has_error` | Bool | `errorCount > 0` |
| `error_count` | Int64 | spans with error status |
| `service_set` | Array(String) | unique services in trace |
| `peer_service_set` | Array(String) | unique peer services |
| `error_fp` | String | first error fingerprint |
| `environment` | String | `deployment.environment` attribute |
| `truncated` | Bool | true if hard-timeout or LRU eviction |
| `last_seen_ms` | Int64 | unix ms of last span seen |

---

## Configuration

```yaml
# internal/ingestion/spans/indexer/ defaults (assembler.go)
capacity:      100_000   # max pending traces in LRU
quiet_window:  10s       # idle time after root span → emit
hard_timeout:  60s       # max wait before forced emit
sweep_every:   5s        # sweep loop interval
```

---

## Shutdown drain

When the application receives SIGTERM, the assembler's `Stop()` is called:

```
assembler.Stop()
  → cancel ticker ctx
  → drain(ctx, 10 s timeout)
       state.Sweep(now + 1h, quietWindow=0, hardTimeout=0)
       forces ALL pending traces to complete immediately
       emits every remaining trace within the 10 s deadline
```

This guarantees no in-flight traces are silently lost on a graceful restart.

---

## Read path

`observability.traces_index` is queried by:

- `internal/modules/traces/explorer/repository.go` — `ListTraces`, `GetTrace`
- `internal/modules/traces/trace_suggest/repository.go` — DSL autocomplete

All readers use `internal/modules/traces/shared/traceidmatch/` (`WhereTraceIDMatchesCH`) for consistent `trace_id` predicate normalisation.
