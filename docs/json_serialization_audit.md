# `toJSONString` audit

The general rule: **don't ship typed JSON through string serialization just to re-parse it on the other side.** CH's `attributes JSON(max_dynamic_paths=100)` column is already structured at rest; `toJSONString(attributes)` round-trips it into a JSON string, which the Go side then parses again. Two redundant passes on a per-row hot path.

This audit walks every current `toJSONString` call site, scores the cost, and recommends what to do.

Last updated 2026-05-05.

## Inventory

| # | Site | Call shape | Per-row work | Verdict |
|---|---|---|---|---|
| 1 | [traces/detail/repository.go:36](internal/modules/traces/detail/repository.go#L36) (`GetSpanAttributes`) | `toJSONString(attributes) AS attributes_json` | Serialize JSON column → ship string → Go `json.Decoder` walks tree → custom flat `map[string]string` | **Acceptable, document.** Query reads exactly one row (`LIMIT 1`); cost is amortized over a single span detail open. |
| 2 | [traces/suggest/repository.go:54](internal/modules/traces/suggest/repository.go#L54) (`SuggestAttribute`) | `JSONExtractString(toJSONString(attributes), @attrKey)` | Serialize full attributes JSON to string per row, then re-parse string to extract one key per row, GROUPed over the entire window | **Replace.** Clear waste — typed JSON columns support direct dynamic-key access. |
| 3 | [db/clickhouse/07_metrics_1m.sql:49](db/clickhouse/07_metrics_1m.sql#L49) (`metrics_1m_mv` definition) | `cityHash64(toJSONString(attributes)) AS attr_hash` | Run once per ingestion row at MV evaluation time, materialized into the rollup PK | **Leave alone.** Schema-level; changing it is a migration that rebuilds `metrics_1m`. |

## Site-by-site tradeoffs

### 1. `GetSpanAttributes` — `attributes_json` round-trip

**What it does**: PREWHEREs one specific `(team_id, span_id, trace_id)` row, projects the typed JSON `attributes` column as a serialized JSON string, ships ~1 KB-1 MB of JSON to Go. Service then calls `flattenJSONAttrs` ([service.go:265](internal/modules/traces/detail/service.go#L265)) which walks the JSON via `json.NewDecoder` and produces a custom `map[string]string` with dot-joined keys (`a.b.c`), array indices (`arr.0`, `arr.1`), null-as-empty-string, etc.

**Why the round-trip exists**: the FE wire shape (`SpanAttributes.Attributes map[string]string`) is a flat string-keyed map — that's not what a CH JSON-column scan would naturally produce. The driver would either hand back nested objects/arrays preserving structure, or scan typed-path subcolumns into their concrete Go types. Neither matches the flat-map shape the FE expects.

**Could we skip `toJSONString`?**
- **Direct JSON column scan**: clickhouse-go v2 supports scanning typed JSON columns (since ~2.30), but the natural target is a structured Go type, not `map[string]string`. We'd still need the same Go-side flatten pass to produce dot-joined keys. We'd save the SQL serialize step but add complexity to the scan path.
- **`JSONAllPathsWithTypes(attributes)` + per-path projection**: enumerate every path Go-side, then project each with `attributes.'<path>'::String`. Two queries per request, dynamic SQL. Worse on every axis.
- **Keep current approach**: 1 query, 1 row, 1 string scan, 1 Go parse. Total cost is bounded — `LIMIT 1` means we never serialize more than one attribute set per request.

**Recommendation**: **keep**. This is the right tradeoff for a `LIMIT 1` detail-page point lookup. The cost is paid once per span-detail open. Document the dto field comment to make the round-trip explicit (already done at [dto.go:35](internal/modules/traces/detail/dto.go#L35)).

The rule of thumb that emerges: **`toJSONString` is acceptable in repo SQL when (a) the row count is bounded to a small constant (≤ a few rows), AND (b) the response wire shape needs the full attribute bag flattened.** Both conditions hold here.

### 2. `SuggestAttribute` — `JSONExtractString(toJSONString(attributes), @attrKey)`

**What it does**: typeahead query for `@<attribute-key>` filter values. Reads `observability.spans` over a window, for **every row** computes `JSONExtractString(toJSONString(attributes), @attrKey)`, GROUPs the result, returns top-K matching prefix.

**Why this is wrong**:
- `toJSONString(attributes)` serializes the full typed JSON to a string per row.
- `JSONExtractString(<string>, @attrKey)` then re-parses that string per row to fish out one key.
- The typed JSON column already supports direct dynamic-key access — the round-trip is pure waste.
- Cost scales with row count in the window (potentially millions of spans).

**Replacement**:
```sql
SELECT attributes[@attrKey]::String AS value, count() AS count
```

`attributes[@attrKey]` is the typed-JSON dynamic subscript — it hits the structured column directly, no string serialization, no JSON re-parse. The existing `attributes.'<key>'::String` pattern (used throughout `saturation/database/*` and `infrastructure/*` repos for compile-time-known keys) is exactly this, and the bracket form works for runtime-bound keys.

**Tradeoff of switching**: none meaningful. Dynamic subscripts on typed JSON columns are a stable CH feature (since the typed JSON GA in 25.x). The only behavior change is that the result for missing keys becomes the empty string directly instead of going through `JSONExtractString`'s missing-key handling — both produce `""`, no test impact.

**Recommendation**: **replace**. Drop both `toJSONString` and `JSONExtractString`; use `attributes[@attrKey]::String`. The query becomes faster proportional to row count and JSON size — for a wide span with many attributes over a 24h window, this is a measurable win.

Sketch of the replacement:
```sql
SELECT attributes[@attrKey]::String AS value, count() AS count
FROM observability.spans
PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
WHERE timestamp BETWEEN @startMs AND @endMs
  AND value != ''
  AND (length(@prefix) = 0 OR positionCaseInsensitive(value, @prefix) > 0)
GROUP BY value
ORDER BY count DESC
LIMIT @limit
```

### 3. `metrics_1m_mv` — `cityHash64(toJSONString(attributes))` for `attr_hash`

**What it does**: at ingestion time, the metrics_1m materialized view computes a stable 64-bit hash of the entire attribute set per data point and stores it as the `attr_hash` column in the rollup's PK. This lets distinct attribute combinations land in distinct PK rows without enumerating attributes in the GROUP BY.

**Why this is fine where it lives**:
- Runs once per ingestion row at MV evaluation time, **never at read time**.
- The result is materialized into `attr_hash UInt64` and read directly as a column thereafter.
- We need a stable canonical hash of an arbitrary attribute set; `toJSONString` provides one canonicalization (object key ordering is deterministic in CH).
- Changing it requires a schema migration that rebuilds the entire `observability.metrics_1m` table — order of magnitude more expensive than any per-query saving could justify.

**Could we replace it?** Theoretically — `cityHash64(arrayMap(p -> (p, JSONType(attributes, p), …), JSONAllPaths(attributes)))` or similar typed enumeration. But:
- `JSONAllPaths` returns paths in non-deterministic order across MV evaluations (uncertain), risking PK collisions across rows that should have collapsed.
- The serialization cost is a write-time cost paid once per ingestion row, not per query — Prometheus dashboards observing CH ingestion CPU don't show this as hot.

**Recommendation**: **leave alone.** Re-evaluate only if profiling shows the MV evaluation as a meaningful share of CH ingestion CPU.

## Convention to enforce going forward

Per CLAUDE.md the existing rule says `toJSONString` is allowed (alongside `JSONExtract*`, `JSONAllPathsWithTypes`, typed-path subscripts). After this audit, refine the rule to:

> **`toJSONString(attributes)` in repo SQL is only acceptable when the row count is a small bounded constant** (e.g. `LIMIT 1` point lookups where the response needs the full flattened attribute bag). For per-row projections in window-scoped queries, use typed-path subscripts (`attributes.'<key>'::String` for compile-time keys, `attributes[@key]::String` for runtime-bound keys). Schema-level MV definitions are out of scope; treat each on its own merits.

## Net effect of the recommended changes

If we replace site #2:
- One repo edit: [traces/suggest/repository.go:54](internal/modules/traces/suggest/repository.go#L54) — swap `JSONExtractString(toJSONString(attributes), @attrKey)` → `attributes[@attrKey]::String`.
- No schema change.
- `SuggestAttribute` latency drops proportional to (row count × attribute payload size). On a wide-span tenant over a 24h window this should move the endpoint from a soft Tier B to a comfortable Tier B.
- No FE-visible behavior change.

Site #1 stays as-is with a clearer comment; site #3 stays untouched. Total change: one query string.
