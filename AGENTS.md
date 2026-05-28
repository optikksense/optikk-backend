# Optikk Backend — Agent Rules

This file is **rules only**. Project info (modules, ingestion, ClickHouse schema, read paths, helpers, runbook) lives in [CODEBASE_INDEX.md](CODEBASE_INDEX.md) and [db/clickhouse/README.md](db/clickhouse/README.md).

## The bar

This project is held to the **highest engineering standards**. Match the bar of the existing code: correctness, clarity, minimality, query budgets respected, every byte across the wire justified. No `fmt.Sprintf` in SQL, no phantom rollup refs, no Go-side recompute of work CH can do server-side, no shortcuts that "work but won't scale." If a change would lower the bar — skip it, push back, or ask. "It compiles and the test passes" is not the bar; "it is the right change, done the right way, at the right cost" is.

## Before any task

1. **Research is mandatory — not optional.** Before writing a single line, do the work to understand what you're about to do:
   - Read [CODEBASE_INDEX.md](CODEBASE_INDEX.md), [db/clickhouse/README.md](db/clickhouse/README.md), and the actual code you'll touch — sibling readers, the relevant filter/CTE helpers, the rollup tier the module reads from.
   - Verify against authoritative docs for anything ClickHouse-specific (functions, aggregate combinators, MergeTree options, JSON paths, query budgets, partition pruning, skip indexes). For OTel semconv attributes, check the spec — assume the latest emitted by our ingestion mapper.
   - For anything non-trivial — new rollup/MV design, distributed-systems behavior, consistency / consensus / replication tradeoffs, indexing strategies, ingestion backpressure, query planners, OTel semconv changes, performance regressions — **read the relevant research papers, RFCs, ClickHouse engineering blog posts, and OTel design docs** before designing. The CH team publishes how they built `quantileTiming`, `AggregatingMergeTree`, the JSON type; the OTel TC publishes its semconv stability rationale; the distributed-systems literature has well-known prior art. Use it. "I haven't read it but I think…" is not acceptable.
   - Borrowing an existing reader pattern from this repo beats inventing one. Inventing one without checking what's already there, or designing something subtle without checking what the literature already says, is a violation.
2. **Plan first.** For non-trivial work (anything beyond a one-line fix), write a plan via `EnterPlanMode` and wait for approval before editing.

## After every iteration

After completing any task — no matter how small — review and update the following if anything changed:

1. [CODEBASE_INDEX.md](CODEBASE_INDEX.md) — new modules, endpoints, helpers, or schema.
2. [db/clickhouse/README.md](db/clickhouse/README.md) — new migration files or schema-level facts.

This is **mandatory**. Documentation must always reflect the current architecture.

## Code patterns

- **Module architecture**: strict 6-file pattern per module (`handler.go`, `service.go`, `repository.go`, `module.go`, `dto.go`, `models.go`). All repository methods stay in `repository.go`.
- **Apm-style reader discipline**: every reader is queries-only `repository.go` + derivations in `service.go`. SQL is `const`; values bound via `clickhouse.Named()`. **No `fmt.Sprintf` in any reader SQL. No phantom rollup table refs.** Emit the `WITH active_fps AS (... <signal>_resource ...)` CTE for partition pruning **only when a resource-side filter is present**; otherwise the CTE is pure overhead and should be omitted. Push time-range predicates (`timestamp BETWEEN @start AND @end`) into PREWHERE so CH can use per-granule min/max stats — explicit PREWHERE disables auto-promotion, so the move must be manual; keep the same condition in WHERE as the base for filter clauses to tack onto.
- **Scan native CH types; convert in Go**: the clickhouse-go v2 driver does **not** implicitly coerce between numeric or temporal types — drift between a SELECT's native return type and the Go scan field is a `[ScanRow]` runtime error. Mapping: `ts_bucket` (UInt32) → `uint32`; `timestamp` (DateTime/DateTime64) → `time.Time`; `count()` / `countIf(…)` / `sum(UInt*)` → `uint64`; `sum(Int*)` and `sum(a) - sum(b)` (CH widens mixed/signed to Int64) → `int64`; `min/max(timestamp)` → `time.Time`. Do NOT add type-coercion casts in SQL purely to satisfy a Go scan field — no `toDateTime(ts_bucket)`, `toUnixTimestamp64Milli(timestamp)`, `toUInt64(sum)`, `toInt64(count)`, `toFloat64(hist_count)`, `CAST(json_col, 'Map(String, String)')`. If the wire model wants a different shape, convert in `service.go`: `timebucket.BucketTime(b)` / `timebucket.BucketDateTimeString(b)` for ts_bucket, `t.UnixMilli()` for time → ms, clamp `<0 → 0` then `uint64(v)` for Int64 differences, `flattenJSONAttrs(s)` (or equivalent) for JSON columns scanned via `toJSONString`. **Allowed (not violations)**: parsing on String columns (`toUInt16OrZero`, `toUInt8OrZero`); bucketing transforms (`toStartOfMinute/FiveMinutes/Hour/Day/Interval`); hash functions (`cityHash64`); case-normalization (`lower`, `lowerUTF8`); aggregation combinators (`quantilesTimingMerge`, `quantileTimingState`, `argMax`); JSON access (`toJSONString`, `JSONExtract*`, `JSONAllPathsWithTypes`, typed-path subscripts); explicit string-formatting (`formatDateTime`). A cast is a **violation** only when its sole purpose is to make a SELECT result match a Go scan field type.
- **Time bucketing**: `ts_bucket` is Go-only — no reader SQL computes a `ts_bucket` value. Display-time aggregation is server-side via `timebucket.DisplayGrainSQL(windowMs)` returning a `toStartOfX(timestamp)` fragment. Pair with `WithBucketGrainSec(args, startMs, endMs)` to bind `@bucketGrainSec` for SQL-side `count / @bucketGrainSec` rate computation. Changing `BucketSeconds` is a breaking schema change.
- **Quantiles are server-side**: spans-side latency readers project `quantilesTimingMerge(...)(latency_state)` on `spans_1m`, or inline `quantileTiming`/`quantilesTiming` on raw spans (free-text `db_statement` only). Metrics-side OTel histograms project `quantilesPrometheusHistogramMerge(0.5, 0.95, 0.99)(latency_state)` on `metrics_1m`. There is no Go-side quantile package. Wrap the merge in a subquery so the `qs` array alias doesn't leak into the row scan.
- **Use shared helpers, don't reinvent**: `httputil` for HTTP (`RespondOK`, `RespondError`, `ParseRequiredRange`, `WithComparison`, `MaxPageSize=200`); `errorcode` for stable codes; `dbutil` (`SelectCH/QueryCH/ExecCH`, `SelectSQL/GetSQL/ExecSQL`) for the instrumentation seam — pass a meaningful `op` label (e.g. `"logsDetail.GetByID"`); `clickhouse.Named()` for binds; the three `DashboardCtx`/`OverviewCtx`/`ExplorerCtx` budget contexts for CH query timeouts.
- **AI observability privacy**: aggregate/list endpoints must not project prompt/input/output body content unless explicitly requested and reviewed.

## Engineering principles

- **SOLID & DRY**: factor shared behavior when a pattern appears more than once.
- **Quality**: leave the code clearer or simpler with every change.
- **No unsolicited tests**: do not add tests unless explicitly asked.
