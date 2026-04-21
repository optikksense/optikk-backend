-- ---------------------------------------------------------------------------
-- Phase 5 — AggregatingMergeTree rollups for Datadog-pattern pre-aggregation.
--
-- Rationale: the overview dashboards need p50/p95/p99 + counters over ranges
-- that scan millions of raw span/metric rows. Pre-aggregated t-digest state
-- per (team_id, 1-min bucket, dims) lets queries scan thousands of rollup rows
-- instead. `quantileTDigestMerge(state)` and `sumMerge(state)` do server-side
-- merges at read time. No Go-side sketch pipeline needed; MVs keep everything
-- current on every INSERT.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS observability.spans_rollup_1m (
    team_id          UInt32            CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime          CODEC(DoubleDelta, LZ4),

    -- grouping dims for the overview dashboard
    service_name     LowCardinality(String),
    operation_name   LowCardinality(String),
    endpoint         LowCardinality(String),   -- coalesced route/target/name at MV time
    http_method      LowCardinality(String),

    -- t-digest state (same algorithm class as DDSketch; CH native)
    latency_ms_digest AggregateFunction(
        quantilesTDigestWeighted(0.5, 0.95, 0.99),
        Float64, UInt64
    ),

    -- sum state for rates, error rates, averages
    request_count    AggregateFunction(sum, UInt64),
    error_count      AggregateFunction(sum, UInt64),
    duration_ms_sum  AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, endpoint, http_method)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_to_rollup_1m
TO observability.spans_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                         AS bucket_ts,
    service_name                                                                        AS service_name,
    name                                                                                AS operation_name,
    coalesce(nullIf(mat_http_route, ''), nullIf(mat_http_target, ''), name)             AS endpoint,
    http_method                                                                         AS http_method,

    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        duration_nano / 1000000.0, toUInt64(1)
    )                                                                                   AS latency_ms_digest,

    sumState(toUInt64(1))                                                               AS request_count,
    sumState(toUInt64(has_error OR toUInt16OrZero(response_status_code) >= 500))        AS error_count,
    sumState(duration_nano / 1000000.0)                                                 AS duration_ms_sum
FROM observability.spans
WHERE (parent_span_id = '' OR parent_span_id = '0000000000000000');

CREATE TABLE IF NOT EXISTS observability.metrics_histograms_rollup_1m (
    team_id      UInt32           CODEC(T64, ZSTD(1)),
    bucket_ts    DateTime         CODEC(DoubleDelta, LZ4),
    metric_name  LowCardinality(String),
    service      LowCardinality(String),

    -- t-digest state built from histogram rows (weight = hist_count, value = avg per row).
    latency_ms_digest AggregateFunction(
        quantilesTDigestWeighted(0.5, 0.95, 0.99),
        Float64, UInt64
    ),
    hist_count   AggregateFunction(sum, UInt64),
    hist_sum     AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_histograms_to_rollup_1m
TO observability.metrics_histograms_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                          AS bucket_ts,
    metric_name                                                                         AS metric_name,
    service                                                                             AS service,

    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        hist_sum / if(hist_count = 0, 1, hist_count),
        hist_count
    )                                                                                   AS latency_ms_digest,

    sumState(hist_count)                                                                AS hist_count,
    sumState(hist_sum)                                                                  AS hist_sum
FROM observability.metrics
WHERE metric_type = 'Histogram' AND hist_count > 0;
