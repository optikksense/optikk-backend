-- slo_burn — narrow rollup for the /overview/slo/* endpoints. Collapses
-- the 6-dim spans_red rollup to just (team_id, bucket_ts, service_name),
-- pre-aggregating request_count + error_count + latency_digest. Enables
-- single-query multi-window burn-rate computation via sumMergeIf.

CREATE TABLE IF NOT EXISTS observability.slo_burn_1m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    service_name   LowCardinality(String),
    request_count  AggregateFunction(sum, UInt64),
    error_count    AggregateFunction(sum, UInt64),
    duration_ms_sum AggregateFunction(sum, Float64),
    latency_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.slo_burn_raw_to_1m
TO observability.slo_burn_1m AS
SELECT
    team_id, bucket_ts, service_name,
    sumMergeState(request_count)  AS request_count,
    sumMergeState(error_count)    AS error_count,
    sumMergeState(duration_ms_sum) AS duration_ms_sum,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_digest
FROM observability.spans_red_1m
GROUP BY team_id, bucket_ts, service_name;

-- 5m tier
CREATE TABLE IF NOT EXISTS observability.slo_burn_5m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    service_name   LowCardinality(String),
    request_count  AggregateFunction(sum, UInt64),
    error_count    AggregateFunction(sum, UInt64),
    duration_ms_sum AggregateFunction(sum, Float64),
    latency_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.slo_burn_1m_to_5m
TO observability.slo_burn_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    service_name,
    sumMergeState(request_count)  AS request_count,
    sumMergeState(error_count)    AS error_count,
    sumMergeState(duration_ms_sum) AS duration_ms_sum,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_digest) AS latency_digest
FROM observability.slo_burn_1m
GROUP BY team_id, bucket_ts, service_name;

-- 1h tier
CREATE TABLE IF NOT EXISTS observability.slo_burn_1h (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    service_name   LowCardinality(String),
    request_count  AggregateFunction(sum, UInt64),
    error_count    AggregateFunction(sum, UInt64),
    duration_ms_sum AggregateFunction(sum, Float64),
    latency_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.slo_burn_5m_to_1h
TO observability.slo_burn_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    service_name,
    sumMergeState(request_count)  AS request_count,
    sumMergeState(error_count)    AS error_count,
    sumMergeState(duration_ms_sum) AS duration_ms_sum,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_digest) AS latency_digest
FROM observability.slo_burn_5m
GROUP BY team_id, bucket_ts, service_name;
