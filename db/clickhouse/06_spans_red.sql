-- spans_red — canonical APM stats rollup (RED metrics: Rate / Errors / Duration).
-- Source: observability.traces_index (root spans only, one row per trace).
-- PK includes http_status_bucket so status-breakdown queries prune without
-- resorting to spans_peer. All three tiers share identical columns + PK.
--
-- Measures: latency digest (quantilesTDigestWeighted), request count, error
-- count, duration sum. MV body is pure GROUP BY + state; no multiIf.

CREATE TABLE IF NOT EXISTS observability.spans_red_1m (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts            DateTime CODEC(DoubleDelta, LZ4),
    service_name         LowCardinality(String),
    operation_name       LowCardinality(String),
    http_method          LowCardinality(String),
    http_status_bucket   LowCardinality(String),
    latency_ms_digest    AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count        AggregateFunction(sum, UInt64),
    error_count          AggregateFunction(sum, UInt64),
    duration_ms_sum      AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, http_method, http_status_bucket)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_red_raw_to_1m
TO observability.spans_red_1m AS
SELECT
    team_id,
    toStartOfMinute(toDateTime64(start_ms / 1000.0, 3))                       AS bucket_ts,
    root_service                                                              AS service_name,
    root_operation                                                            AS operation_name,
    root_http_method                                                          AS http_method,
    http_status_bucket,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_ns / 1e6, toUInt64(1)) AS latency_ms_digest,
    sumState(toUInt64(1))                                                     AS request_count,
    sumState(toUInt64(has_error))                                             AS error_count,
    sumState(duration_ns / 1e6)                                               AS duration_ms_sum
FROM observability.traces_index
GROUP BY team_id, bucket_ts, service_name, operation_name, http_method, http_status_bucket;

CREATE TABLE IF NOT EXISTS observability.spans_red_5m (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts            DateTime CODEC(DoubleDelta, LZ4),
    service_name         LowCardinality(String),
    operation_name       LowCardinality(String),
    http_method          LowCardinality(String),
    http_status_bucket   LowCardinality(String),
    latency_ms_digest    AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count        AggregateFunction(sum, UInt64),
    error_count          AggregateFunction(sum, UInt64),
    duration_ms_sum      AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, http_method, http_status_bucket)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_red_1m_to_5m
TO observability.spans_red_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5))                         AS bucket_ts,
    service_name, operation_name, http_method, http_status_bucket,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest)    AS latency_ms_digest,
    sumMergeState(request_count)                                              AS request_count,
    sumMergeState(error_count)                                                AS error_count,
    sumMergeState(duration_ms_sum)                                            AS duration_ms_sum
FROM observability.spans_red_1m
GROUP BY team_id, bucket_ts, service_name, operation_name, http_method, http_status_bucket;

CREATE TABLE IF NOT EXISTS observability.spans_red_1h (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts            DateTime CODEC(DoubleDelta, LZ4),
    service_name         LowCardinality(String),
    operation_name       LowCardinality(String),
    http_method          LowCardinality(String),
    http_status_bucket   LowCardinality(String),
    latency_ms_digest    AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count        AggregateFunction(sum, UInt64),
    error_count          AggregateFunction(sum, UInt64),
    duration_ms_sum      AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, http_method, http_status_bucket)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_red_5m_to_1h
TO observability.spans_red_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts)                                                  AS bucket_ts,
    service_name, operation_name, http_method, http_status_bucket,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest)    AS latency_ms_digest,
    sumMergeState(request_count)                                              AS request_count,
    sumMergeState(error_count)                                                AS error_count,
    sumMergeState(duration_ms_sum)                                            AS duration_ms_sum
FROM observability.spans_red_5m
GROUP BY team_id, bucket_ts, service_name, operation_name, http_method, http_status_bucket;
