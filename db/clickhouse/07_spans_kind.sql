-- spans_kind — latency + rate by span kind (client/server/producer/consumer/internal).
-- Source: raw observability.spans (kind_string is low-cardinality, ~5 values per service).
-- Purpose: quickly answer "what's the p95 for CLIENT spans in service X" without
-- scanning raw.

CREATE TABLE IF NOT EXISTS observability.spans_kind_1m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    service_name      LowCardinality(String),
    kind_string       LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    duration_ms_sum   AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, kind_string)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_kind_raw_to_1m
TO observability.spans_kind_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp) AS bucket_ts,
    service_name,
    kind_string,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_nano / 1e6, toUInt64(1)) AS latency_ms_digest,
    sumState(toUInt64(1))                                                            AS request_count,
    sumState(duration_nano / 1e6)                                                    AS duration_ms_sum
FROM observability.spans
GROUP BY team_id, bucket_ts, service_name, kind_string;

CREATE TABLE IF NOT EXISTS observability.spans_kind_5m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    service_name      LowCardinality(String),
    kind_string       LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    duration_ms_sum   AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, kind_string)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_kind_1m_to_5m
TO observability.spans_kind_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    service_name, kind_string,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(request_count)                                           AS request_count,
    sumMergeState(duration_ms_sum)                                         AS duration_ms_sum
FROM observability.spans_kind_1m
GROUP BY team_id, bucket_ts, service_name, kind_string;

CREATE TABLE IF NOT EXISTS observability.spans_kind_1h (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    service_name      LowCardinality(String),
    kind_string       LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    duration_ms_sum   AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, kind_string)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_kind_5m_to_1h
TO observability.spans_kind_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    service_name, kind_string,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(request_count)                                           AS request_count,
    sumMergeState(duration_ms_sum)                                         AS duration_ms_sum
FROM observability.spans_kind_5m
GROUP BY team_id, bucket_ts, service_name, kind_string;
