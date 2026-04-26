-- spans_latency -- all-span latency rollup for dashboard histogram + heatmap
-- reads. Source: raw spans (not root-only), keyed by (service, operation,
-- latency_bucket) so the same family can answer both:
--   - histogram/summary queries via digest/count/sum/max merged across buckets
--   - heatmap queries via latency_bucket + span_count grouped by time bucket

CREATE TABLE IF NOT EXISTS observability.spans_latency_1m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    service_name      LowCardinality(String),
    operation_name    LowCardinality(String),
    latency_bucket    Float64 CODEC(Gorilla, ZSTD(1)),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.9, 0.95, 0.99), Float64, UInt64),
    span_count        AggregateFunction(sum, UInt64),
    duration_ms_sum   AggregateFunction(sum, Float64),
    max_latency_ms    AggregateFunction(max, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, latency_bucket)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_latency_raw_to_1m
TO observability.spans_latency_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                          AS bucket_ts,
    service_name,
    name                                                                AS operation_name,
    round(log10(greatest(duration_nano, toUInt64(1))) * 10) / 10        AS latency_bucket,
    quantilesTDigestWeightedState(0.5, 0.9, 0.95, 0.99)(duration_nano / 1e6, toUInt64(1)) AS latency_ms_digest,
    sumState(toUInt64(1))                                               AS span_count,
    sumState(duration_nano / 1e6)                                       AS duration_ms_sum,
    maxState(duration_nano / 1e6)                                       AS max_latency_ms
FROM observability.spans
GROUP BY team_id, bucket_ts, service_name, operation_name, latency_bucket;

CREATE TABLE IF NOT EXISTS observability.spans_latency_5m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    service_name      LowCardinality(String),
    operation_name    LowCardinality(String),
    latency_bucket    Float64 CODEC(Gorilla, ZSTD(1)),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.9, 0.95, 0.99), Float64, UInt64),
    span_count        AggregateFunction(sum, UInt64),
    duration_ms_sum   AggregateFunction(sum, Float64),
    max_latency_ms    AggregateFunction(max, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, latency_bucket)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_latency_1m_to_5m
TO observability.spans_latency_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5))                   AS bucket_ts,
    service_name,
    operation_name,
    latency_bucket,
    quantilesTDigestWeightedMergeState(0.5, 0.9, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(span_count)                                           AS span_count,
    sumMergeState(duration_ms_sum)                                      AS duration_ms_sum,
    maxMergeState(max_latency_ms)                                       AS max_latency_ms
FROM observability.spans_latency_1m
GROUP BY team_id, bucket_ts, service_name, operation_name, latency_bucket;

CREATE TABLE IF NOT EXISTS observability.spans_latency_1h (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    service_name      LowCardinality(String),
    operation_name    LowCardinality(String),
    latency_bucket    Float64 CODEC(Gorilla, ZSTD(1)),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.9, 0.95, 0.99), Float64, UInt64),
    span_count        AggregateFunction(sum, UInt64),
    duration_ms_sum   AggregateFunction(sum, Float64),
    max_latency_ms    AggregateFunction(max, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, latency_bucket)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_latency_5m_to_1h
TO observability.spans_latency_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts)                                            AS bucket_ts,
    service_name,
    operation_name,
    latency_bucket,
    quantilesTDigestWeightedMergeState(0.5, 0.9, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(span_count)                                           AS span_count,
    sumMergeState(duration_ms_sum)                                      AS duration_ms_sum,
    maxMergeState(max_latency_ms)                                       AS max_latency_ms
FROM observability.spans_latency_5m
GROUP BY team_id, bucket_ts, service_name, operation_name, latency_bucket;
