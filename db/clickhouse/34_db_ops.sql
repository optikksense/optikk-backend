-- db_ops — narrow rollup for /saturation/datastores/system/operations and
-- /system/latency endpoints. Collapses 13-dim db_saturation to 5 dims:
-- (team_id, bucket_ts, db_system, db_operation, error_type). Covers system
-- latency, ops, errors, and top-collections panels.

CREATE TABLE IF NOT EXISTS observability.db_ops_1m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    db_system         LowCardinality(String),
    db_operation      LowCardinality(String),
    error_type        LowCardinality(String),
    db_namespace      LowCardinality(String),
    db_collection     LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, db_system, db_operation, error_type, db_namespace, db_collection)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_ops_raw_to_1m
TO observability.db_ops_1m AS
SELECT
    team_id, bucket_ts, db_system, db_operation, error_type,
    db_namespace, db_collection,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(hist_count) AS hist_count,
    sumMergeState(hist_sum)   AS hist_sum
FROM observability.db_saturation_1m
WHERE metric_name = 'db.client.operation.duration'
GROUP BY team_id, bucket_ts, db_system, db_operation, error_type, db_namespace, db_collection;

-- 5m tier
CREATE TABLE IF NOT EXISTS observability.db_ops_5m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    db_system         LowCardinality(String),
    db_operation      LowCardinality(String),
    error_type        LowCardinality(String),
    db_namespace      LowCardinality(String),
    db_collection     LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, db_system, db_operation, error_type, db_namespace, db_collection)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_ops_1m_to_5m
TO observability.db_ops_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    db_system, db_operation, error_type, db_namespace, db_collection,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(hist_count) AS hist_count,
    sumMergeState(hist_sum)   AS hist_sum
FROM observability.db_ops_1m
GROUP BY team_id, bucket_ts, db_system, db_operation, error_type, db_namespace, db_collection;

-- 1h tier
CREATE TABLE IF NOT EXISTS observability.db_ops_1h (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    db_system         LowCardinality(String),
    db_operation      LowCardinality(String),
    error_type        LowCardinality(String),
    db_namespace      LowCardinality(String),
    db_collection     LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, db_system, db_operation, error_type, db_namespace, db_collection)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_ops_5m_to_1h
TO observability.db_ops_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    db_system, db_operation, error_type, db_namespace, db_collection,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(hist_count) AS hist_count,
    sumMergeState(hist_sum)   AS hist_sum
FROM observability.db_ops_5m
GROUP BY team_id, bucket_ts, db_system, db_operation, error_type, db_namespace, db_collection;
