-- db_collections — narrow MV for the "Slowest Collections" dashboard panel
-- Drops high-cardinality db_operation, server_address, and pool_name from db_saturation

CREATE TABLE IF NOT EXISTS observability.db_collections_1m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    db_system         LowCardinality(String),
    metric_name       LowCardinality(String),
    db_collection     LowCardinality(String),
    error_type        LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, db_system, metric_name, db_collection, error_type)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_sat_to_collections_1m
TO observability.db_collections_1m AS
SELECT
    team_id,
    bucket_ts,
    db_system,
    metric_name,
    db_collection,
    error_type,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(hist_count)                                              AS hist_count
FROM observability.db_saturation_1m
WHERE db_collection != ''
GROUP BY team_id, bucket_ts, db_system, metric_name, db_collection, error_type;

CREATE TABLE IF NOT EXISTS observability.db_collections_5m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    db_system         LowCardinality(String),
    metric_name       LowCardinality(String),
    db_collection     LowCardinality(String),
    error_type        LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, db_system, metric_name, db_collection, error_type)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_collections_1m_to_5m
TO observability.db_collections_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    db_system,
    metric_name,
    db_collection,
    error_type,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(hist_count)                                              AS hist_count
FROM observability.db_collections_1m
GROUP BY team_id, bucket_ts, db_system, metric_name, db_collection, error_type;

CREATE TABLE IF NOT EXISTS observability.db_collections_1h (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    db_system         LowCardinality(String),
    metric_name       LowCardinality(String),
    db_collection     LowCardinality(String),
    error_type        LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, db_system, metric_name, db_collection, error_type)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_collections_5m_to_1h
TO observability.db_collections_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    db_system,
    metric_name,
    db_collection,
    error_type,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(hist_count)                                              AS hist_count
FROM observability.db_collections_5m
GROUP BY team_id, bucket_ts, db_system, metric_name, db_collection, error_type;
