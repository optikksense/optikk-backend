-- db_summary — narrow rollup for /saturation/datastores/summary endpoint.
-- Pre-aggregates the 4 sub-queries (latency, errors, connections, cache)
-- into a single narrow table keyed on (team_id, bucket_ts, metric_name,
-- error_type, db_connection_state, db_system). Summary handler reads one
-- table with conditional aggregation instead of 4 wide-table queries.

CREATE TABLE IF NOT EXISTS observability.db_summary_1m (
    team_id             UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts           DateTime CODEC(DoubleDelta, LZ4),
    metric_name         LowCardinality(String),
    error_type          LowCardinality(String),
    db_connection_state LowCardinality(String),
    db_system           LowCardinality(String),
    latency_digest      AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count          AggregateFunction(sum, UInt64),
    value_sum           AggregateFunction(sum, Float64),
    sample_count        AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, error_type, db_connection_state, db_system)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_summary_raw_to_1m
TO observability.db_summary_1m AS
SELECT
    team_id, bucket_ts, metric_name, error_type, db_connection_state, db_system,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_digest,
    sumMergeState(hist_count)   AS hist_count,
    sumMergeState(value_sum)    AS value_sum,
    sumMergeState(sample_count) AS sample_count
FROM observability.db_saturation_1m
WHERE metric_name IN ('db.client.operation.duration', 'db.client.connections.count')
GROUP BY team_id, bucket_ts, metric_name, error_type, db_connection_state, db_system;

-- 5m tier
CREATE TABLE IF NOT EXISTS observability.db_summary_5m (
    team_id             UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts           DateTime CODEC(DoubleDelta, LZ4),
    metric_name         LowCardinality(String),
    error_type          LowCardinality(String),
    db_connection_state LowCardinality(String),
    db_system           LowCardinality(String),
    latency_digest      AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count          AggregateFunction(sum, UInt64),
    value_sum           AggregateFunction(sum, Float64),
    sample_count        AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, error_type, db_connection_state, db_system)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_summary_1m_to_5m
TO observability.db_summary_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    metric_name, error_type, db_connection_state, db_system,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_digest) AS latency_digest,
    sumMergeState(hist_count)   AS hist_count,
    sumMergeState(value_sum)    AS value_sum,
    sumMergeState(sample_count) AS sample_count
FROM observability.db_summary_1m
GROUP BY team_id, bucket_ts, metric_name, error_type, db_connection_state, db_system;

-- 1h tier
CREATE TABLE IF NOT EXISTS observability.db_summary_1h (
    team_id             UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts           DateTime CODEC(DoubleDelta, LZ4),
    metric_name         LowCardinality(String),
    error_type          LowCardinality(String),
    db_connection_state LowCardinality(String),
    db_system           LowCardinality(String),
    latency_digest      AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count          AggregateFunction(sum, UInt64),
    value_sum           AggregateFunction(sum, Float64),
    sample_count        AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, error_type, db_connection_state, db_system)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_summary_5m_to_1h
TO observability.db_summary_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    metric_name, error_type, db_connection_state, db_system,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_digest) AS latency_digest,
    sumMergeState(hist_count)   AS hist_count,
    sumMergeState(value_sum)    AS value_sum,
    sumMergeState(sample_count) AS sample_count
FROM observability.db_summary_5m
GROUP BY team_id, bucket_ts, metric_name, error_type, db_connection_state, db_system;
