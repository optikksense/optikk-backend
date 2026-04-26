DROP VIEW IF EXISTS observability.db_conn_pool_5m_to_1h;
DROP TABLE IF EXISTS observability.db_conn_pool_1h;

DROP VIEW IF EXISTS observability.db_conn_pool_1m_to_5m;
DROP TABLE IF EXISTS observability.db_conn_pool_5m;

DROP VIEW IF EXISTS observability.db_conn_pool_raw_to_1m;
DROP TABLE IF EXISTS observability.db_conn_pool_1m;

-- 1m tier
CREATE TABLE IF NOT EXISTS observability.db_conn_pool_1m (
    team_id             UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts           DateTime CODEC(DoubleDelta, LZ4),
    pool_name           LowCardinality(String),
    db_system           LowCardinality(String),
    metric_name         LowCardinality(String),
    db_connection_state LowCardinality(String),
    value_sum           AggregateFunction(sum, Float64),
    sample_count        AggregateFunction(sum, UInt64),
    latency_ms_digest   AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, pool_name, db_system, metric_name, db_connection_state)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_conn_pool_raw_to_1m
TO observability.db_conn_pool_1m AS
SELECT
    team_id, bucket_ts, pool_name, db_system, metric_name, db_connection_state,
    sumMergeState(value_sum)     AS value_sum,
    sumMergeState(sample_count)  AS sample_count,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest
FROM observability.db_saturation_1m
WHERE metric_name IN (
    'db.client.connections.usage',
    'db.client.connections.idle.min',
    'db.client.connections.idle.max',
    'db.client.connections.max',
    'db.client.connections.pending_requests',
    'db.client.connections.timeouts',
    'db.client.connections.create_time',
    'db.client.connections.use_time'
)
GROUP BY team_id, bucket_ts, pool_name, db_system, metric_name, db_connection_state;

-- 5m tier
CREATE TABLE IF NOT EXISTS observability.db_conn_pool_5m (
    team_id             UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts           DateTime CODEC(DoubleDelta, LZ4),
    pool_name           LowCardinality(String),
    db_system           LowCardinality(String),
    metric_name         LowCardinality(String),
    db_connection_state LowCardinality(String),
    value_sum           AggregateFunction(sum, Float64),
    sample_count        AggregateFunction(sum, UInt64),
    latency_ms_digest   AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, pool_name, db_system, metric_name, db_connection_state)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_conn_pool_1m_to_5m
TO observability.db_conn_pool_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    pool_name, db_system, metric_name, db_connection_state,
    sumMergeState(value_sum)     AS value_sum,
    sumMergeState(sample_count)  AS sample_count,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest
FROM observability.db_conn_pool_1m
GROUP BY team_id, bucket_ts, pool_name, db_system, metric_name, db_connection_state;

-- 1h tier
CREATE TABLE IF NOT EXISTS observability.db_conn_pool_1h (
    team_id             UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts           DateTime CODEC(DoubleDelta, LZ4),
    pool_name           LowCardinality(String),
    db_system           LowCardinality(String),
    metric_name         LowCardinality(String),
    db_connection_state LowCardinality(String),
    value_sum           AggregateFunction(sum, Float64),
    sample_count        AggregateFunction(sum, UInt64),
    latency_ms_digest   AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, pool_name, db_system, metric_name, db_connection_state)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_conn_pool_5m_to_1h
TO observability.db_conn_pool_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    pool_name, db_system, metric_name, db_connection_state,
    sumMergeState(value_sum)     AS value_sum,
    sumMergeState(sample_count)  AS sample_count,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest
FROM observability.db_conn_pool_5m
GROUP BY team_id, bucket_ts, pool_name, db_system, metric_name, db_connection_state;
