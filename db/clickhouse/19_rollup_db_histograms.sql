-- db_histograms_rollup — saturation/database histogram metrics keyed on
-- (db.system, db.operation, db.collection.name, db.namespace, pool.name,
--  error.type, server.address).
-- Powers: saturation/database/* aggregate panels (latency, volume, errors,
-- slowqueries, systems, collections, connections).

CREATE TABLE IF NOT EXISTS observability.db_histograms_rollup_1m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    metric_name     LowCardinality(String),
    service         LowCardinality(String),
    db_system       LowCardinality(String),
    db_operation    LowCardinality(String),
    db_collection   LowCardinality(String),
    db_namespace    LowCardinality(String),
    pool_name       LowCardinality(String),
    error_type      LowCardinality(String),
    server_address  LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_histograms_to_rollup_1m
TO observability.db_histograms_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                             AS bucket_ts,
    metric_name                                                                            AS metric_name,
    service                                                                                AS service,
    attributes.`db.system`::String                                                         AS db_system,
    attributes.`db.operation`::String                                                      AS db_operation,
    attributes.`db.collection.name`::String                                                AS db_collection,
    attributes.`db.namespace`::String                                                      AS db_namespace,
    attributes.`pool.name`::String                                                         AS pool_name,
    attributes.`error.type`::String                                                        AS error_type,
    attributes.`server.address`::String                                                    AS server_address,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        hist_sum / if(hist_count = 0, 1, hist_count),
        hist_count
    )                                                                                      AS latency_ms_digest,
    sumState(hist_count)                                                                   AS hist_count,
    sumState(hist_sum)                                                                     AS hist_sum
FROM observability.metrics
WHERE metric_type = 'Histogram'
  AND hist_count > 0
  AND (metric_name LIKE 'db.%' OR metric_name LIKE 'pool.%');

CREATE TABLE IF NOT EXISTS observability.db_histograms_rollup_5m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    metric_name     LowCardinality(String),
    service         LowCardinality(String),
    db_system       LowCardinality(String),
    db_operation    LowCardinality(String),
    db_collection   LowCardinality(String),
    db_namespace    LowCardinality(String),
    pool_name       LowCardinality(String),
    error_type      LowCardinality(String),
    server_address  LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_histograms_rollup_1m_to_5m
TO observability.db_histograms_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.db_histograms_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address;

CREATE TABLE IF NOT EXISTS observability.db_histograms_rollup_1h (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    metric_name     LowCardinality(String),
    service         LowCardinality(String),
    db_system       LowCardinality(String),
    db_operation    LowCardinality(String),
    db_collection   LowCardinality(String),
    db_namespace    LowCardinality(String),
    pool_name       LowCardinality(String),
    error_type      LowCardinality(String),
    server_address  LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_histograms_rollup_5m_to_1h
TO observability.db_histograms_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.db_histograms_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address;
