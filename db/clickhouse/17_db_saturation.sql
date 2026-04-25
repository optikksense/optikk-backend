-- db_saturation — rollup for all db.* / pool.* metrics (latency digest,
-- connection counts, query volume, errors). db_system is derived in the
-- MV body from pool_name prefix — kept here (not MATERIALIZED on raw)
-- because fan-in is 1 (only this rollup uses it) per the fan-in-2 rule.
-- PK leads with db_system because every reader filters by it first.

CREATE TABLE IF NOT EXISTS observability.db_saturation_1m (
    team_id                   UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts                 DateTime CODEC(DoubleDelta, LZ4),
    db_system                 LowCardinality(String),
    service                   LowCardinality(String),
    metric_name               LowCardinality(String),
    db_operation              LowCardinality(String),
    db_namespace              LowCardinality(String),
    db_collection             LowCardinality(String),
    pool_name                 LowCardinality(String),
    error_type                LowCardinality(String),
    server_address            LowCardinality(String),
    db_connection_state       LowCardinality(String),
    db_response_status_code   LowCardinality(String),
    latency_ms_digest         AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count                AggregateFunction(sum, UInt64),
    hist_sum                  AggregateFunction(sum, Float64),
    value_sum                 AggregateFunction(sum, Float64),
    sample_count              AggregateFunction(sum, UInt64),
    value_last                AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, db_system, service, metric_name, db_operation, db_namespace, db_collection, pool_name, error_type, server_address, db_connection_state, db_response_status_code)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_saturation_raw_to_1m
TO observability.db_saturation_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp) AS bucket_ts,
    multiIf(
        attributes.`db.system`::String != '',        attributes.`db.system`::String,
        attributes.`pool.name`::String LIKE '%hikari%',   'jdbc',
        attributes.`pool.name`::String LIKE '%mongo%',    'mongodb',
        attributes.`pool.name`::String LIKE '%redis%',    'redis',
        attributes.`pool.name`::String LIKE '%pg%',       'postgresql',
        ''
    )                                                AS db_system,
    service,
    metric_name,
    attributes.`db.operation`::String                AS db_operation,
    attributes.`db.namespace`::String                AS db_namespace,
    attributes.`db.collection.name`::String          AS db_collection,
    attributes.`pool.name`::String                   AS pool_name,
    attributes.`error.type`::String                  AS error_type,
    attributes.`server.address`::String              AS server_address,
    attributes.`db.client.connections.state`::String AS db_connection_state,
    attributes.`db.response.status_code`::String     AS db_response_status_code,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        if(hist_count > 0, hist_sum / hist_count, 0.0),
        if(hist_count > 0, hist_count, toUInt64(1))
    )                                                AS latency_ms_digest,
    sumState(hist_count)                             AS hist_count,
    sumState(hist_sum)                               AS hist_sum,
    sumState(value)                                  AS value_sum,
    sumState(toUInt64(1))                            AS sample_count,
    argMaxState(value, timestamp)                    AS value_last
FROM observability.metrics
WHERE metric_name LIKE 'db.%' OR metric_name LIKE 'pool.%'
GROUP BY team_id, bucket_ts, db_system, service, metric_name, db_operation, db_namespace, db_collection, pool_name, error_type, server_address, db_connection_state, db_response_status_code;

CREATE TABLE IF NOT EXISTS observability.db_saturation_5m (
    team_id                   UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts                 DateTime CODEC(DoubleDelta, LZ4),
    db_system                 LowCardinality(String),
    service                   LowCardinality(String),
    metric_name               LowCardinality(String),
    db_operation              LowCardinality(String),
    db_namespace              LowCardinality(String),
    db_collection             LowCardinality(String),
    pool_name                 LowCardinality(String),
    error_type                LowCardinality(String),
    server_address            LowCardinality(String),
    db_connection_state       LowCardinality(String),
    db_response_status_code   LowCardinality(String),
    latency_ms_digest         AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count                AggregateFunction(sum, UInt64),
    hist_sum                  AggregateFunction(sum, Float64),
    value_sum                 AggregateFunction(sum, Float64),
    sample_count              AggregateFunction(sum, UInt64),
    value_last                AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, db_system, service, metric_name, db_operation, db_namespace, db_collection, pool_name, error_type, server_address, db_connection_state, db_response_status_code)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_saturation_1m_to_5m
TO observability.db_saturation_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    db_system, service, metric_name, db_operation, db_namespace, db_collection,
    pool_name, error_type, server_address, db_connection_state, db_response_status_code,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(hist_count)    AS hist_count,
    sumMergeState(hist_sum)      AS hist_sum,
    sumMergeState(value_sum)     AS value_sum,
    sumMergeState(sample_count)  AS sample_count,
    argMaxMergeState(value_last) AS value_last
FROM observability.db_saturation_1m
GROUP BY team_id, bucket_ts, db_system, service, metric_name, db_operation, db_namespace, db_collection, pool_name, error_type, server_address, db_connection_state, db_response_status_code;

CREATE TABLE IF NOT EXISTS observability.db_saturation_1h (
    team_id                   UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts                 DateTime CODEC(DoubleDelta, LZ4),
    db_system                 LowCardinality(String),
    service                   LowCardinality(String),
    metric_name               LowCardinality(String),
    db_operation              LowCardinality(String),
    db_namespace              LowCardinality(String),
    db_collection             LowCardinality(String),
    pool_name                 LowCardinality(String),
    error_type                LowCardinality(String),
    server_address            LowCardinality(String),
    db_connection_state       LowCardinality(String),
    db_response_status_code   LowCardinality(String),
    latency_ms_digest         AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count                AggregateFunction(sum, UInt64),
    hist_sum                  AggregateFunction(sum, Float64),
    value_sum                 AggregateFunction(sum, Float64),
    sample_count              AggregateFunction(sum, UInt64),
    value_last                AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, db_system, service, metric_name, db_operation, db_namespace, db_collection, pool_name, error_type, server_address, db_connection_state, db_response_status_code)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_saturation_5m_to_1h
TO observability.db_saturation_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    db_system, service, metric_name, db_operation, db_namespace, db_collection,
    pool_name, error_type, server_address, db_connection_state, db_response_status_code,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(hist_count)    AS hist_count,
    sumMergeState(hist_sum)      AS hist_sum,
    sumMergeState(value_sum)     AS value_sum,
    sumMergeState(sample_count)  AS sample_count,
    argMaxMergeState(value_last) AS value_last
FROM observability.db_saturation_5m
GROUP BY team_id, bucket_ts, db_system, service, metric_name, db_operation, db_namespace, db_collection, pool_name, error_type, server_address, db_connection_state, db_response_status_code;
