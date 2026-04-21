-- db_histograms_rollup — unified saturation/database rollup (histogram + gauge rows).
-- Keys: db_system, db_operation, db_collection, db_namespace, pool_name, error_type,
-- server_address, db_connection_state, db_response_status_code.
-- Histogram metrics (db.*, pool.* Histogram) populate latency digest + hist_count/hist_sum;
-- Gauge/Sum rows (connection counts, etc.) populate value_sum / sample_count / value_last.

CREATE TABLE IF NOT EXISTS observability.db_histograms_rollup_1m (
    team_id               UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts             DateTime CODEC(DoubleDelta, LZ4),
    metric_name           LowCardinality(String),
    service               LowCardinality(String),
    db_system             LowCardinality(String),
    db_operation          LowCardinality(String),
    db_collection         LowCardinality(String),
    db_namespace          LowCardinality(String),
    pool_name             LowCardinality(String),
    error_type            LowCardinality(String),
    server_address        LowCardinality(String),
    db_connection_state   LowCardinality(String),
    db_response_status_code LowCardinality(String),

    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64),
    value_sum         AggregateFunction(sum, Float64),
    sample_count      AggregateFunction(sum, UInt64),
    value_last        AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address, db_connection_state, db_response_status_code)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_histograms_to_rollup_1m
TO observability.db_histograms_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(src_timestamp)                                                         AS bucket_ts,
    metric_name                                                                            AS metric_name,
    service                                                                                AS service,
    db_system,
    db_operation,
    db_collection,
    db_namespace,
    pool_name,
    error_type,
    server_address,
    db_connection_state,
    db_response_status_code,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        if(toUInt64(hcount) > 0, hsum / toFloat64(hcount), 0.0),
        if(toUInt64(hcount) > 0, toUInt64(hcount), toUInt64(1))
    )                                                                                      AS latency_ms_digest,
    sumState(hcount)                                                                       AS hist_count,
    sumState(hsum)                                                                         AS hist_sum,
    sumState(src_value)                                                                    AS value_sum,
    sumState(toUInt64(1))                                                                  AS sample_count,
    argMaxState(src_value, src_timestamp)                                                  AS value_last
FROM (
    SELECT team_id, timestamp AS src_timestamp, metric_name, service,
           -- Extract db.system from pool.name since the attribute doesn't exist in telemetry
           CASE 
               WHEN JSONExtractString(attributes, 'db.client.connection.pool.name') LIKE '%postgresql%' OR 
                    JSONExtractString(attributes, 'db.client.connection.pool.name') LIKE '%postgres%' OR
                    JSONExtractString(attributes, 'db.client.connection.pool.name') LIKE '%-db%' OR
                    JSONExtractString(attributes, 'db.client.connection.pool.name') LIKE '%Host=%' THEN 'postgresql'
               WHEN JSONExtractString(attributes, 'db.client.connection.pool.name') LIKE '%mysql%' THEN 'mysql'
               WHEN JSONExtractString(attributes, 'db.client.connection.pool.name') LIKE '%mongodb%' THEN 'mongodb'
               WHEN JSONExtractString(attributes, 'db.client.connection.pool.name') LIKE '%redis%' THEN 'redis'
               WHEN JSONExtractString(attributes, 'db.client.connection.pool.name') LIKE '%elasticsearch%' THEN 'elasticsearch'
               ELSE 'unknown'
           END AS db_system,
           attributes.`db.operation`::String                 AS db_operation,
           attributes.`db.collection.name`::String           AS db_collection,
           attributes.`db.namespace`::String                 AS db_namespace,
           JSONExtractString(attributes, 'db.client.connection.pool.name') AS pool_name,
           attributes.`error.type`::String                   AS error_type,
           attributes.`server.address`::String               AS server_address,
           attributes.`db.client.connections.state`::String  AS db_connection_state,
           attributes.`db.response.status_code`::String      AS db_response_status_code,
           hist_count AS hcount, hist_sum AS hsum, value AS src_value
    FROM observability.metrics
    WHERE (metric_name LIKE 'db.%' OR metric_name LIKE 'pool.%')
      AND (metric_type = 'Histogram' OR metric_type IN ('Gauge','Sum'))
)
GROUP BY team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address, db_connection_state, db_response_status_code;

CREATE TABLE IF NOT EXISTS observability.db_histograms_rollup_5m (
    team_id               UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts             DateTime CODEC(DoubleDelta, LZ4),
    metric_name           LowCardinality(String),
    service               LowCardinality(String),
    db_system             LowCardinality(String),
    db_operation          LowCardinality(String),
    db_collection         LowCardinality(String),
    db_namespace          LowCardinality(String),
    pool_name             LowCardinality(String),
    error_type            LowCardinality(String),
    server_address        LowCardinality(String),
    db_connection_state   LowCardinality(String),
    db_response_status_code LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64),
    value_sum         AggregateFunction(sum, Float64),
    sample_count      AggregateFunction(sum, UInt64),
    value_last        AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address, db_connection_state, db_response_status_code)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_histograms_rollup_1m_to_5m
TO observability.db_histograms_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address, db_connection_state, db_response_status_code,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum,
       sumMergeState(value_sum)  AS value_sum,
       sumMergeState(sample_count) AS sample_count,
       argMaxMergeState(value_last) AS value_last
FROM observability.db_histograms_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address, db_connection_state, db_response_status_code;

CREATE TABLE IF NOT EXISTS observability.db_histograms_rollup_1h (
    team_id               UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts             DateTime CODEC(DoubleDelta, LZ4),
    metric_name           LowCardinality(String),
    service               LowCardinality(String),
    db_system             LowCardinality(String),
    db_operation          LowCardinality(String),
    db_collection         LowCardinality(String),
    db_namespace          LowCardinality(String),
    pool_name             LowCardinality(String),
    error_type            LowCardinality(String),
    server_address        LowCardinality(String),
    db_connection_state   LowCardinality(String),
    db_response_status_code LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64),
    value_sum         AggregateFunction(sum, Float64),
    sample_count      AggregateFunction(sum, UInt64),
    value_last        AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address, db_connection_state, db_response_status_code)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_histograms_rollup_5m_to_1h
TO observability.db_histograms_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address, db_connection_state, db_response_status_code,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum,
       sumMergeState(value_sum)  AS value_sum,
       sumMergeState(sample_count) AS sample_count,
       argMaxMergeState(value_last) AS value_last
FROM observability.db_histograms_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address, db_connection_state, db_response_status_code;
