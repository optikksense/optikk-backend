-- messaging — unified rollup for all messaging.* metrics (histograms +
-- counters combined; the old messaging_histograms_rollup was unread in its
-- own DDL comment, so it's collapsed into this family). Optional latency
-- digest columns are populated only by Histogram-type metrics.

CREATE TABLE IF NOT EXISTS observability.messaging_1m (
    team_id                 UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts               DateTime CODEC(DoubleDelta, LZ4),
    messaging_system        LowCardinality(String),
    service                 LowCardinality(String),
    messaging_destination   LowCardinality(String),
    consumer_group          LowCardinality(String),
    messaging_operation     LowCardinality(String),
    broker                  LowCardinality(String),
    partition               LowCardinality(String),
    error_type              LowCardinality(String),
    metric_name             LowCardinality(String),
    latency_ms_digest       AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count              AggregateFunction(sum, UInt64),
    hist_sum                AggregateFunction(sum, Float64),
    value_sum               AggregateFunction(sum, Float64),
    sample_count            AggregateFunction(sum, UInt64),
    value_last              AggregateFunction(argMax, Float64, DateTime64(3)),
    value_max               AggregateFunction(max, Float64),
    value_avg_num           AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, messaging_system, service, messaging_destination, consumer_group, messaging_operation, broker, partition, error_type, metric_name)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_raw_to_1m
TO observability.messaging_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp) AS bucket_ts,
    attributes.`messaging.system`::String                AS messaging_system,
    service,
    attributes.`messaging.destination.name`::String      AS messaging_destination,
    attributes.`messaging.kafka.consumer.group`::String  AS consumer_group,
    attributes.`messaging.operation`::String             AS messaging_operation,
    attributes.`server.address`::String                  AS broker,
    attributes.`messaging.kafka.partition`::String       AS partition,
    attributes.`error.type`::String                      AS error_type,
    metric_name,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        if(hist_count > 0, hist_sum / hist_count, 0.0),
        if(hist_count > 0, hist_count, toUInt64(1))
    ) AS latency_ms_digest,
    sumState(hist_count)          AS hist_count,
    sumState(hist_sum)            AS hist_sum,
    sumState(value)               AS value_sum,
    sumState(toUInt64(1))         AS sample_count,
    argMaxState(value, timestamp) AS value_last,
    maxState(value)               AS value_max,
    sumState(value)               AS value_avg_num
FROM observability.metrics
WHERE metric_name LIKE 'messaging.%'
GROUP BY team_id, bucket_ts, messaging_system, service, messaging_destination, consumer_group, messaging_operation, broker, partition, error_type, metric_name;

CREATE TABLE IF NOT EXISTS observability.messaging_5m (
    team_id                 UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts               DateTime CODEC(DoubleDelta, LZ4),
    messaging_system        LowCardinality(String),
    service                 LowCardinality(String),
    messaging_destination   LowCardinality(String),
    consumer_group          LowCardinality(String),
    messaging_operation     LowCardinality(String),
    broker                  LowCardinality(String),
    partition               LowCardinality(String),
    error_type              LowCardinality(String),
    metric_name             LowCardinality(String),
    latency_ms_digest       AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count              AggregateFunction(sum, UInt64),
    hist_sum                AggregateFunction(sum, Float64),
    value_sum               AggregateFunction(sum, Float64),
    sample_count            AggregateFunction(sum, UInt64),
    value_last              AggregateFunction(argMax, Float64, DateTime64(3)),
    value_max               AggregateFunction(max, Float64),
    value_avg_num           AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, messaging_system, service, messaging_destination, consumer_group, messaging_operation, broker, partition, error_type, metric_name)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_1m_to_5m
TO observability.messaging_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    messaging_system, service, messaging_destination, consumer_group, messaging_operation,
    broker, partition, error_type, metric_name,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(hist_count)    AS hist_count,
    sumMergeState(hist_sum)      AS hist_sum,
    sumMergeState(value_sum)     AS value_sum,
    sumMergeState(sample_count)  AS sample_count,
    argMaxMergeState(value_last) AS value_last,
    maxMergeState(value_max)     AS value_max,
    sumMergeState(value_avg_num) AS value_avg_num
FROM observability.messaging_1m
GROUP BY team_id, bucket_ts, messaging_system, service, messaging_destination, consumer_group, messaging_operation, broker, partition, error_type, metric_name;

CREATE TABLE IF NOT EXISTS observability.messaging_1h (
    team_id                 UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts               DateTime CODEC(DoubleDelta, LZ4),
    messaging_system        LowCardinality(String),
    service                 LowCardinality(String),
    messaging_destination   LowCardinality(String),
    consumer_group          LowCardinality(String),
    messaging_operation     LowCardinality(String),
    broker                  LowCardinality(String),
    partition               LowCardinality(String),
    error_type              LowCardinality(String),
    metric_name             LowCardinality(String),
    latency_ms_digest       AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count              AggregateFunction(sum, UInt64),
    hist_sum                AggregateFunction(sum, Float64),
    value_sum               AggregateFunction(sum, Float64),
    sample_count            AggregateFunction(sum, UInt64),
    value_last              AggregateFunction(argMax, Float64, DateTime64(3)),
    value_max               AggregateFunction(max, Float64),
    value_avg_num           AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, messaging_system, service, messaging_destination, consumer_group, messaging_operation, broker, partition, error_type, metric_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_5m_to_1h
TO observability.messaging_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    messaging_system, service, messaging_destination, consumer_group, messaging_operation,
    broker, partition, error_type, metric_name,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(hist_count)    AS hist_count,
    sumMergeState(hist_sum)      AS hist_sum,
    sumMergeState(value_sum)     AS value_sum,
    sumMergeState(sample_count)  AS sample_count,
    argMaxMergeState(value_last) AS value_last,
    maxMergeState(value_max)     AS value_max,
    sumMergeState(value_avg_num) AS value_avg_num
FROM observability.messaging_5m
GROUP BY team_id, bucket_ts, messaging_system, service, messaging_destination, consumer_group, messaging_operation, broker, partition, error_type, metric_name;
