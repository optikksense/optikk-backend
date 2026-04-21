-- messaging_counters_rollup — messaging counter + gauge metrics
-- (metric_type IN ('Gauge','Sum')) excluded from the histogram-only
-- `messaging_histograms_rollup`.
-- Keys include broker + partition + error_type so rate / error / lag /
-- per-partition panels can drop to the rollup.
-- Powers: saturation/kafka rate + error + lag + broker-connection + rebalance
-- + per-partition panels.

CREATE TABLE IF NOT EXISTS observability.messaging_counters_rollup_1m (
    team_id                UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts              DateTime CODEC(DoubleDelta, LZ4),
    metric_name            LowCardinality(String),
    service                LowCardinality(String),
    messaging_system       LowCardinality(String),
    messaging_destination  LowCardinality(String),
    messaging_operation    LowCardinality(String),
    consumer_group         LowCardinality(String),
    broker                 LowCardinality(String),
    partition              LowCardinality(String),
    error_type             LowCardinality(String),

    value_sum    AggregateFunction(sum, Float64),
    sample_count AggregateFunction(sum, UInt64),
    value_last   AggregateFunction(argMax, Float64, DateTime64(3)),
    value_max    AggregateFunction(max, Float64),
    value_avg_num AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group, broker, partition, error_type)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_counters_to_rollup_1m
TO observability.messaging_counters_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                             AS bucket_ts,
    metric_name                                                                            AS metric_name,
    service                                                                                AS service,
    attributes.`messaging.system`::String                                                  AS messaging_system,
    attributes.`messaging.destination.name`::String                                        AS messaging_destination,
    attributes.`messaging.operation`::String                                               AS messaging_operation,
    attributes.`messaging.kafka.consumer.group`::String                                    AS consumer_group,
    attributes.`server.address`::String                                                    AS broker,
    attributes.`messaging.kafka.destination.partition`::String                             AS partition,
    attributes.`error.type`::String                                                        AS error_type,
    sumState(value)                                                                        AS value_sum,
    sumState(toUInt64(1))                                                                  AS sample_count,
    argMaxState(value, timestamp)                                                          AS value_last,
    maxState(value)                                                                        AS value_max,
    sumState(value)                                                                        AS value_avg_num
FROM observability.metrics
WHERE metric_type IN ('Gauge','Sum')
  AND hist_count = 0
  AND metric_name LIKE 'messaging.%';

CREATE TABLE IF NOT EXISTS observability.messaging_counters_rollup_5m (
    team_id                UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts              DateTime CODEC(DoubleDelta, LZ4),
    metric_name            LowCardinality(String),
    service                LowCardinality(String),
    messaging_system       LowCardinality(String),
    messaging_destination  LowCardinality(String),
    messaging_operation    LowCardinality(String),
    consumer_group         LowCardinality(String),
    broker                 LowCardinality(String),
    partition              LowCardinality(String),
    error_type             LowCardinality(String),
    value_sum    AggregateFunction(sum, Float64),
    sample_count AggregateFunction(sum, UInt64),
    value_last   AggregateFunction(argMax, Float64, DateTime64(3)),
    value_max    AggregateFunction(max, Float64),
    value_avg_num AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group, broker, partition, error_type)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_counters_rollup_1m_to_5m
TO observability.messaging_counters_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group, broker, partition, error_type,
       sumMergeState(value_sum)    AS value_sum,
       sumMergeState(sample_count) AS sample_count,
       argMaxMergeState(value_last) AS value_last,
       maxMergeState(value_max)    AS value_max,
       sumMergeState(value_avg_num) AS value_avg_num
FROM observability.messaging_counters_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group, broker, partition, error_type;

CREATE TABLE IF NOT EXISTS observability.messaging_counters_rollup_1h (
    team_id                UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts              DateTime CODEC(DoubleDelta, LZ4),
    metric_name            LowCardinality(String),
    service                LowCardinality(String),
    messaging_system       LowCardinality(String),
    messaging_destination  LowCardinality(String),
    messaging_operation    LowCardinality(String),
    consumer_group         LowCardinality(String),
    broker                 LowCardinality(String),
    partition              LowCardinality(String),
    error_type             LowCardinality(String),
    value_sum    AggregateFunction(sum, Float64),
    sample_count AggregateFunction(sum, UInt64),
    value_last   AggregateFunction(argMax, Float64, DateTime64(3)),
    value_max    AggregateFunction(max, Float64),
    value_avg_num AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group, broker, partition, error_type)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_counters_rollup_5m_to_1h
TO observability.messaging_counters_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group, broker, partition, error_type,
       sumMergeState(value_sum)    AS value_sum,
       sumMergeState(sample_count) AS sample_count,
       argMaxMergeState(value_last) AS value_last,
       maxMergeState(value_max)    AS value_max,
       sumMergeState(value_avg_num) AS value_avg_num
FROM observability.messaging_counters_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group, broker, partition, error_type;
