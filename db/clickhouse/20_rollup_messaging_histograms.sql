-- messaging_histograms_rollup — messaging/Kafka histogram metrics keyed on
-- (messaging.system, messaging.destination.name, messaging.operation,
--  messaging.kafka.consumer.group).
-- Target: saturation/kafka aggregate panels (produce/consume rate + latency).
-- NOTE: saturation/kafka repository currently stays on raw metrics until its
-- multi-alias attribute coalescing is consolidated to this rollup's dims.

CREATE TABLE IF NOT EXISTS observability.messaging_histograms_rollup_1m (
    team_id               UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts             DateTime CODEC(DoubleDelta, LZ4),
    metric_name           LowCardinality(String),
    service               LowCardinality(String),
    messaging_system      LowCardinality(String),
    messaging_destination LowCardinality(String),
    messaging_operation   LowCardinality(String),
    consumer_group        LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_histograms_to_rollup_1m
TO observability.messaging_histograms_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                             AS bucket_ts,
    metric_name                                                                            AS metric_name,
    service                                                                                AS service,
    messaging_system,
    messaging_destination,
    messaging_operation,
    consumer_group,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        hsum / toFloat64(hcount),
        toUInt64(hcount)
    )                                                                                      AS latency_ms_digest,
    sumState(hcount)                                                                       AS hist_count,
    sumState(hsum)                                                                         AS hist_sum
FROM (
    SELECT team_id, timestamp, metric_name, service,
           attributes.`messaging.system`::String                       AS messaging_system,
           attributes.`messaging.destination.name`::String             AS messaging_destination,
           attributes.`messaging.operation`::String                    AS messaging_operation,
           attributes.`messaging.kafka.consumer.group`::String         AS consumer_group,
           hist_count AS hcount, hist_sum AS hsum
    FROM observability.metrics
    WHERE metric_type = 'Histogram'
      AND hist_count > 0
      AND metric_name LIKE 'messaging.%'
)
GROUP BY team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group;

CREATE TABLE IF NOT EXISTS observability.messaging_histograms_rollup_5m (
    team_id               UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts             DateTime CODEC(DoubleDelta, LZ4),
    metric_name           LowCardinality(String),
    service               LowCardinality(String),
    messaging_system      LowCardinality(String),
    messaging_destination LowCardinality(String),
    messaging_operation   LowCardinality(String),
    consumer_group        LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_histograms_rollup_1m_to_5m
TO observability.messaging_histograms_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.messaging_histograms_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group;

CREATE TABLE IF NOT EXISTS observability.messaging_histograms_rollup_1h (
    team_id               UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts             DateTime CODEC(DoubleDelta, LZ4),
    metric_name           LowCardinality(String),
    service               LowCardinality(String),
    messaging_system      LowCardinality(String),
    messaging_destination LowCardinality(String),
    messaging_operation   LowCardinality(String),
    consumer_group        LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_histograms_rollup_5m_to_1h
TO observability.messaging_histograms_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.messaging_histograms_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group;
