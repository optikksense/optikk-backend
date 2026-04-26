-- kafka_summary — narrow rollup for /saturation/kafka/summary-stats endpoint.
-- Collapses the 11-dim messaging rollup to (team_id, bucket_ts, metric_name).
-- Enables single-query summary computation via sumMergeIf / quantilesTDigestWeightedMergeIf.

CREATE TABLE IF NOT EXISTS observability.kafka_summary_1m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    metric_name    LowCardinality(String),
    value_sum      AggregateFunction(sum, Float64),
    value_max      AggregateFunction(max, Float64),
    sample_count   AggregateFunction(sum, UInt64),
    latency_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.kafka_summary_raw_to_1m
TO observability.kafka_summary_1m AS
SELECT
    team_id, bucket_ts, metric_name,
    sumMergeState(value_sum)     AS value_sum,
    maxMergeState(value_max)     AS value_max,
    sumMergeState(sample_count)  AS sample_count,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_digest
FROM observability.messaging_1m
WHERE metric_name IN (
    'messaging.publish.duration',
    'messaging.receive.duration',
    'messaging.process.duration',
    'messaging.kafka.consumer.lag',
    'messaging.publish.messages',
    'messaging.receive.messages'
)
GROUP BY team_id, bucket_ts, metric_name;

-- 5m tier
CREATE TABLE IF NOT EXISTS observability.kafka_summary_5m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    metric_name    LowCardinality(String),
    value_sum      AggregateFunction(sum, Float64),
    value_max      AggregateFunction(max, Float64),
    sample_count   AggregateFunction(sum, UInt64),
    latency_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.kafka_summary_1m_to_5m
TO observability.kafka_summary_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    metric_name,
    sumMergeState(value_sum)     AS value_sum,
    maxMergeState(value_max)     AS value_max,
    sumMergeState(sample_count)  AS sample_count,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_digest) AS latency_digest
FROM observability.kafka_summary_1m
GROUP BY team_id, bucket_ts, metric_name;

-- 1h tier
CREATE TABLE IF NOT EXISTS observability.kafka_summary_1h (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    metric_name    LowCardinality(String),
    value_sum      AggregateFunction(sum, Float64),
    value_max      AggregateFunction(max, Float64),
    sample_count   AggregateFunction(sum, UInt64),
    latency_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.kafka_summary_5m_to_1h
TO observability.kafka_summary_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    metric_name,
    sumMergeState(value_sum)     AS value_sum,
    maxMergeState(value_max)     AS value_max,
    sumMergeState(sample_count)  AS sample_count,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_digest) AS latency_digest
FROM observability.kafka_summary_5m
GROUP BY team_id, bucket_ts, metric_name;
