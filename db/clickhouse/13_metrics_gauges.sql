-- metrics_gauges — unified gauge + counter metric rollup.
-- Covers infrastructure/{cpu,disk,memory,network,jvm,connpool} and the subset
-- of overview/apm panels that read non-histogram metrics. state_dim is now a
-- MATERIALIZED column on observability.metrics (see 03_metrics.sql) so the MV
-- body below is pure GROUP BY + state — no multiIf.

CREATE TABLE IF NOT EXISTS observability.metrics_gauges_1m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    metric_name    LowCardinality(String),
    service        LowCardinality(String),
    host           LowCardinality(String),
    pod            LowCardinality(String),
    state_dim      LowCardinality(String),
    value_sum      AggregateFunction(sum, Float64),
    value_avg_num  AggregateFunction(sum, Float64),
    sample_count   AggregateFunction(sum, UInt64),
    value_max      AggregateFunction(max, Float64),
    value_min      AggregateFunction(min, Float64),
    value_last     AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, host, pod, state_dim)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_raw_to_1m
TO observability.metrics_gauges_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                AS bucket_ts,
    metric_name,
    service,
    host,
    attributes.`k8s.pod.name`::String         AS pod,
    state_dim,
    sumState(value)                           AS value_sum,
    sumState(value)                           AS value_avg_num,
    sumState(toUInt64(1))                     AS sample_count,
    maxState(value)                           AS value_max,
    minState(value)                           AS value_min,
    argMaxState(value, timestamp)             AS value_last
FROM observability.metrics
WHERE metric_type IN ('Gauge','Sum') AND hist_count = 0
GROUP BY team_id, bucket_ts, metric_name, service, host, pod, state_dim;

CREATE TABLE IF NOT EXISTS observability.metrics_gauges_5m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    metric_name    LowCardinality(String),
    service        LowCardinality(String),
    host           LowCardinality(String),
    pod            LowCardinality(String),
    state_dim      LowCardinality(String),
    value_sum      AggregateFunction(sum, Float64),
    value_avg_num  AggregateFunction(sum, Float64),
    sample_count   AggregateFunction(sum, UInt64),
    value_max      AggregateFunction(max, Float64),
    value_min      AggregateFunction(min, Float64),
    value_last     AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, host, pod, state_dim)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_1m_to_5m
TO observability.metrics_gauges_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    metric_name, service, host, pod, state_dim,
    sumMergeState(value_sum)       AS value_sum,
    sumMergeState(value_avg_num)   AS value_avg_num,
    sumMergeState(sample_count)    AS sample_count,
    maxMergeState(value_max)       AS value_max,
    minMergeState(value_min)       AS value_min,
    argMaxMergeState(value_last)   AS value_last
FROM observability.metrics_gauges_1m
GROUP BY team_id, bucket_ts, metric_name, service, host, pod, state_dim;

CREATE TABLE IF NOT EXISTS observability.metrics_gauges_1h (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    metric_name    LowCardinality(String),
    service        LowCardinality(String),
    host           LowCardinality(String),
    pod            LowCardinality(String),
    state_dim      LowCardinality(String),
    value_sum      AggregateFunction(sum, Float64),
    value_avg_num  AggregateFunction(sum, Float64),
    sample_count   AggregateFunction(sum, UInt64),
    value_max      AggregateFunction(max, Float64),
    value_min      AggregateFunction(min, Float64),
    value_last     AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, host, pod, state_dim)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_5m_to_1h
TO observability.metrics_gauges_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    metric_name, service, host, pod, state_dim,
    sumMergeState(value_sum)       AS value_sum,
    sumMergeState(value_avg_num)   AS value_avg_num,
    sumMergeState(sample_count)    AS sample_count,
    maxMergeState(value_max)       AS value_max,
    minMergeState(value_min)       AS value_min,
    argMaxMergeState(value_last)   AS value_last
FROM observability.metrics_gauges_5m
GROUP BY team_id, bucket_ts, metric_name, service, host, pod, state_dim;
