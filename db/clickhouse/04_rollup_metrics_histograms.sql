-- metrics_histograms_rollup — generic histogram-metric latency per (metric, service).
-- Powers: overview/apm and anywhere a metric_type='Histogram' row needs p50/p95/p99.

CREATE TABLE IF NOT EXISTS observability.metrics_histograms_rollup_1m (
    team_id      UInt32           CODEC(T64, ZSTD(1)),
    bucket_ts    DateTime         CODEC(DoubleDelta, LZ4),
    metric_name  LowCardinality(String),
    service      LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count   AggregateFunction(sum, UInt64),
    hist_sum     AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_histograms_to_rollup_1m
TO observability.metrics_histograms_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                          AS bucket_ts,
    metric_name                                                                         AS metric_name,
    service                                                                             AS service,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        hsum / toFloat64(hcount),
        toUInt64(hcount)
    )                                                                                   AS latency_ms_digest,
    sumState(hcount)                                                                    AS hist_count,
    sumState(hsum)                                                                      AS hist_sum
FROM (
    SELECT team_id, timestamp, metric_name, service,
           hist_count AS hcount, hist_sum AS hsum
    FROM observability.metrics
    WHERE metric_type = 'Histogram' AND hist_count > 0
)
GROUP BY team_id, bucket_ts, metric_name, service;

CREATE TABLE IF NOT EXISTS observability.metrics_histograms_rollup_5m (
    team_id      UInt32           CODEC(T64, ZSTD(1)),
    bucket_ts    DateTime         CODEC(DoubleDelta, LZ4),
    metric_name  LowCardinality(String),
    service      LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count   AggregateFunction(sum, UInt64),
    hist_sum     AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_histograms_rollup_1m_to_5m
TO observability.metrics_histograms_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.metrics_histograms_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service;

CREATE TABLE IF NOT EXISTS observability.metrics_histograms_rollup_1h (
    team_id      UInt32           CODEC(T64, ZSTD(1)),
    bucket_ts    DateTime         CODEC(DoubleDelta, LZ4),
    metric_name  LowCardinality(String),
    service      LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count   AggregateFunction(sum, UInt64),
    hist_sum     AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_histograms_rollup_5m_to_1h
TO observability.metrics_histograms_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.metrics_histograms_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service;
