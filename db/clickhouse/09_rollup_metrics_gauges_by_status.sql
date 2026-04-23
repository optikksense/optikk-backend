-- metrics_gauges_by_status_rollup — HTTP request count per status code.
-- Powers: overview/httpmetrics GetRequestRate. status_code dim is HTTP-specific,
-- too narrow for the general metrics_gauges_rollup.

CREATE TABLE IF NOT EXISTS observability.metrics_gauges_by_status_rollup_1m (
    team_id          UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime CODEC(DoubleDelta, LZ4),
    metric_name      LowCardinality(String),
    service          LowCardinality(String),
    http_status_code LowCardinality(String),
    sample_count     AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, http_status_code)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_by_status_to_rollup_1m
TO observability.metrics_gauges_by_status_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                    AS bucket_ts,
    metric_name                                                   AS metric_name,
    service                                                       AS service,
    attributes.`http.status_code`::String                         AS http_status_code,
    sumState(toUInt64(1))                                         AS sample_count
FROM observability.metrics
WHERE metric_name IN ('http.server.request.duration','http.server.request.size','http.client.request.duration')
GROUP BY team_id, bucket_ts, metric_name, service, http_status_code;

CREATE TABLE IF NOT EXISTS observability.metrics_gauges_by_status_rollup_5m (
    team_id          UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime CODEC(DoubleDelta, LZ4),
    metric_name      LowCardinality(String),
    service          LowCardinality(String),
    http_status_code LowCardinality(String),
    sample_count     AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, http_status_code)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_by_status_rollup_1m_to_5m
TO observability.metrics_gauges_by_status_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service, http_status_code,
       sumMergeState(sample_count) AS sample_count
FROM observability.metrics_gauges_by_status_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service, http_status_code;

CREATE TABLE IF NOT EXISTS observability.metrics_gauges_by_status_rollup_1h (
    team_id          UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime CODEC(DoubleDelta, LZ4),
    metric_name      LowCardinality(String),
    service          LowCardinality(String),
    http_status_code LowCardinality(String),
    sample_count     AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, http_status_code)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_by_status_rollup_5m_to_1h
TO observability.metrics_gauges_by_status_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service, http_status_code,
       sumMergeState(sample_count) AS sample_count
FROM observability.metrics_gauges_by_status_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service, http_status_code;
