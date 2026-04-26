-- spans_node — aggregates spans_host up to just the host_name level for fast summary counts
-- Drops service_name and pod_name dimensions.

CREATE TABLE IF NOT EXISTS observability.spans_node_1m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    host_name         LowCardinality(String),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64),
    pod_count         AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, host_name)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_host_to_node_1m
TO observability.spans_node_1m AS
SELECT
    team_id,
    bucket_ts,
    host_name,
    sumMergeState(request_count)                   AS request_count,
    sumMergeState(error_count)                     AS error_count,
    uniqIfState(pod_name, pod_name != '')          AS pod_count
FROM observability.spans_host_1m
WHERE host_name != ''
GROUP BY team_id, bucket_ts, host_name;

CREATE TABLE IF NOT EXISTS observability.spans_node_5m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    host_name         LowCardinality(String),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64),
    pod_count         AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, host_name)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_node_1m_to_5m
TO observability.spans_node_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    host_name,
    sumMergeState(request_count)                      AS request_count,
    sumMergeState(error_count)                        AS error_count,
    uniqMergeState(pod_count)                         AS pod_count
FROM observability.spans_node_1m
GROUP BY team_id, bucket_ts, host_name;

CREATE TABLE IF NOT EXISTS observability.spans_node_1h (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    host_name         LowCardinality(String),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64),
    pod_count         AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, host_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_node_5m_to_1h
TO observability.spans_node_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    host_name,
    sumMergeState(request_count)                      AS request_count,
    sumMergeState(error_count)                        AS error_count,
    uniqMergeState(pod_count)                         AS pod_count
FROM observability.spans_node_5m
GROUP BY team_id, bucket_ts, host_name;
