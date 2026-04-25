-- logs_volume — log-count + error-count rollup by severity and resource.
-- Pod is consistent across all three tiers (the pre-rewrite schema dropped
-- it at 5m/1h, breaking drill-down). severity_bucket is a MATERIALIZED
-- column on observability.logs so the MV body is pure GROUP BY + state.

CREATE TABLE IF NOT EXISTS observability.logs_volume_1m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    service         LowCardinality(String),
    environment     LowCardinality(String),
    severity_bucket UInt8,
    host            LowCardinality(String),
    pod             LowCardinality(String),
    log_count       AggregateFunction(sum, UInt64),
    error_count     AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service, environment, severity_bucket, host, pod)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_volume_raw_to_1m
TO observability.logs_volume_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp) AS bucket_ts,
    service,
    environment,
    severity_bucket,
    host,
    pod,
    sumState(toUInt64(1))                        AS log_count,
    sumState(toUInt64(severity_bucket >= 3))     AS error_count
FROM observability.logs
GROUP BY team_id, bucket_ts, service, environment, severity_bucket, host, pod;

CREATE TABLE IF NOT EXISTS observability.logs_volume_5m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    service         LowCardinality(String),
    environment     LowCardinality(String),
    severity_bucket UInt8,
    host            LowCardinality(String),
    pod             LowCardinality(String),
    log_count       AggregateFunction(sum, UInt64),
    error_count     AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service, environment, severity_bucket, host, pod)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_volume_1m_to_5m
TO observability.logs_volume_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    service, environment, severity_bucket, host, pod,
    sumMergeState(log_count)    AS log_count,
    sumMergeState(error_count)  AS error_count
FROM observability.logs_volume_1m
GROUP BY team_id, bucket_ts, service, environment, severity_bucket, host, pod;

CREATE TABLE IF NOT EXISTS observability.logs_volume_1h (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    service         LowCardinality(String),
    environment     LowCardinality(String),
    severity_bucket UInt8,
    host            LowCardinality(String),
    pod             LowCardinality(String),
    log_count       AggregateFunction(sum, UInt64),
    error_count     AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service, environment, severity_bucket, host, pod)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_volume_5m_to_1h
TO observability.logs_volume_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    service, environment, severity_bucket, host, pod,
    sumMergeState(log_count)    AS log_count,
    sumMergeState(error_count)  AS error_count
FROM observability.logs_volume_5m
GROUP BY team_id, bucket_ts, service, environment, severity_bucket, host, pod;
