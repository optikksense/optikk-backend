-- logs_rollup — log volume + error counts per (severity, service, host, pod).
-- Powers: logs overview + heatmap aggregates. Drill-down stays on raw logs.

CREATE TABLE IF NOT EXISTS observability.logs_rollup_1m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    severity_text  LowCardinality(String),
    service        LowCardinality(String),
    host           LowCardinality(String),
    pod            LowCardinality(String),
    log_count      AggregateFunction(sum, UInt64),
    error_count    AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, severity_text, service, host, pod)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_to_rollup_1m
TO observability.logs_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                                     AS bucket_ts,
    severity_text                                                                                  AS severity_text,
    service                                                                                        AS service,
    host                                                                                           AS host,
    pod                                                                                            AS pod,
    sumState(toUInt64(1))                                                                          AS log_count,
    sumState(toUInt64(severity_text IN ('ERROR','FATAL','CRITICAL','SEVERE') OR severity_number >= 17)) AS error_count
FROM observability.logs;

CREATE TABLE IF NOT EXISTS observability.logs_rollup_5m (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    severity_text LowCardinality(String),
    service       LowCardinality(String),
    host          LowCardinality(String),
    pod           LowCardinality(String),
    log_count     AggregateFunction(sum, UInt64),
    error_count   AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, severity_text, service, host, pod)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_rollup_1m_to_5m
TO observability.logs_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       severity_text, service, host, pod,
       sumMergeState(log_count)   AS log_count,
       sumMergeState(error_count) AS error_count
FROM observability.logs_rollup_1m
GROUP BY team_id, bucket_ts, severity_text, service, host, pod;

CREATE TABLE IF NOT EXISTS observability.logs_rollup_1h (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    severity_text LowCardinality(String),
    service       LowCardinality(String),
    host          LowCardinality(String),
    pod           LowCardinality(String),
    log_count     AggregateFunction(sum, UInt64),
    error_count   AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, severity_text, service, host, pod)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_rollup_5m_to_1h
TO observability.logs_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       severity_text, service, host, pod,
       sumMergeState(log_count)   AS log_count,
       sumMergeState(error_count) AS error_count
FROM observability.logs_rollup_5m
GROUP BY team_id, bucket_ts, severity_text, service, host, pod;
