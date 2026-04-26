-- infra_resource_by_service — narrow rollup for the
-- /infrastructure/resource-utilisation/by-service endpoint. Collapses the
-- 7-dim metrics_gauges rollup down to (team_id, bucket_ts, service, metric_name).
-- Eliminates the N+1 per-service query pattern: one query returns all
-- services × all resource types.

CREATE TABLE IF NOT EXISTS observability.infra_resource_by_service_1m (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    service       LowCardinality(String),
    metric_name   LowCardinality(String),
    value_avg_num AggregateFunction(sum, Float64),
    sample_count  AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service, metric_name)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.infra_resource_by_service_raw_to_1m
TO observability.infra_resource_by_service_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp) AS bucket_ts,
    service,
    metric_name,
    sumState(value)        AS value_avg_num,
    sumState(toUInt64(1))  AS sample_count
FROM observability.metrics
WHERE metric_name IN (
    'system.cpu.utilization',
    'system.memory.utilization',
    'system.disk.utilization',
    'system.network.io',
    'db.client.connections.usage'
) AND metric_type IN ('Gauge', 'Sum') AND hist_count = 0
GROUP BY team_id, bucket_ts, service, metric_name;

-- 5m tier
CREATE TABLE IF NOT EXISTS observability.infra_resource_by_service_5m (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    service       LowCardinality(String),
    metric_name   LowCardinality(String),
    value_avg_num AggregateFunction(sum, Float64),
    sample_count  AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service, metric_name)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.infra_resource_by_service_1m_to_5m
TO observability.infra_resource_by_service_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    service, metric_name,
    sumMergeState(value_avg_num) AS value_avg_num,
    sumMergeState(sample_count)  AS sample_count
FROM observability.infra_resource_by_service_1m
GROUP BY team_id, bucket_ts, service, metric_name;

-- 1h tier
CREATE TABLE IF NOT EXISTS observability.infra_resource_by_service_1h (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    service       LowCardinality(String),
    metric_name   LowCardinality(String),
    value_avg_num AggregateFunction(sum, Float64),
    sample_count  AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service, metric_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.infra_resource_by_service_5m_to_1h
TO observability.infra_resource_by_service_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    service, metric_name,
    sumMergeState(value_avg_num) AS value_avg_num,
    sumMergeState(sample_count)  AS sample_count
FROM observability.infra_resource_by_service_5m
GROUP BY team_id, bucket_ts, service, metric_name;
