-- Minimal SigNoz-style metrics metadata helpers. These keep label and
-- timeseries discovery off the wide raw samples table.

CREATE TABLE IF NOT EXISTS observability.time_series_v4 (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts            DateTime CODEC(DoubleDelta, LZ4),
    metric_name          LowCardinality(String) CODEC(ZSTD(1)),
    resource_fingerprint String CODEC(ZSTD(3)),
    service              LowCardinality(String) CODEC(ZSTD(1)),
    host                 LowCardinality(String) CODEC(ZSTD(1)),
    environment          LowCardinality(String) CODEC(ZSTD(1)),
    k8s_namespace        LowCardinality(String) CODEC(ZSTD(1)),
    http_method          LowCardinality(String) CODEC(ZSTD(1)),
    http_status_code     UInt16 CODEC(T64, ZSTD(1))
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, resource_fingerprint, service, environment, host, k8s_namespace)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_to_time_series_v4
TO observability.time_series_v4 AS
SELECT DISTINCT
    team_id,
    toStartOfHour(timestamp) AS bucket_ts,
    metric_name,
    resource_fingerprint,
    service,
    host,
    environment,
    k8s_namespace,
    http_method,
    http_status_code
FROM observability.metrics;

CREATE TABLE IF NOT EXISTS observability.time_series_v4_6hrs AS observability.time_series_v4
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, resource_fingerprint, service, environment, host, k8s_namespace)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_to_time_series_v4_6hrs
TO observability.time_series_v4_6hrs AS
SELECT DISTINCT
    team_id,
    toStartOfInterval(timestamp, toIntervalHour(6)) AS bucket_ts,
    metric_name,
    resource_fingerprint,
    service,
    host,
    environment,
    k8s_namespace,
    http_method,
    http_status_code
FROM observability.metrics;

CREATE TABLE IF NOT EXISTS observability.time_series_v4_1day AS observability.time_series_v4
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, resource_fingerprint, service, environment, host, k8s_namespace)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_to_time_series_v4_1day
TO observability.time_series_v4_1day AS
SELECT DISTINCT
    team_id,
    toStartOfDay(timestamp) AS bucket_ts,
    metric_name,
    resource_fingerprint,
    service,
    host,
    environment,
    k8s_namespace,
    http_method,
    http_status_code
FROM observability.metrics;
