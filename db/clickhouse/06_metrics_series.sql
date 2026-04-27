-- INVARIANT: bucket values are computed Go-side (internal/infra/timebucket); MVs below pass through per-row ts_bucket_hour / _6hr / _day populated by the mapper. Resource labels are mapper-written top-level columns on observability.metrics; MVs are pure column copies.

CREATE TABLE IF NOT EXISTS observability.time_series_v4 (
    team_id          UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket        DateTime CODEC(DoubleDelta, LZ4),
    metric_name      LowCardinality(String) CODEC(ZSTD(1)),
    fingerprint      String CODEC(ZSTD(3)),
    service          LowCardinality(String) CODEC(ZSTD(1)),
    host             LowCardinality(String) CODEC(ZSTD(1)),
    environment      LowCardinality(String) CODEC(ZSTD(1)),
    k8s_namespace    LowCardinality(String) CODEC(ZSTD(1)),
    http_method      LowCardinality(String) CODEC(ZSTD(1)),
    http_status_code UInt16 CODEC(T64, ZSTD(1))
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(ts_bucket)
ORDER BY (team_id, ts_bucket, metric_name, fingerprint)
TTL ts_bucket + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_to_time_series_v4
TO observability.time_series_v4 AS
SELECT DISTINCT
    team_id,
    ts_bucket_hour AS ts_bucket,
    metric_name,
    fingerprint,
    service,
    host,
    environment,
    k8s_namespace,
    http_method,
    http_status_code
FROM observability.metrics
WHERE fingerprint != '';

CREATE TABLE IF NOT EXISTS observability.time_series_v4_6hrs AS observability.time_series_v4
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(ts_bucket)
ORDER BY (team_id, ts_bucket, metric_name, fingerprint)
TTL ts_bucket + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_to_time_series_v4_6hrs
TO observability.time_series_v4_6hrs AS
SELECT DISTINCT
    team_id,
    ts_bucket_6hr AS ts_bucket,
    metric_name,
    fingerprint,
    service,
    host,
    environment,
    k8s_namespace,
    http_method,
    http_status_code
FROM observability.metrics
WHERE fingerprint != '';

CREATE TABLE IF NOT EXISTS observability.time_series_v4_1day AS observability.time_series_v4
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(ts_bucket)
ORDER BY (team_id, ts_bucket, metric_name, fingerprint)
TTL ts_bucket + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_to_time_series_v4_1day
TO observability.time_series_v4_1day AS
SELECT DISTINCT
    team_id,
    ts_bucket_day AS ts_bucket,
    metric_name,
    fingerprint,
    service,
    host,
    environment,
    k8s_namespace,
    http_method,
    http_status_code
FROM observability.metrics
WHERE fingerprint != '';

