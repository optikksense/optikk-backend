-- Resource helper tables — narrow MV-fed dictionaries used to resolve fingerprint before the raw-table scan. No CH bucket functions; ts_bucket passes through from the upstream raw table.

CREATE TABLE IF NOT EXISTS observability.spans_resource (
    team_id     UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket   UInt64 CODEC(DoubleDelta, LZ4),
    fingerprint String CODEC(ZSTD(1)),
    service     LowCardinality(String) CODEC(ZSTD(1)),
    host        LowCardinality(String) CODEC(ZSTD(1)),
    pod         LowCardinality(String) CODEC(ZSTD(1)),
    environment LowCardinality(String) CODEC(ZSTD(1))
) ENGINE = ReplacingMergeTree()
PARTITION BY (toYYYYMMDD(toDateTime(ts_bucket)), toHour(toDateTime(ts_bucket)))
ORDER BY (team_id, ts_bucket, fingerprint)
TTL toDateTime(ts_bucket) + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_to_spans_resource
TO observability.spans_resource AS
SELECT DISTINCT
    team_id,
    ts_bucket,
    fingerprint,
    service,
    host,
    pod,
    environment
FROM observability.spans
WHERE fingerprint != '';

CREATE TABLE IF NOT EXISTS observability.logs_resource (
    team_id     UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket   UInt32 CODEC(Delta(4), LZ4),
    fingerprint String CODEC(ZSTD(1)),
    service     LowCardinality(String) CODEC(ZSTD(1)),
    host        LowCardinality(String) CODEC(ZSTD(1)),
    pod         LowCardinality(String) CODEC(ZSTD(1)),
    container   LowCardinality(String) CODEC(ZSTD(1)),
    environment LowCardinality(String) CODEC(ZSTD(1))
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(toDateTime(ts_bucket))
ORDER BY (team_id, ts_bucket, fingerprint)
TTL toDateTime(ts_bucket) + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_to_logs_resource
TO observability.logs_resource AS
SELECT DISTINCT
    team_id,
    ts_bucket,
    fingerprint,
    service,
    host,
    pod,
    container,
    environment
FROM observability.logs
WHERE fingerprint != '';

-- Metrics resource dictionary at hourly grain. Keyed per-(metric_name, fingerprint) so it doubles as a per-metric series catalog; resolver scans `SELECT DISTINCT fingerprint`.

CREATE TABLE IF NOT EXISTS observability.metrics_resource (
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

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_to_metrics_resource
TO observability.metrics_resource AS
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
