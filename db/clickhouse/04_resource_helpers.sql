-- SigNoz-style resource helper tables. These stay narrow and exist only to
-- resolve resource_fingerprint values before the main raw-table scan.

CREATE TABLE IF NOT EXISTS observability.traces_v3_resource (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket_start      UInt64 CODEC(DoubleDelta, LZ4),
    resource_fingerprint String CODEC(ZSTD(1)),
    service_name         LowCardinality(String) CODEC(ZSTD(1)),
    host_name            LowCardinality(String) CODEC(ZSTD(1)),
    pod_name             LowCardinality(String) CODEC(ZSTD(1)),
    environment          LowCardinality(String) CODEC(ZSTD(1))
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(toDateTime(ts_bucket_start))
ORDER BY (team_id, ts_bucket_start, resource_fingerprint, service_name, environment, host_name, pod_name)
TTL toDateTime(ts_bucket_start) + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_to_traces_v3_resource
TO observability.traces_v3_resource AS
SELECT DISTINCT
    team_id,
    ts_bucket_start,
    resource_fingerprint,
    service_name,
    mat_host_name AS host_name,
    mat_k8s_pod_name AS pod_name,
    mat_deployment_environment AS environment
FROM observability.signoz_index_v3
WHERE resource_fingerprint != '';

CREATE TABLE IF NOT EXISTS observability.logs_v2_resource (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket_start      UInt32 CODEC(Delta(4), LZ4),
    resource_fingerprint String CODEC(ZSTD(1)),
    service              LowCardinality(String) CODEC(ZSTD(1)),
    host                 LowCardinality(String) CODEC(ZSTD(1)),
    pod                  LowCardinality(String) CODEC(ZSTD(1)),
    container            LowCardinality(String) CODEC(ZSTD(1)),
    environment          LowCardinality(String) CODEC(ZSTD(1))
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(toDateTime(ts_bucket_start))
ORDER BY (team_id, ts_bucket_start, resource_fingerprint, service, environment, host, pod, container)
TTL toDateTime(ts_bucket_start) + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_to_logs_v2_resource
TO observability.logs_v2_resource AS
SELECT DISTINCT
    team_id,
    ts_bucket_start,
    resource_fingerprint,
    service,
    host,
    pod,
    container,
    environment
FROM observability.logs_v2
WHERE resource_fingerprint != '';
