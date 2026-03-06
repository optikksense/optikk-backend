CREATE DATABASE IF NOT EXISTS observability;

-- ===========================================================================
-- DATA TIERING MODEL (GCP)
-- ===========================================================================
-- This schema uses a hot/warm/cold tiering strategy on GCP infrastructure:
--
--   Hot   (Local SSD):  Active data. Never use Persistent Disk for hot data.
--   Warm  (GCS Nearline): Data >30 days. Moved automatically via TTL rules.
--   Cold  (GCS Coldline): Data >90 days. Archived for long-term retention.
--
-- move_factor = 0.2 on the hot volume — when the local SSD is 80% full,
-- ClickHouse automatically spills cold parts to GCS.
--
-- Storage policy 'tiered_gcs' must be configured in clickhouse_storage_config.xml.
-- Without it, TTL MOVE rules are safely ignored.
--
-- Per-team retention is enforced by the RetentionManager in the Go backend,
-- which runs ALTER TABLE ... DELETE queries for data older than each team's
-- configured retention_days.
-- ===========================================================================


-- ===========================================================================
-- SPANS: one row per OTel span
-- ===========================================================================
-- Always ReplicatedMergeTree — never plain MergeTree in production.
-- Partition by toYYYYMMDD for time-series tables.
-- Order keys: (team_id, service_name, start_time, ...).
-- LowCardinality(String) for any column with <10k unique values.
-- Codec(Delta, ZSTD(3)) for timestamps. No Gorilla on integer durations.
-- Tags as Map(String, String) with MATERIALIZED indexes on top 10 OTel keys.
-- Never store JSON blobs in String columns — parse at ingest.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS observability.spans (
    team_id              LowCardinality(String),
    trace_id             String,
    span_id              String,
    parent_span_id       String,
    parent_service_name  LowCardinality(String),
    is_root              UInt8,
    operation_name       LowCardinality(String),
    service_name         LowCardinality(String),
    span_kind            LowCardinality(String),
    start_time           DateTime64(9)   CODEC(Delta, ZSTD(3)),
    end_time             DateTime64(9)   CODEC(Delta, ZSTD(3)),
    duration_ms          UInt64          CODEC(Delta, ZSTD(1)),
    status               LowCardinality(String),
    status_message       String          CODEC(ZSTD(3)),
    http_method          LowCardinality(String),
    http_url             String          CODEC(ZSTD(3)),
    http_status_code     Int32,
    host                 LowCardinality(String),
    pod                  LowCardinality(String),
    container            LowCardinality(String),
    attributes           String          CODEC(ZSTD(3)),
    tags                 Map(String, String),

    -- Skip indexes for fast point lookups
    INDEX idx_trace_id       trace_id             TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_span_id        span_id              TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_service        service_name         TYPE set(100)           GRANULARITY 1,

    -- Materialized indexes on top 10 queried tag keys
    INDEX idx_tags_service   tags['service.name']            TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tags_env       tags['deployment.environment']  TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tags_version   tags['service.version']         TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tags_http_url  tags['http.url']                TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tags_http_meth tags['http.method']             TYPE set(20)            GRANULARITY 1,
    INDEX idx_tags_status    tags['otel.status_code']        TYPE set(10)            GRANULARITY 1,
    INDEX idx_tags_host      tags['host.name']               TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tags_k8s_ns    tags['k8s.namespace.name']      TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tags_k8s_pod   tags['k8s.pod.name']            TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tags_error     tags['error']                   TYPE set(5)             GRANULARITY 1
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/spans', '{replica}')
PARTITION BY toYYYYMMDD(start_time)
ORDER BY (team_id, service_name, start_time, trace_id, span_id)
TTL toDateTime(start_time) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192,
         storage_policy = 'tiered_gcs';


-- ===========================================================================
-- LOGS
-- ===========================================================================
CREATE TABLE IF NOT EXISTS observability.logs (
    team_id          LowCardinality(String),
    timestamp        DateTime64(9)   CODEC(Delta, ZSTD(3)),
    level            LowCardinality(String),
    service_name     LowCardinality(String),
    logger           LowCardinality(String),
    message          String          CODEC(ZSTD(3)),
    trace_id         String,
    span_id          String,
    host             LowCardinality(String),
    pod              LowCardinality(String),
    container        LowCardinality(String),
    thread           LowCardinality(String),
    exception        String          CODEC(ZSTD(3)),
    attributes       String          CODEC(ZSTD(3)),
    tags             Map(String, String),

    -- Skip indexes
    INDEX idx_trace_id       trace_id             TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_span_id        span_id              TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_level          level                TYPE set(10)            GRANULARITY 1,
    INDEX idx_service        service_name         TYPE set(100)           GRANULARITY 1,

    -- Materialized indexes on top 10 queried tag keys
    INDEX idx_tags_service   tags['service.name']            TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tags_env       tags['deployment.environment']  TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tags_version   tags['service.version']         TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tags_http_url  tags['http.url']                TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tags_http_meth tags['http.method']             TYPE set(20)            GRANULARITY 1,
    INDEX idx_tags_status    tags['otel.status_code']        TYPE set(10)            GRANULARITY 1,
    INDEX idx_tags_host      tags['host.name']               TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tags_k8s_ns    tags['k8s.namespace.name']      TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tags_k8s_pod   tags['k8s.pod.name']            TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tags_error     tags['error']                   TYPE set(5)             GRANULARITY 1
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, service_name, timestamp)
TTL toDateTime(timestamp) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192,
         storage_policy = 'tiered_gcs';


-- ===========================================================================
-- METRICS: raw per-datapoint rows
-- ===========================================================================
-- Column names use PascalCase to match the Go backend query constants
-- (ColMetricName = "MetricName", ColValue = "Value", etc.).
--
-- The Attributes column stores OTel resource/metric attributes as a JSON
-- string. Go code uses JSONExtractString / JSONExtractFloat / JSONExtractInt
-- to query individual keys at read time.
--
-- Batch inserts only: 1000 rows or 500ms minimum.
-- Never insert from HTTP hot path — queue → worker → ClickHouse always.
-- Requires ClickHouse 26+ (JSON type is stable from 26.x onwards).
-- ===========================================================================
CREATE TABLE IF NOT EXISTS observability.metrics (

    team_id              LowCardinality(String),
    env                  LowCardinality(String) DEFAULT 'default',
    metric_name          LowCardinality(String),
    metric_type          LowCardinality(String),
    temporality          LowCardinality(String) DEFAULT 'Unspecified',
    is_monotonic         Bool CODEC(T64, ZSTD(1)),
    unit                 LowCardinality(String) DEFAULT '',
    description          LowCardinality(String) DEFAULT '',
    resource_fingerprint UInt64 CODEC(Delta(8), ZSTD(1)),
    timestamp            DateTime64(3) CODEC(DoubleDelta, LZ4),
    value                Float64 CODEC(Gorilla, ZSTD(1)),
    hist_sum             Float64 CODEC(Gorilla, ZSTD(1)),
    hist_count           UInt64 CODEC(T64, ZSTD(1)),
    hist_buckets         Array(Float64) CODEC(ZSTD(1)),
    hist_counts          Array(UInt64) CODEC(T64, ZSTD(1)),
    attributes           JSON(max_dynamic_paths=100) CODEC(ZSTD(1)),

    -- === MATERIALIZED columns (native JSON path extraction, requires CH 26+) ===
    service              LowCardinality(String) MATERIALIZED attributes.`service.name`::String,
    host                 LowCardinality(String) MATERIALIZED attributes.`host.name`::String,
    environment          LowCardinality(String) MATERIALIZED attributes.`deployment.environment`::String,
    k8s_namespace        LowCardinality(String) MATERIALIZED attributes.`k8s.namespace.name`::String,
    http_method          LowCardinality(String) MATERIALIZED attributes.`http.method`::String,
    http_status_code     UInt16                 MATERIALIZED attributes.`http.status_code`::UInt16,
    has_error            Bool                   MATERIALIZED attributes.`error`::Bool,

    -- === INDEXES on materialized columns ===
    INDEX idx_service          service              TYPE set(200)      GRANULARITY 1,
    INDEX idx_host             host                 TYPE bloom_filter  GRANULARITY 1,
    INDEX idx_environment      environment          TYPE set(10)       GRANULARITY 1,
    INDEX idx_k8s_namespace    k8s_namespace        TYPE set(100)      GRANULARITY 1,
    INDEX idx_http_method      http_method          TYPE set(20)       GRANULARITY 1,
    INDEX idx_http_status_code http_status_code     TYPE minmax        GRANULARITY 1,
    INDEX idx_has_error        has_error            TYPE set(2)        GRANULARITY 1,
    INDEX idx_fingerprint      resource_fingerprint TYPE bloom_filter  GRANULARITY 4

) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/metrics', '{replica}')
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, metric_name, service, environment, temporality, timestamp, resource_fingerprint)
TTL toDateTime(timestamp) + INTERVAL 30 DAY   TO VOLUME 'warm',
    toDateTime(timestamp) + INTERVAL 90 DAY   TO VOLUME 'cold',
    toDateTime(timestamp) + INTERVAL 365 DAY  DELETE
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    storage_policy = 'tiered_gcs';