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
-- ===========================================================================
CREATE TABLE IF NOT EXISTS observability.metrics (
    TeamId           LowCardinality(String),
    ServiceName      LowCardinality(String),
    MetricName       LowCardinality(String),
    Timestamp        DateTime        CODEC(Delta, ZSTD(3)),
    Value            Float64         CODEC(ZSTD(1)),
    Count            Float64         CODEC(ZSTD(1)),
    Avg              Float64         CODEC(ZSTD(1)),
    Max              Float64         CODEC(ZSTD(1)),
    P95              Float64         CODEC(ZSTD(1)),
    Attributes       String          CODEC(ZSTD(3)),

    INDEX idx_metric_name   MetricName   TYPE set(500)           GRANULARITY 1,
    INDEX idx_service       ServiceName  TYPE set(100)           GRANULARITY 1,
    INDEX idx_attrs         Attributes   TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/metrics', '{replica}')
PARTITION BY toYYYYMMDD(Timestamp)
ORDER BY (TeamId, ServiceName, Timestamp, MetricName)
TTL Timestamp + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192,
         storage_policy = 'tiered_gcs';
