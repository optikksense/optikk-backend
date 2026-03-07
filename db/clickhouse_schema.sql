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
-- SPANS: one row per OTel span (optimized schema with JSON and materialized columns)
-- ===========================================================================
-- Optimizations applied:
-- 1. Replaced Map columns with JSON (max_dynamic_paths=50) for better query performance
-- 2. Added materialized columns for hot attributes (HTTP, DB, RPC, Messaging, Network)
-- 3. Monthly partitions (toYYYYMM) instead of daily for better partition management
-- 4. Optimized ORDER BY with ts_bucket_start for better time-range queries
-- 5. Added bloom filter indexes on materialized columns
-- ===========================================================================
CREATE TABLE IF NOT EXISTS observability.spans (
    -- BUCKETING & ORDERING
    ts_bucket_start                       UInt64          CODEC(DoubleDelta, LZ4),
    resource_fingerprint                  String          CODEC(ZSTD(1)),

    -- MULTI-TENANCY
    team_id                               LowCardinality(String) CODEC(ZSTD(1)),

    -- CORE IDENTITY
    timestamp                             DateTime64(9)   CODEC(DoubleDelta, LZ4),
    trace_id                              FixedString(32) CODEC(ZSTD(1)),
    span_id                               String          CODEC(ZSTD(1)),
    parent_span_id                        String          CODEC(ZSTD(1)),
    trace_state                           String          CODEC(ZSTD(1)),
    flags                                 UInt32          CODEC(T64, ZSTD(1)),

    -- SPAN METADATA
    name                                  LowCardinality(String) CODEC(ZSTD(1)),
    kind                                  Int8            CODEC(T64, ZSTD(1)),
    kind_string                           LowCardinality(String) CODEC(ZSTD(1)),
    duration_nano                         UInt64          CODEC(T64, ZSTD(1)),
    has_error                             Bool            CODEC(T64, ZSTD(1)),
    is_remote                             LowCardinality(String) CODEC(ZSTD(1)),

    -- STATUS
    status_code                           Int16           CODEC(T64, ZSTD(1)),
    status_code_string                    LowCardinality(String) CODEC(ZSTD(1)),
    status_message                        String          CODEC(ZSTD(1)),

    -- HTTP / DB SHORTCUTS (derived at ingest)
    http_url                              LowCardinality(String) CODEC(ZSTD(1)),
    http_method                           LowCardinality(String) CODEC(ZSTD(1)),
    http_host                             LowCardinality(String) CODEC(ZSTD(1)),
    external_http_url                     LowCardinality(String) CODEC(ZSTD(1)),
    external_http_method                  LowCardinality(String) CODEC(ZSTD(1)),
    response_status_code                  LowCardinality(String) CODEC(ZSTD(1)),
    db_name                               LowCardinality(String) CODEC(ZSTD(1)),
    db_operation                          LowCardinality(String) CODEC(ZSTD(1)),

    -- ATTRIBUTES (JSON overflow - replaces Map for better performance)
    attributes                            JSON(max_dynamic_paths = 50) CODEC(ZSTD(1)),

    -- EVENTS & LINKS
    events                                Array(String)   CODEC(ZSTD(2)),
    links                                 String          CODEC(ZSTD(1)),

    -- ERROR FIELDS
    exception_type                        LowCardinality(String) CODEC(ZSTD(1)),
    exception_message                     String          CODEC(ZSTD(1)),
    exception_stacktrace                  String          CODEC(ZSTD(1)),
    exception_escaped                     Bool            CODEC(T64, ZSTD(1)),

    -- MATERIALIZED HOT ATTRIBUTES (computed at insert from JSON column)
    -- HTTP
    mat_http_route            LowCardinality(String)
                                    MATERIALIZED attributes.`http.route`::String               CODEC(ZSTD(1)),
    mat_http_status_code      LowCardinality(String)
                                    MATERIALIZED attributes.`http.status_code`::String         CODEC(ZSTD(1)),
    mat_http_target           LowCardinality(String)
                                    MATERIALIZED attributes.`http.target`::String              CODEC(ZSTD(1)),
    mat_http_scheme           LowCardinality(String)
                                    MATERIALIZED attributes.`http.scheme`::String              CODEC(ZSTD(1)),

    -- DB
    mat_db_system             LowCardinality(String)
                                    MATERIALIZED attributes.`db.system`::String                CODEC(ZSTD(1)),
    mat_db_name               LowCardinality(String)
                                    MATERIALIZED attributes.`db.name`::String                  CODEC(ZSTD(1)),
    mat_db_operation          LowCardinality(String)
                                    MATERIALIZED attributes.`db.operation`::String             CODEC(ZSTD(1)),
    mat_db_statement          String
                                    MATERIALIZED attributes.`db.statement`::String             CODEC(ZSTD(1)),

    -- RPC
    mat_rpc_system            LowCardinality(String)
                                    MATERIALIZED attributes.`rpc.system`::String               CODEC(ZSTD(1)),
    mat_rpc_service           LowCardinality(String)
                                    MATERIALIZED attributes.`rpc.service`::String              CODEC(ZSTD(1)),
    mat_rpc_method            LowCardinality(String)
                                    MATERIALIZED attributes.`rpc.method`::String               CODEC(ZSTD(1)),
    mat_rpc_grpc_status_code  LowCardinality(String)
                                    MATERIALIZED attributes.`rpc.grpc.status_code`::String     CODEC(ZSTD(1)),

    -- Messaging
    mat_messaging_system      LowCardinality(String)
                                    MATERIALIZED attributes.`messaging.system`::String         CODEC(ZSTD(1)),
    mat_messaging_operation   LowCardinality(String)
                                    MATERIALIZED attributes.`messaging.operation`::String      CODEC(ZSTD(1)),
    mat_messaging_destination LowCardinality(String)
                                    MATERIALIZED attributes.`messaging.destination`::String    CODEC(ZSTD(1)),

    -- Network / Peer
    mat_peer_service          LowCardinality(String)
                                    MATERIALIZED attributes.`peer.service`::String             CODEC(ZSTD(1)),
    mat_net_peer_name         LowCardinality(String)
                                    MATERIALIZED attributes.`net.peer.name`::String            CODEC(ZSTD(1)),
    mat_net_peer_port         LowCardinality(String)
                                    MATERIALIZED attributes.`net.peer.port`::String            CODEC(ZSTD(1)),

    -- Exceptions
    mat_exception_type        LowCardinality(String)
                                    MATERIALIZED attributes.`exception.type`::String           CODEC(ZSTD(1)),

    -- BLOOM FILTER INDEXES
    INDEX idx_trace_id              trace_id                TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_span_name             name                    TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_http_route        mat_http_route          TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_http_status_code  mat_http_status_code    TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_db_system         mat_db_system           TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_db_name           mat_db_name             TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_rpc_service       mat_rpc_service         TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_peer_service      mat_peer_service        TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_exception_type    mat_exception_type      TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/spans', '{replica}')
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, ts_bucket_start, has_error, name, timestamp)
TTL toDate(timestamp) + INTERVAL 30 DAY
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1,
    storage_policy = 'tiered_gcs';


-- ===========================================================================
-- SPAN EVENTS: one row per OTel span event (exceptions, logs within spans)
-- ===========================================================================
-- Queried by trace detail (waterfall) and error tracking (exception rate).
-- ORDER BY puts team_id first for multi-tenant partition pruning.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS observability.span_events (
    team_id              LowCardinality(String)     CODEC(ZSTD(1)),
    trace_id             FixedString(32)            CODEC(ZSTD(1)),
    span_id              String                     CODEC(ZSTD(1)),
    timestamp            DateTime64(9)              CODEC(DoubleDelta, LZ4),
    name                 LowCardinality(String)     CODEC(ZSTD(1)),
    attributes           JSON(max_dynamic_paths=50) CODEC(ZSTD(1)),

    INDEX idx_trace_id   trace_id  TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_span_id    span_id   TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_name       name      TYPE set(50)            GRANULARITY 1
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/span_events', '{replica}')
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, trace_id, span_id, timestamp)
TTL toDate(timestamp) + INTERVAL 30 DAY
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1,
    storage_policy = 'tiered_gcs';


-- ===========================================================================
-- RESOURCES: resource metadata for spans (optimized schema)
-- ===========================================================================
-- Stores unique resource fingerprints with top known keys as real columns
-- to avoid JSON parsing at query time. Overflow attributes stored in JSON.
-- ===========================================================================
CREATE TABLE IF NOT EXISTS observability.resources (
    fingerprint               String  CODEC(ZSTD(1)),
    seen_at_ts_bucket_start   Int64   CODEC(Delta(8), ZSTD(1)),

    -- Multi-tenancy
    team_id                   LowCardinality(String) CODEC(ZSTD(1)),

    -- Top known keys as real columns — zero JSON parsing at query time
    service_name              LowCardinality(String) CODEC(ZSTD(1)),
    environment               LowCardinality(String) CODEC(ZSTD(1)),
    host_name                 LowCardinality(String) CODEC(ZSTD(1)),
    host_type                 LowCardinality(String) CODEC(ZSTD(1)),
    k8s_namespace             LowCardinality(String) CODEC(ZSTD(1)),
    k8s_pod_name              LowCardinality(String) CODEC(ZSTD(1)),
    k8s_deployment_name       LowCardinality(String) CODEC(ZSTD(1)),
    k8s_cluster_name          LowCardinality(String) CODEC(ZSTD(1)),
    cloud_provider            LowCardinality(String) CODEC(ZSTD(1)),
    cloud_region              LowCardinality(String) CODEC(ZSTD(1)),
    telemetry_sdk_language    LowCardinality(String) CODEC(ZSTD(1)),
    telemetry_sdk_version     LowCardinality(String) CODEC(ZSTD(1)),

    -- Overflow JSON capped at 50 paths
    labels                    JSON(max_dynamic_paths = 50) CODEC(ZSTD(1)),

    INDEX idx_service_name      service_name    TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_environment       environment     TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_k8s_namespace     k8s_namespace   TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_k8s_cluster       k8s_cluster_name TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/resources', '{replica}')
PARTITION BY toYYYYMM(toDateTime(seen_at_ts_bucket_start))
ORDER BY (team_id, fingerprint, seen_at_ts_bucket_start)
TTL toDate(toDateTime(seen_at_ts_bucket_start)) + INTERVAL 30 DAY
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1,
    storage_policy = 'tiered_gcs';


-- ===========================================================================
-- LOGS
-- ===========================================================================
CREATE TABLE IF NOT EXISTS observability.logs (
    -- Multi-tenancy
    team_id              LowCardinality(String) CODEC(ZSTD(1)),

    -- Time columns
    ts_bucket_start      UInt32 CODEC(DoubleDelta, LZ4),
    timestamp            UInt64 CODEC(DoubleDelta, LZ4),
    observed_timestamp   UInt64 CODEC(DoubleDelta, LZ4),

    -- Identity
    id                   String CODEC(ZSTD(1)),
    trace_id             String CODEC(ZSTD(1)),
    span_id              String CODEC(ZSTD(1)),
    trace_flags          UInt32 DEFAULT 0,

    -- Severity
    severity_text        LowCardinality(String) CODEC(ZSTD(1)),
    severity_number      UInt8 DEFAULT 0,

    -- Body
    body                 String CODEC(ZSTD(2)),

    -- Typed attribute maps
    attributes_string    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    attributes_number    Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    attributes_bool      Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),

    -- Resource (JSON column with MATERIALIZED extraction for hot paths)
    resource             JSON(max_dynamic_paths=100) CODEC(ZSTD(1)),
    resource_fingerprint String CODEC(ZSTD(1)),

    -- Scope
    scope_name           String CODEC(ZSTD(1)),
    scope_version        String CODEC(ZSTD(1)),
    scope_string         Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    -- MATERIALIZED columns (native JSON path extraction, requires CH 26+)
    service              LowCardinality(String) MATERIALIZED resource.`service.name`::String,
    host                 LowCardinality(String) MATERIALIZED resource.`host.name`::String,
    pod                  LowCardinality(String) MATERIALIZED resource.`k8s.pod.name`::String,
    container            LowCardinality(String) MATERIALIZED resource.`k8s.container.name`::String,
    environment          LowCardinality(String) MATERIALIZED resource.`deployment.environment`::String,

    -- Skip indexes
    INDEX idx_trace_id   trace_id        TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_id    span_id         TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_body       body            TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1,
    INDEX idx_severity   severity_text   TYPE set(10) GRANULARITY 1,
    INDEX idx_service    service         TYPE set(200) GRANULARITY 1,
    INDEX idx_host       host            TYPE bloom_filter(0.01) GRANULARITY 1

) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/logs', '{replica}')
PARTITION BY toDate(toDateTime(ts_bucket_start))
ORDER BY (team_id, ts_bucket_start, service, timestamp)
TTL toDateTime(ts_bucket_start) + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
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