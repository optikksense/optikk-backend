CREATE DATABASE IF NOT EXISTS observability;

-- ===========================================================================
-- DATA TIERING MODEL
-- ===========================================================================
-- This schema uses a warm/cold tiering strategy via ClickHouse TTL rules:
--
--   Hot tier  (days 0-3):   Data lives on the default storage volume (fast SSD).
--   Warm tier (days 3-30):  Data is moved to the 'warm' volume (cheaper storage).
--   Deletion  (day 30+):    Data is deleted after the default retention period.
--
-- IMPORTANT: For tiering to work, you must configure a storage policy with a
-- 'warm' volume in your ClickHouse server's config.xml. Example:
--
--   <storage_configuration>
--     <disks>
--       <default> ... </default>
--       <warm_disk>
--         <path>/mnt/warm-storage/clickhouse/</path>
--       </warm_disk>
--     </disks>
--     <policies>
--       <tiered>
--         <volumes>
--           <hot>
--             <disk>default</disk>
--           </hot>
--           <warm>
--             <disk>warm_disk</disk>
--           </warm>
--         </volumes>
--       </tiered>
--     </policies>
--   </storage_configuration>
--
-- Then set SETTINGS storage_policy = 'tiered' on each table.
-- Without this configuration the warm TTL MOVE rules are ignored safely.
--
-- Per-team retention is enforced by the RetentionManager in the Go backend,
-- which runs ALTER TABLE ... DELETE queries for data older than each team's
-- configured retention_days. The TTL DELETE below acts as a hard backstop.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- spans: one row per OTel span. Root spans (is_root=1) drive service metrics.
-- LowCardinality on high-repeat string columns cuts storage ~10x and speeds
-- GROUP BY. DateTime64(9) preserves nanosecond OTLP timestamps losslessly.
-- Delta+ZSTD codec on monotonic columns gives 3-5x additional compression.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.spans (
    team_id              LowCardinality(String),
    trace_id             String,
    span_id              String,
    parent_span_id       String,
    parent_service_name  LowCardinality(String),   -- denormalized at ingest; enables JOIN-free dependency views
    is_root              UInt8,
    operation_name       String,
    service_name         LowCardinality(String),
    span_kind            LowCardinality(String),
    start_time           DateTime64(9) CODEC(Delta, ZSTD(1)),
    end_time             DateTime64(9) CODEC(Delta, ZSTD(1)),
    duration_ms          UInt64        CODEC(Delta, ZSTD(1)),
    status               LowCardinality(String),
    status_message       String        CODEC(ZSTD(3)),
    http_method          LowCardinality(String),
    http_url             String        CODEC(ZSTD(3)),
    http_status_code     Int32,
    host                 LowCardinality(String),
    pod                  LowCardinality(String),
    container            LowCardinality(String),
    attributes           String        CODEC(ZSTD(3)),
    -- Skip indexes for fast point lookups and service filtering
    INDEX idx_trace_id   trace_id     TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_span_id    span_id      TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_service    service_name TYPE set(100)           GRANULARITY 1
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/spans', '{replica}')
PARTITION BY toYYYYMM(start_time)
ORDER BY (team_id, service_name, start_time, trace_id, span_id)
TTL toDateTime(start_time) + INTERVAL 3 DAY TO VOLUME 'warm',
    toDateTime(start_time) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- logs
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.logs (
    id               UInt64,
    team_id          LowCardinality(String),
    timestamp        DateTime64(9) CODEC(Delta, ZSTD(1)),
    level            LowCardinality(String),
    service_name     LowCardinality(String),
    logger           String,
    message          String        CODEC(ZSTD(3)),
    trace_id         String,
    span_id          String,
    host             LowCardinality(String),
    pod              LowCardinality(String),
    container        LowCardinality(String),
    thread           String,
    exception        String        CODEC(ZSTD(3)),
    attributes       String        CODEC(ZSTD(3)),
    -- Skip indexes for fast point lookups and filter acceleration
    INDEX idx_trace_id   trace_id     TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_span_id    span_id      TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_level      level        TYPE set(10)            GRANULARITY 1,
    INDEX idx_service    service_name TYPE set(100)           GRANULARITY 1
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/logs', '{replica}')
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, service_name, timestamp, id)
TTL toDateTime(timestamp) + INTERVAL 3 DAY TO VOLUME 'warm',
    toDateTime(timestamp) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- metrics
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.metrics (
    id               UInt64,
    team_id          LowCardinality(String),
    metric_id        String,
    metric_name      LowCardinality(String),
    metric_type      LowCardinality(String),
    metric_category  LowCardinality(String),
    service_name     LowCardinality(String),
    operation_name   String,
    timestamp        DateTime64(9) CODEC(Delta, ZSTD(1)),
    value            Float64,
    count            UInt64,
    sum              Float64,
    min              Float64,
    max              Float64,
    avg              Float64,
    p50              Float64,
    p95              Float64,
    p99              Float64,
    http_method      LowCardinality(String),
    http_status_code Int32,
    status           LowCardinality(String),
    host             LowCardinality(String),
    pod              LowCardinality(String),
    container        LowCardinality(String),
    attributes       String        CODEC(ZSTD(3))
) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/metrics', '{replica}')
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, service_name, timestamp, metric_name)
TTL toDateTime(timestamp) + INTERVAL 3 DAY TO VOLUME 'warm',
    toDateTime(timestamp) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;

