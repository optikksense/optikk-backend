CREATE DATABASE IF NOT EXISTS observability;

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
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(start_time)
ORDER BY (team_id, start_time, trace_id, span_id)
TTL toDateTime(start_time) + INTERVAL 30 DAY
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
    attributes       String        CODEC(ZSTD(3))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, timestamp, level, service_name)
TTL toDateTime(timestamp) + INTERVAL 30 DAY
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
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, timestamp, metric_type, service_name)
TTL toDateTime(timestamp) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- deployments
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.deployments (
    id               UInt64,
    team_id          LowCardinality(String),
    deploy_id        String,
    service_name     LowCardinality(String),
    version          String,
    environment      LowCardinality(String),
    deployed_by      String,
    deploy_time      DateTime64(9) CODEC(Delta, ZSTD(1)),
    status           LowCardinality(String),
    commit_sha       String,
    duration_seconds Int32,
    attributes       String CODEC(ZSTD(3))
) ENGINE = MergeTree()
ORDER BY (team_id, deploy_time, service_name)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- health_check_results
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.health_check_results (
    id               UInt64,
    team_id          LowCardinality(String),
    check_id         String,
    check_name       String,
    check_type       LowCardinality(String),
    target_url       String,
    timestamp        DateTime64(9) CODEC(Delta, ZSTD(1)),
    status           LowCardinality(String),
    response_time_ms Float64,
    http_status_code Int32,
    error_message    String,
    region           LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (team_id, timestamp, check_id)
TTL toDateTime(timestamp) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- ai_requests
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.ai_requests (
    id                UInt64,
    team_id           LowCardinality(String),
    trace_id          String,
    span_id           String,
    model_name        LowCardinality(String),
    model_provider    LowCardinality(String),
    request_type      LowCardinality(String),
    timestamp         DateTime64(9) CODEC(Delta, ZSTD(1)),
    duration_ms       UInt64        CODEC(Delta, ZSTD(1)),
    status            LowCardinality(String),
    timeout           UInt8,
    retry_count       Int32,
    cost_usd          Float64,
    tokens_prompt     UInt64,
    tokens_completion UInt64,
    tokens_system     UInt64,
    cache_hit         UInt8,
    cache_tokens      UInt64,
    pii_detected      UInt8,
    pii_categories    String,
    guardrail_blocked UInt8,
    content_policy    UInt8,
    attributes        String CODEC(ZSTD(3))
) ENGINE = MergeTree()
ORDER BY (team_id, timestamp, model_name, status)
SETTINGS index_granularity = 8192;
