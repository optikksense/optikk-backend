CREATE DATABASE IF NOT EXISTS observability;

CREATE TABLE IF NOT EXISTS observability.spans (
    team_id String,
    trace_id String,
    span_id String,
    parent_span_id String,
    is_root UInt8,
    operation_name String,
    service_name String,
    span_kind String,
    start_time DateTime,
    end_time DateTime,
    duration_ms UInt64,
    status String,
    status_message String,
    http_method String,
    http_url String,
    http_status_code Int32,
    host String,
    pod String,
    container String,
    attributes String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(start_time)
ORDER BY (team_id, start_time, trace_id, span_id)
TTL start_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS observability.logs (
    id UInt64,
    team_id String,
    timestamp DateTime,
    level String,
    service_name String,
    logger String,
    message String,
    trace_id String,
    span_id String,
    host String,
    pod String,
    container String,
    thread String,
    exception String,
    attributes String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, timestamp, level, service_name)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS observability.metrics (
    id UInt64,
    team_id String,
    metric_id String,
    metric_name String,
    metric_type String,
    metric_category String,
    service_name String,
    operation_name String,
    timestamp DateTime,
    value Float64,
    count UInt64,
    sum Float64,
    min Float64,
    max Float64,
    avg Float64,
    p50 Float64,
    p95 Float64,
    p99 Float64,
    http_method String,
    http_status_code Int32,
    status String,
    host String,
    pod String,
    container String,
    attributes String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, timestamp, metric_type, service_name)
TTL timestamp + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS observability.deployments (
    id UInt64,
    team_id String,
    deploy_id String,
    service_name String,
    version String,
    environment String,
    deployed_by String,
    deploy_time DateTime,
    status String,
    commit_sha String,
    duration_seconds Int32,
    attributes String
) ENGINE = MergeTree()
ORDER BY (team_id, deploy_time, service_name)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS observability.health_check_results (
    id UInt64,
    team_id String,
    check_id String,
    check_name String,
    check_type String,
    target_url String,
    timestamp DateTime,
    status String,
    response_time_ms Float64,
    http_status_code Int32,
    error_message String,
    region String
) ENGINE = MergeTree()
ORDER BY (team_id, timestamp, check_id)
TTL timestamp + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS observability.ai_requests (
    id UInt64,
    team_id String,
    trace_id String,
    span_id String,
    model_name String,
    model_provider String,
    request_type String,
    timestamp DateTime,
    duration_ms UInt64,
    status String,
    timeout UInt8,
    retry_count Int32,
    cost_usd Float64,
    tokens_prompt UInt64,
    tokens_completion UInt64,
    tokens_system UInt64,
    cache_hit UInt8,
    cache_tokens UInt64,
    pii_detected UInt8,
    pii_categories String,
    guardrail_blocked UInt8,
    content_policy UInt8,
    attributes String
) ENGINE = MergeTree()
ORDER BY (team_id, timestamp, model_name, status)
SETTINGS index_granularity = 8192;
