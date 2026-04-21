-- =============================================================================
-- Phase 6 — new 1m rollup tables + MVs (§B of the plan)
-- =============================================================================

-- B1. Logs volume / histogram / facets rollup
CREATE TABLE IF NOT EXISTS observability.logs_rollup_1m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    severity_text  LowCardinality(String),
    service        LowCardinality(String),
    host           LowCardinality(String),
    pod            LowCardinality(String),

    log_count      AggregateFunction(sum, UInt64),
    error_count    AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, severity_text, service, host, pod)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_to_rollup_1m
TO observability.logs_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                                     AS bucket_ts,
    severity_text                                                                                  AS severity_text,
    service                                                                                        AS service,
    host                                                                                           AS host,
    pod                                                                                            AS pod,
    sumState(toUInt64(1))                                                                          AS log_count,
    sumState(toUInt64(severity_text IN ('ERROR','FATAL','CRITICAL','SEVERE') OR severity_number >= 17)) AS error_count
FROM observability.logs;

-- B2. AI / LLM spans rollup
CREATE TABLE IF NOT EXISTS observability.ai_spans_rollup_1m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    ai_system      LowCardinality(String),
    ai_model       LowCardinality(String),
    ai_operation   LowCardinality(String),
    service_name   LowCardinality(String),

    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    input_tokens_sum   AggregateFunction(sum, UInt64),
    output_tokens_sum  AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, ai_system, ai_model, ai_operation, service_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.ai_spans_to_rollup_1m
TO observability.ai_spans_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                                 AS bucket_ts,
    attributes.`gen_ai.system`::String                                                         AS ai_system,
    coalesce(nullIf(attributes.`gen_ai.request.model`::String, ''),
             attributes.`gen_ai.response.model`::String)                                       AS ai_model,
    attributes.`gen_ai.operation.name`::String                                                 AS ai_operation,
    service_name                                                                               AS service_name,

    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_nano / 1000000.0, toUInt64(1))     AS latency_ms_digest,
    sumState(toUInt64(1))                                                                      AS request_count,
    sumState(toUInt64(has_error OR toUInt16OrZero(response_status_code) >= 500))               AS error_count,
    sumState(toUInt64OrZero(attributes.`gen_ai.usage.input_tokens`::String))                   AS input_tokens_sum,
    sumState(toUInt64OrZero(attributes.`gen_ai.usage.output_tokens`::String))                  AS output_tokens_sum
FROM observability.spans
WHERE attributes.`gen_ai.system`::String != '';

-- B3. Spans error fingerprint rollup
CREATE TABLE IF NOT EXISTS observability.spans_error_fingerprint_1m (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts            DateTime CODEC(DoubleDelta, LZ4),
    service_name         LowCardinality(String),
    operation_name       LowCardinality(String),
    exception_type       LowCardinality(String),
    status_message_hash  UInt64 CODEC(T64, ZSTD(1)),
    http_status_bucket   LowCardinality(String),

    error_count          AggregateFunction(sum, UInt64),
    sample_trace_id      AggregateFunction(any, String),
    sample_status_message AggregateFunction(any, String),
    first_seen           AggregateFunction(min, DateTime64(9)),
    last_seen            AggregateFunction(max, DateTime64(9))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, exception_type, status_message_hash, http_status_bucket)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_error_fingerprint_to_1m
TO observability.spans_error_fingerprint_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                                 AS bucket_ts,
    service_name                                                                               AS service_name,
    name                                                                                       AS operation_name,
    mat_exception_type                                                                         AS exception_type,
    cityHash64(status_message)                                                                 AS status_message_hash,
    multiIf(
        toUInt16OrZero(response_status_code) BETWEEN 400 AND 499, '4xx',
        toUInt16OrZero(response_status_code) >= 500, '5xx',
        has_error, 'err',
        'other'
    )                                                                                          AS http_status_bucket,

    sumState(toUInt64(1))                                                                      AS error_count,
    anyState(trace_id)                                                                         AS sample_trace_id,
    anyState(status_message)                                                                   AS sample_status_message,
    minState(timestamp)                                                                        AS first_seen,
    maxState(timestamp)                                                                        AS last_seen
FROM observability.spans
WHERE has_error = true OR toUInt16OrZero(response_status_code) >= 400;

-- B4. Spans host/pod rollup (all spans, not just root)
CREATE TABLE IF NOT EXISTS observability.spans_host_rollup_1m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    host_name       LowCardinality(String),
    pod_name        LowCardinality(String),
    service_name    LowCardinality(String),

    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    duration_ms_sum    AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, host_name, pod_name, service_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_host_to_rollup_1m
TO observability.spans_host_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                                 AS bucket_ts,
    mat_host_name                                                                              AS host_name,
    mat_k8s_pod_name                                                                           AS pod_name,
    service_name                                                                               AS service_name,

    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_nano / 1000000.0, toUInt64(1))     AS latency_ms_digest,
    sumState(toUInt64(1))                                                                      AS request_count,
    sumState(toUInt64(has_error OR toUInt16OrZero(response_status_code) >= 500))               AS error_count,
    sumState(duration_nano / 1000000.0)                                                        AS duration_ms_sum
FROM observability.spans
WHERE mat_host_name != '' OR mat_k8s_pod_name != '';

-- B5. Spans by deployment version rollup
CREATE TABLE IF NOT EXISTS observability.spans_by_version_1m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    service_name    LowCardinality(String),
    service_version LowCardinality(String),
    environment     LowCardinality(String),

    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    first_seen         AggregateFunction(min, DateTime64(9)),
    last_seen          AggregateFunction(max, DateTime64(9)),

    commit_sha         AggregateFunction(any, String),
    commit_author      AggregateFunction(any, String),
    repo_url           AggregateFunction(any, String),
    pr_url             AggregateFunction(any, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, service_version, environment)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_by_version_to_1m
TO observability.spans_by_version_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                                 AS bucket_ts,
    service_name                                                                               AS service_name,
    mat_service_version                                                                        AS service_version,
    mat_deployment_environment                                                                 AS environment,

    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_nano / 1000000.0, toUInt64(1))     AS latency_ms_digest,
    sumState(toUInt64(1))                                                                      AS request_count,
    sumState(toUInt64(has_error OR toUInt16OrZero(response_status_code) >= 500))               AS error_count,
    minState(timestamp)                                                                        AS first_seen,
    maxState(timestamp)                                                                        AS last_seen,

    anyState(attributes.`vcs.commit.sha`::String)                                              AS commit_sha,
    anyState(attributes.`vcs.commit.author`::String)                                           AS commit_author,
    anyState(attributes.`vcs.repository.url`::String)                                          AS repo_url,
    anyState(attributes.`vcs.pr.url`::String)                                                  AS pr_url
FROM observability.spans
WHERE mat_service_version != ''
  AND (parent_span_id = '' OR parent_span_id = '0000000000000000');


-- =============================================================================
-- Phase 6 — cascade tiers: 1m → 5m → 1h (§E of the plan)
-- =============================================================================
-- Every rollup gets _5m and _1h tables with the SAME column shape as _1m; cascade MVs
-- read FROM the finer tier using `-MergeState` combinators so AggregateFunction columns
-- round-trip without distribution-fidelity loss. Query layer uses rollup.TierTableFor
-- to pick the coarsest tier that keeps bucket count cheap for the requested range.

