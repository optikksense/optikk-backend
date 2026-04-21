CREATE DATABASE IF NOT EXISTS observability;

CREATE TABLE IF NOT EXISTS observability.spans (
    ts_bucket_start                       UInt64          CODEC(DoubleDelta, LZ4),
    team_id                               UInt32          CODEC(T64, ZSTD(1)),
    timestamp                             DateTime64(9)   CODEC(DoubleDelta, LZ4),
    trace_id                              String CODEC(ZSTD(1)),
    span_id                               String CODEC(ZSTD(1)),
    parent_span_id                        String CODEC(ZSTD(1)),
    trace_state                           String          CODEC(ZSTD(1)),
    flags                                 UInt32          CODEC(T64, ZSTD(1)),
    name                                  LowCardinality(String) CODEC(ZSTD(1)),
    kind                                  Int8            CODEC(T64, ZSTD(1)),
    kind_string                           LowCardinality(String) CODEC(ZSTD(1)),
    duration_nano                         UInt64          CODEC(T64, ZSTD(1)),
    has_error                             Bool            CODEC(T64, ZSTD(1)),
    is_remote                             Bool            CODEC(T64, ZSTD(1)),
    status_code                           Int16           CODEC(T64, ZSTD(1)),
    status_code_string                    LowCardinality(String) CODEC(ZSTD(1)),
    status_message                        String          CODEC(ZSTD(1)),
    http_url                              LowCardinality(String) CODEC(ZSTD(1)),
    http_method                           LowCardinality(String) CODEC(ZSTD(1)),
    http_host                             LowCardinality(String) CODEC(ZSTD(1)),
    external_http_url                     LowCardinality(String) CODEC(ZSTD(1)),
    external_http_method                  LowCardinality(String) CODEC(ZSTD(1)),
    response_status_code                  LowCardinality(String) CODEC(ZSTD(1)),
    attributes                            JSON(max_dynamic_paths = 50) CODEC(ZSTD(1)),
    events                                Array(String)   CODEC(ZSTD(2)),
    links                                 String          CODEC(ZSTD(1)),
    exception_type                        LowCardinality(String) CODEC(ZSTD(1)),
    exception_message                     String          CODEC(ZSTD(1)),
    exception_stacktrace                  String          CODEC(ZSTD(1)),
    exception_escaped                     Bool            CODEC(T64, ZSTD(1)),
    service_name                          LowCardinality(String)
                                    MATERIALIZED attributes.`service.name`::String CODEC(ZSTD(1)),
    operation_name                        LowCardinality(String)
                                    ALIAS name,
    start_time                            DateTime64(9)
                                    ALIAS timestamp,
    duration_ms                           Float64
                                    ALIAS duration_nano / 1000000.0,
    status                                LowCardinality(String)
                                    ALIAS status_code_string,
    http_status_code                      UInt16
                                    ALIAS toUInt16OrZero(response_status_code),
    is_root                               UInt8
                                    ALIAS if((parent_span_id = '') OR (parent_span_id = '0000000000000000'), 1, 0),
    parent_service_name                   LowCardinality(String)
                                    ALIAS '',
    peer_address                          LowCardinality(String)
                                    ALIAS CAST(attributes.`peer.address`, 'String'),
    mat_http_route            LowCardinality(String)
                                    MATERIALIZED attributes.`http.route`::String               CODEC(ZSTD(1)),
    mat_http_status_code      LowCardinality(String)
                                    MATERIALIZED attributes.`http.status_code`::String         CODEC(ZSTD(1)),
    mat_http_target           LowCardinality(String)
                                    MATERIALIZED attributes.`http.target`::String              CODEC(ZSTD(1)),
    mat_http_scheme           LowCardinality(String)
                                    MATERIALIZED attributes.`http.scheme`::String              CODEC(ZSTD(1)),
    mat_db_system             LowCardinality(String)
                                    MATERIALIZED attributes.`db.system`::String                CODEC(ZSTD(1)),
    mat_db_name               LowCardinality(String)
                                    MATERIALIZED attributes.`db.name`::String                  CODEC(ZSTD(1)),
    mat_db_operation          LowCardinality(String)
                                    MATERIALIZED attributes.`db.operation`::String             CODEC(ZSTD(1)),
    mat_db_statement          String
                                    MATERIALIZED attributes.`db.statement`::String             CODEC(ZSTD(1)),
    mat_rpc_system            LowCardinality(String)
                                    MATERIALIZED attributes.`rpc.system`::String               CODEC(ZSTD(1)),
    mat_rpc_service           LowCardinality(String)
                                    MATERIALIZED attributes.`rpc.service`::String              CODEC(ZSTD(1)),
    mat_rpc_method            LowCardinality(String)
                                    MATERIALIZED attributes.`rpc.method`::String               CODEC(ZSTD(1)),
    mat_rpc_grpc_status_code  LowCardinality(String)
                                    MATERIALIZED attributes.`rpc.grpc.status_code`::String     CODEC(ZSTD(1)),
    mat_messaging_system      LowCardinality(String)
                                    MATERIALIZED attributes.`messaging.system`::String         CODEC(ZSTD(1)),
    mat_messaging_operation   LowCardinality(String)
                                    MATERIALIZED attributes.`messaging.operation`::String      CODEC(ZSTD(1)),
    mat_messaging_destination LowCardinality(String)
                                    MATERIALIZED attributes.`messaging.destination`::String    CODEC(ZSTD(1)),
    mat_peer_service          LowCardinality(String)
                                    MATERIALIZED attributes.`peer.service`::String             CODEC(ZSTD(1)),
    mat_net_peer_name         LowCardinality(String)
                                    MATERIALIZED attributes.`net.peer.name`::String            CODEC(ZSTD(1)),
    mat_net_peer_port         LowCardinality(String)
                                    MATERIALIZED attributes.`net.peer.port`::String            CODEC(ZSTD(1)),
    mat_exception_type        LowCardinality(String)
                                    MATERIALIZED attributes.`exception.type`::String           CODEC(ZSTD(1)),
    mat_host_name             LowCardinality(String)
                                    MATERIALIZED attributes.`host.name`::String                CODEC(ZSTD(1)),
    mat_k8s_pod_name          LowCardinality(String)
                                    MATERIALIZED attributes.`k8s.pod.name`::String             CODEC(ZSTD(1)),
    mat_service_version       LowCardinality(String)
                                    MATERIALIZED attributes.`service.version`::String          CODEC(ZSTD(1)),
    mat_deployment_environment LowCardinality(String)
                                    MATERIALIZED attributes.`deployment.environment`::String    CODEC(ZSTD(1)),
    INDEX idx_service_name          service_name            TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_trace_id              trace_id                TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_span_id               span_id                 TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_span_name             name                    TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_http_route        mat_http_route          TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_http_status_code  mat_http_status_code    TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_db_system         mat_db_system           TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_db_name           mat_db_name             TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_rpc_service       mat_rpc_service         TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_peer_service      mat_peer_service        TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_exception_type    mat_exception_type      TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_host_name         mat_host_name           TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_k8s_pod_name      mat_k8s_pod_name        TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_service_version        mat_service_version        TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_deployment_environment mat_deployment_environment TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, ts_bucket_start, service_name, name, timestamp)
TTL timestamp + INTERVAL 1 HOUR DELETE
SETTINGS
    index_granularity = 8192,
    non_replicated_deduplication_window = 1000;

CREATE TABLE IF NOT EXISTS observability.logs (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket_start      UInt32 CODEC(Delta(4), LZ4),
    timestamp            DateTime64(9) CODEC(DoubleDelta, LZ4),
    observed_timestamp   UInt64 CODEC(DoubleDelta, LZ4),
    trace_id             String CODEC(ZSTD(1)),
    span_id              String CODEC(ZSTD(1)),
    trace_flags          UInt32 DEFAULT 0,
    severity_text        LowCardinality(String) CODEC(ZSTD(1)),
    severity_number      UInt8 DEFAULT 0,
    body                 String CODEC(ZSTD(2)),
    attributes_string    Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    attributes_number    Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    attributes_bool      Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),
    resource             JSON(max_dynamic_paths=100) CODEC(ZSTD(1)),
    resource_fingerprint String CODEC(ZSTD(1)),
    scope_name           String CODEC(ZSTD(1)),
    scope_version        String CODEC(ZSTD(1)),
    scope_string         Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    service              LowCardinality(String) MATERIALIZED resource.`service.name`::String,
    host                 LowCardinality(String) MATERIALIZED resource.`host.name`::String,
    pod                  LowCardinality(String) MATERIALIZED resource.`k8s.pod.name`::String,
    container            LowCardinality(String) MATERIALIZED resource.`k8s.container.name`::String,
    environment          LowCardinality(String) MATERIALIZED resource.`deployment.environment`::String,
    INDEX idx_trace_id   trace_id        TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_span_id    span_id         TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_body       body            TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1,
    INDEX idx_severity   severity_text   TYPE set(10) GRANULARITY 1,
    INDEX idx_service    service         TYPE set(200) GRANULARITY 1,
    INDEX idx_host       host            TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime(ts_bucket_start))
ORDER BY (team_id, ts_bucket_start, service, timestamp)
TTL timestamp + INTERVAL 1 HOUR DELETE
SETTINGS
    index_granularity = 8192,
    non_replicated_deduplication_window = 1000;

CREATE TABLE IF NOT EXISTS observability.metrics (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    env                  LowCardinality(String) DEFAULT 'default',
    metric_name          LowCardinality(String),
    metric_type          LowCardinality(String),
    temporality          LowCardinality(String) DEFAULT 'Unspecified',
    is_monotonic         Bool CODEC(T64, ZSTD(1)),
    unit                 LowCardinality(String) DEFAULT '',
    description          LowCardinality(String) DEFAULT '',
    resource_fingerprint UInt64 CODEC(ZSTD(3)),
    timestamp            DateTime64(3) CODEC(DoubleDelta, LZ4),
    value                Float64 CODEC(Gorilla, ZSTD(1)),
    hist_sum             Float64 CODEC(Gorilla, ZSTD(1)),
    hist_count           UInt64 CODEC(T64, ZSTD(1)),
    hist_buckets         Array(Float64) CODEC(ZSTD(1)),
    hist_counts          Array(UInt64) CODEC(T64, ZSTD(1)),
    attributes           JSON(max_dynamic_paths=100) CODEC(ZSTD(1)),
    service              LowCardinality(String) MATERIALIZED attributes.`service.name`::String,
    host                 LowCardinality(String) MATERIALIZED attributes.`host.name`::String,
    environment          LowCardinality(String) MATERIALIZED attributes.`deployment.environment`::String,
    k8s_namespace        LowCardinality(String) MATERIALIZED attributes.`k8s.namespace.name`::String,
    http_method          LowCardinality(String) MATERIALIZED attributes.`http.method`::String,
    http_status_code     UInt16                 MATERIALIZED attributes.`http.status_code`::UInt16,
    has_error            Bool                   MATERIALIZED attributes.`error`::Bool,
    INDEX idx_service          service              TYPE set(200)      GRANULARITY 1,
    INDEX idx_host             host                 TYPE bloom_filter  GRANULARITY 1,
    INDEX idx_environment      environment          TYPE set(10)       GRANULARITY 1,
    INDEX idx_k8s_namespace    k8s_namespace        TYPE set(100)      GRANULARITY 1,
    INDEX idx_http_method      http_method          TYPE set(20)       GRANULARITY 1,
    INDEX idx_http_status_code http_status_code     TYPE minmax        GRANULARITY 1,
    INDEX idx_has_error        has_error            TYPE set(2)        GRANULARITY 1,
    INDEX idx_fingerprint      resource_fingerprint TYPE bloom_filter  GRANULARITY 4
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, metric_name, service, environment, temporality, timestamp, resource_fingerprint)
TTL timestamp + INTERVAL 1 HOUR DELETE
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    non_replicated_deduplication_window = 1000;

CREATE TABLE IF NOT EXISTS observability.alert_events (
    ts             DateTime64(3) CODEC(DoubleDelta, LZ4),
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    alert_id       Int64 CODEC(T64, ZSTD(1)),
    instance_key   String CODEC(ZSTD(1)),
    kind           LowCardinality(String),
    from_state     LowCardinality(String),
    to_state       LowCardinality(String),
    values         String CODEC(ZSTD(1)),
    actor_user_id  Int64 CODEC(T64, ZSTD(1)),
    message        String CODEC(ZSTD(1)),
    deploy_refs    String CODEC(ZSTD(1)),
    transition_id  Int64 CODEC(T64, ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (team_id, alert_id, ts)
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------------------
-- Phase 5 — AggregatingMergeTree rollups for Datadog-pattern pre-aggregation.
--
-- Rationale: the overview dashboards need p50/p95/p99 + counters over ranges
-- that scan millions of raw span/metric rows. Pre-aggregated t-digest state
-- per (team_id, 1-min bucket, dims) lets queries scan thousands of rollup rows
-- instead. `quantileTDigestMerge(state)` and `sumMerge(state)` do server-side
-- merges at read time. No Go-side sketch pipeline needed; MVs keep everything
-- current on every INSERT.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS observability.spans_rollup_1m (
    team_id          UInt32            CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime          CODEC(DoubleDelta, LZ4),

    -- grouping dims for the overview dashboard
    service_name     LowCardinality(String),
    operation_name   LowCardinality(String),
    endpoint         LowCardinality(String),   -- coalesced route/target/name at MV time
    http_method      LowCardinality(String),

    -- t-digest state (same algorithm class as DDSketch; CH native)
    latency_ms_digest AggregateFunction(
        quantilesTDigestWeighted(0.5, 0.95, 0.99),
        Float64, UInt64
    ),

    -- sum state for rates, error rates, averages
    request_count    AggregateFunction(sum, UInt64),
    error_count      AggregateFunction(sum, UInt64),
    duration_ms_sum  AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, endpoint, http_method)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_to_rollup_1m
TO observability.spans_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                         AS bucket_ts,
    service_name                                                                        AS service_name,
    name                                                                                AS operation_name,
    coalesce(nullIf(mat_http_route, ''), nullIf(mat_http_target, ''), name)             AS endpoint,
    http_method                                                                         AS http_method,

    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        duration_nano / 1000000.0, toUInt64(1)
    )                                                                                   AS latency_ms_digest,

    sumState(toUInt64(1))                                                               AS request_count,
    sumState(toUInt64(has_error OR toUInt16OrZero(response_status_code) >= 500))        AS error_count,
    sumState(duration_nano / 1000000.0)                                                 AS duration_ms_sum
FROM observability.spans
WHERE (parent_span_id = '' OR parent_span_id = '0000000000000000');

CREATE TABLE IF NOT EXISTS observability.metrics_histograms_rollup_1m (
    team_id      UInt32           CODEC(T64, ZSTD(1)),
    bucket_ts    DateTime         CODEC(DoubleDelta, LZ4),
    metric_name  LowCardinality(String),
    service      LowCardinality(String),

    -- t-digest state built from histogram rows (weight = hist_count, value = avg per row).
    latency_ms_digest AggregateFunction(
        quantilesTDigestWeighted(0.5, 0.95, 0.99),
        Float64, UInt64
    ),
    hist_count   AggregateFunction(sum, UInt64),
    hist_sum     AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_histograms_to_rollup_1m
TO observability.metrics_histograms_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                          AS bucket_ts,
    metric_name                                                                         AS metric_name,
    service                                                                             AS service,

    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        hist_sum / if(hist_count = 0, 1, hist_count),
        hist_count
    )                                                                                   AS latency_ms_digest,

    sumState(hist_count)                                                                AS hist_count,
    sumState(hist_sum)                                                                  AS hist_sum
FROM observability.metrics
WHERE metric_type = 'Histogram' AND hist_count > 0;

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

-- spans_rollup_5m
CREATE TABLE IF NOT EXISTS observability.spans_rollup_5m (
    team_id          UInt32            CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime          CODEC(DoubleDelta, LZ4),
    service_name     LowCardinality(String),
    operation_name   LowCardinality(String),
    endpoint         LowCardinality(String),
    http_method      LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count    AggregateFunction(sum, UInt64),
    error_count      AggregateFunction(sum, UInt64),
    duration_ms_sum  AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, endpoint, http_method)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_rollup_1m_to_5m
TO observability.spans_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       service_name, operation_name, endpoint, http_method,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)   AS request_count,
       sumMergeState(error_count)     AS error_count,
       sumMergeState(duration_ms_sum) AS duration_ms_sum
FROM observability.spans_rollup_1m
GROUP BY team_id, bucket_ts, service_name, operation_name, endpoint, http_method;

CREATE TABLE IF NOT EXISTS observability.spans_rollup_1h (
    team_id          UInt32            CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime          CODEC(DoubleDelta, LZ4),
    service_name     LowCardinality(String),
    operation_name   LowCardinality(String),
    endpoint         LowCardinality(String),
    http_method      LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count    AggregateFunction(sum, UInt64),
    error_count      AggregateFunction(sum, UInt64),
    duration_ms_sum  AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service_name, operation_name, endpoint, http_method)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_rollup_5m_to_1h
TO observability.spans_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       service_name, operation_name, endpoint, http_method,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)   AS request_count,
       sumMergeState(error_count)     AS error_count,
       sumMergeState(duration_ms_sum) AS duration_ms_sum
FROM observability.spans_rollup_5m
GROUP BY team_id, bucket_ts, service_name, operation_name, endpoint, http_method;

-- metrics_histograms_rollup_5m + _1h
CREATE TABLE IF NOT EXISTS observability.metrics_histograms_rollup_5m (
    team_id      UInt32           CODEC(T64, ZSTD(1)),
    bucket_ts    DateTime         CODEC(DoubleDelta, LZ4),
    metric_name  LowCardinality(String),
    service      LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count   AggregateFunction(sum, UInt64),
    hist_sum     AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_histograms_rollup_1m_to_5m
TO observability.metrics_histograms_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.metrics_histograms_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service;

CREATE TABLE IF NOT EXISTS observability.metrics_histograms_rollup_1h (
    team_id      UInt32           CODEC(T64, ZSTD(1)),
    bucket_ts    DateTime         CODEC(DoubleDelta, LZ4),
    metric_name  LowCardinality(String),
    service      LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count   AggregateFunction(sum, UInt64),
    hist_sum     AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_histograms_rollup_5m_to_1h
TO observability.metrics_histograms_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.metrics_histograms_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service;

-- logs_rollup_5m + _1h
CREATE TABLE IF NOT EXISTS observability.logs_rollup_5m (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    severity_text LowCardinality(String),
    service       LowCardinality(String),
    host          LowCardinality(String),
    pod           LowCardinality(String),
    log_count     AggregateFunction(sum, UInt64),
    error_count   AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, severity_text, service, host, pod)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_rollup_1m_to_5m
TO observability.logs_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       severity_text, service, host, pod,
       sumMergeState(log_count)   AS log_count,
       sumMergeState(error_count) AS error_count
FROM observability.logs_rollup_1m
GROUP BY team_id, bucket_ts, severity_text, service, host, pod;

CREATE TABLE IF NOT EXISTS observability.logs_rollup_1h (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    severity_text LowCardinality(String),
    service       LowCardinality(String),
    host          LowCardinality(String),
    pod           LowCardinality(String),
    log_count     AggregateFunction(sum, UInt64),
    error_count   AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, severity_text, service, host, pod)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_rollup_5m_to_1h
TO observability.logs_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       severity_text, service, host, pod,
       sumMergeState(log_count)   AS log_count,
       sumMergeState(error_count) AS error_count
FROM observability.logs_rollup_5m
GROUP BY team_id, bucket_ts, severity_text, service, host, pod;

-- ai_spans_rollup_5m + _1h
CREATE TABLE IF NOT EXISTS observability.ai_spans_rollup_5m (
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

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.ai_spans_rollup_1m_to_5m
TO observability.ai_spans_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       ai_system, ai_model, ai_operation, service_name,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)     AS request_count,
       sumMergeState(error_count)       AS error_count,
       sumMergeState(input_tokens_sum)  AS input_tokens_sum,
       sumMergeState(output_tokens_sum) AS output_tokens_sum
FROM observability.ai_spans_rollup_1m
GROUP BY team_id, bucket_ts, ai_system, ai_model, ai_operation, service_name;

CREATE TABLE IF NOT EXISTS observability.ai_spans_rollup_1h (
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

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.ai_spans_rollup_5m_to_1h
TO observability.ai_spans_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       ai_system, ai_model, ai_operation, service_name,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)     AS request_count,
       sumMergeState(error_count)       AS error_count,
       sumMergeState(input_tokens_sum)  AS input_tokens_sum,
       sumMergeState(output_tokens_sum) AS output_tokens_sum
FROM observability.ai_spans_rollup_5m
GROUP BY team_id, bucket_ts, ai_system, ai_model, ai_operation, service_name;

-- spans_error_fingerprint_5m + _1h
CREATE TABLE IF NOT EXISTS observability.spans_error_fingerprint_5m (
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

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_error_fingerprint_1m_to_5m
TO observability.spans_error_fingerprint_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       service_name, operation_name, exception_type, status_message_hash, http_status_bucket,
       sumMergeState(error_count)                AS error_count,
       anyMergeState(sample_trace_id)            AS sample_trace_id,
       anyMergeState(sample_status_message)      AS sample_status_message,
       minMergeState(first_seen)                 AS first_seen,
       maxMergeState(last_seen)                  AS last_seen
FROM observability.spans_error_fingerprint_1m
GROUP BY team_id, bucket_ts, service_name, operation_name, exception_type, status_message_hash, http_status_bucket;

CREATE TABLE IF NOT EXISTS observability.spans_error_fingerprint_1h (
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

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_error_fingerprint_5m_to_1h
TO observability.spans_error_fingerprint_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       service_name, operation_name, exception_type, status_message_hash, http_status_bucket,
       sumMergeState(error_count)                AS error_count,
       anyMergeState(sample_trace_id)            AS sample_trace_id,
       anyMergeState(sample_status_message)      AS sample_status_message,
       minMergeState(first_seen)                 AS first_seen,
       maxMergeState(last_seen)                  AS last_seen
FROM observability.spans_error_fingerprint_5m
GROUP BY team_id, bucket_ts, service_name, operation_name, exception_type, status_message_hash, http_status_bucket;

-- spans_host_rollup_5m + _1h
CREATE TABLE IF NOT EXISTS observability.spans_host_rollup_5m (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    host_name     LowCardinality(String),
    pod_name      LowCardinality(String),
    service_name  LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    duration_ms_sum    AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, host_name, pod_name, service_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_host_rollup_1m_to_5m
TO observability.spans_host_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       host_name, pod_name, service_name,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)   AS request_count,
       sumMergeState(error_count)     AS error_count,
       sumMergeState(duration_ms_sum) AS duration_ms_sum
FROM observability.spans_host_rollup_1m
GROUP BY team_id, bucket_ts, host_name, pod_name, service_name;

CREATE TABLE IF NOT EXISTS observability.spans_host_rollup_1h (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    host_name     LowCardinality(String),
    pod_name      LowCardinality(String),
    service_name  LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    duration_ms_sum    AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, host_name, pod_name, service_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_host_rollup_5m_to_1h
TO observability.spans_host_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       host_name, pod_name, service_name,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)   AS request_count,
       sumMergeState(error_count)     AS error_count,
       sumMergeState(duration_ms_sum) AS duration_ms_sum
FROM observability.spans_host_rollup_5m
GROUP BY team_id, bucket_ts, host_name, pod_name, service_name;

-- spans_by_version_5m + _1h
CREATE TABLE IF NOT EXISTS observability.spans_by_version_5m (
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

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_by_version_1m_to_5m
TO observability.spans_by_version_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       service_name, service_version, environment,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)  AS request_count,
       sumMergeState(error_count)    AS error_count,
       minMergeState(first_seen)     AS first_seen,
       maxMergeState(last_seen)      AS last_seen,
       anyMergeState(commit_sha)     AS commit_sha,
       anyMergeState(commit_author)  AS commit_author,
       anyMergeState(repo_url)       AS repo_url,
       anyMergeState(pr_url)         AS pr_url
FROM observability.spans_by_version_1m
GROUP BY team_id, bucket_ts, service_name, service_version, environment;

CREATE TABLE IF NOT EXISTS observability.spans_by_version_1h (
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

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_by_version_5m_to_1h
TO observability.spans_by_version_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       service_name, service_version, environment,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)  AS request_count,
       sumMergeState(error_count)    AS error_count,
       minMergeState(first_seen)     AS first_seen,
       maxMergeState(last_seen)      AS last_seen,
       anyMergeState(commit_sha)     AS commit_sha,
       anyMergeState(commit_author)  AS commit_author,
       anyMergeState(repo_url)       AS repo_url,
       anyMergeState(pr_url)         AS pr_url
FROM observability.spans_by_version_5m
GROUP BY team_id, bucket_ts, service_name, service_version, environment;


-- =============================================================================
-- Phase 7 — finish-the-job rollups: gauges, DB, messaging, topology
-- =============================================================================
-- Closes the remaining raw-scan aggregate gaps: infrastructure gauges
-- (cpu/mem/disk/network/jvm/k8s/connpool), apm process metrics + httpmetrics
-- gauges, saturation DB per-domain histogram breakdowns, saturation Kafka
-- messaging, and service-to-service topology edges. Each base table gets _5m
-- and _1h cascade tiers.

-- ---------------------------------------------------------------------------
-- A1. metrics_gauges_rollup — generic gauge + counter metric rollup.
--     Covers infrastructure/{cpu,disk,memory,network,jvm,kubernetes,connpool}
--     + overview/apm (Uptime/OpenFDs/ProcessCPU/ProcessMemory/ActiveRequests)
--     + overview/httpmetrics (ActiveRequests).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS observability.metrics_gauges_rollup_1m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    metric_name    LowCardinality(String),
    service        LowCardinality(String),
    host           LowCardinality(String),
    pod            LowCardinality(String),
    state_dim      LowCardinality(String),

    value_sum      AggregateFunction(sum, Float64),
    value_avg_num  AggregateFunction(sum, Float64),
    sample_count   AggregateFunction(sum, UInt64),
    value_max      AggregateFunction(max, Float64),
    value_min      AggregateFunction(min, Float64),
    value_last     AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, host, pod, state_dim)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_to_rollup_1m
TO observability.metrics_gauges_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                   AS bucket_ts,
    metric_name                                                                  AS metric_name,
    service                                                                      AS service,
    host                                                                         AS host,
    attributes.`k8s.pod.name`::String                                            AS pod,
    multiIf(
        metric_name IN ('system.cpu.time','system.cpu.utilization'),
            attributes.`system.cpu.state`::String,
        metric_name = 'process.cpu.time',
            attributes.`process.cpu.state`::String,
        metric_name IN ('system.memory.usage','system.memory.utilization'),
            attributes.`system.memory.state`::String,
        metric_name IN ('system.disk.io','system.disk.operations'),
            attributes.`system.disk.direction`::String,
        metric_name IN ('system.network.io','system.network.packets','system.network.errors','system.network.connections'),
            attributes.`system.network.direction`::String,
        metric_name IN ('jvm.memory.used','jvm.memory.committed','jvm.memory.limit','jvm.memory.used_after_last_gc'),
            attributes.`jvm.memory.pool.name`::String,
        metric_name = 'jvm.gc.duration',
            attributes.`jvm.gc.name`::String,
        ''
    )                                                                            AS state_dim,

    sumState(value)                                                              AS value_sum,
    sumState(value)                                                              AS value_avg_num,
    sumState(toUInt64(1))                                                        AS sample_count,
    maxState(value)                                                              AS value_max,
    minState(value)                                                              AS value_min,
    argMaxState(value, timestamp)                                                AS value_last
FROM observability.metrics
WHERE metric_type IN ('Gauge','Sum') AND hist_count = 0;

-- ---------------------------------------------------------------------------
-- A2. metrics_gauges_by_status_rollup — HTTP request rate by status.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS observability.metrics_gauges_by_status_rollup_1m (
    team_id          UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime CODEC(DoubleDelta, LZ4),
    metric_name      LowCardinality(String),
    service          LowCardinality(String),
    http_status_code LowCardinality(String),
    sample_count     AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, http_status_code)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_by_status_to_rollup_1m
TO observability.metrics_gauges_by_status_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                    AS bucket_ts,
    metric_name                                                   AS metric_name,
    service                                                       AS service,
    attributes.`http.status_code`::String                         AS http_status_code,
    sumState(toUInt64(1))                                         AS sample_count
FROM observability.metrics
WHERE metric_name IN ('http.server.request.duration','http.server.request.size','http.client.request.duration');

-- ---------------------------------------------------------------------------
-- A3. db_histograms_rollup — saturation/database histogram-metric rollup
--     keyed by db.* attributes.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS observability.db_histograms_rollup_1m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    metric_name     LowCardinality(String),
    service         LowCardinality(String),
    db_system       LowCardinality(String),
    db_operation    LowCardinality(String),
    db_collection   LowCardinality(String),
    db_namespace    LowCardinality(String),
    pool_name       LowCardinality(String),
    error_type      LowCardinality(String),
    server_address  LowCardinality(String),

    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_histograms_to_rollup_1m
TO observability.db_histograms_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                             AS bucket_ts,
    metric_name                                                                            AS metric_name,
    service                                                                                AS service,
    attributes.`db.system`::String                                                         AS db_system,
    attributes.`db.operation`::String                                                      AS db_operation,
    attributes.`db.collection.name`::String                                                AS db_collection,
    attributes.`db.namespace`::String                                                      AS db_namespace,
    attributes.`pool.name`::String                                                         AS pool_name,
    attributes.`error.type`::String                                                        AS error_type,
    attributes.`server.address`::String                                                    AS server_address,

    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        hist_sum / if(hist_count = 0, 1, hist_count),
        hist_count
    )                                                                                      AS latency_ms_digest,
    sumState(hist_count)                                                                   AS hist_count,
    sumState(hist_sum)                                                                     AS hist_sum
FROM observability.metrics
WHERE metric_type = 'Histogram'
  AND hist_count > 0
  AND (metric_name LIKE 'db.%' OR metric_name LIKE 'pool.%');

-- ---------------------------------------------------------------------------
-- A4. messaging_histograms_rollup — saturation/kafka rollup.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS observability.messaging_histograms_rollup_1m (
    team_id               UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts             DateTime CODEC(DoubleDelta, LZ4),
    metric_name           LowCardinality(String),
    service               LowCardinality(String),
    messaging_system      LowCardinality(String),
    messaging_destination LowCardinality(String),
    messaging_operation   LowCardinality(String),
    consumer_group        LowCardinality(String),

    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_histograms_to_rollup_1m
TO observability.messaging_histograms_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                             AS bucket_ts,
    metric_name                                                                            AS metric_name,
    service                                                                                AS service,
    attributes.`messaging.system`::String                                                  AS messaging_system,
    attributes.`messaging.destination.name`::String                                        AS messaging_destination,
    attributes.`messaging.operation`::String                                               AS messaging_operation,
    attributes.`messaging.kafka.consumer.group`::String                                    AS consumer_group,

    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        hist_sum / if(hist_count = 0, 1, hist_count),
        hist_count
    )                                                                                      AS latency_ms_digest,
    sumState(hist_count)                                                                   AS hist_count,
    sumState(hist_sum)                                                                     AS hist_sum
FROM observability.metrics
WHERE metric_type = 'Histogram'
  AND hist_count > 0
  AND metric_name LIKE 'messaging.%';

-- ---------------------------------------------------------------------------
-- A5. spans_topology_rollup — service-to-service edges from CLIENT spans.
--     Uses peer.service attribute (set by OTel SDKs on CLIENT spans) as the
--     edge target; service_name as source. Single-pass, no self-join.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS observability.spans_topology_rollup_1m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    client_service  LowCardinality(String),
    server_service  LowCardinality(String),
    operation       LowCardinality(String),

    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, client_service, server_service, operation)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_topology_to_rollup_1m
TO observability.spans_topology_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                             AS bucket_ts,
    service_name                                                                           AS client_service,
    mat_peer_service                                                                       AS server_service,
    name                                                                                   AS operation,

    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_nano / 1000000.0, toUInt64(1)) AS latency_ms_digest,
    sumState(toUInt64(1))                                                                  AS request_count,
    sumState(toUInt64(has_error OR toUInt16OrZero(response_status_code) >= 500))           AS error_count
FROM observability.spans
WHERE kind = 3 AND mat_peer_service != ''; -- SPAN_KIND_CLIENT = 3

-- =============================================================================
-- Phase 7 cascade tiers — _5m + _1h for all 5 new rollups
-- =============================================================================

-- metrics_gauges_rollup 5m/1h
CREATE TABLE IF NOT EXISTS observability.metrics_gauges_rollup_5m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    metric_name    LowCardinality(String),
    service        LowCardinality(String),
    host           LowCardinality(String),
    pod            LowCardinality(String),
    state_dim      LowCardinality(String),
    value_sum      AggregateFunction(sum, Float64),
    value_avg_num  AggregateFunction(sum, Float64),
    sample_count   AggregateFunction(sum, UInt64),
    value_max      AggregateFunction(max, Float64),
    value_min      AggregateFunction(min, Float64),
    value_last     AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, host, pod, state_dim)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_rollup_1m_to_5m
TO observability.metrics_gauges_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service, host, pod, state_dim,
       sumMergeState(value_sum)       AS value_sum,
       sumMergeState(value_avg_num)   AS value_avg_num,
       sumMergeState(sample_count)    AS sample_count,
       maxMergeState(value_max)       AS value_max,
       minMergeState(value_min)       AS value_min,
       argMaxMergeState(value_last)   AS value_last
FROM observability.metrics_gauges_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service, host, pod, state_dim;

CREATE TABLE IF NOT EXISTS observability.metrics_gauges_rollup_1h (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    metric_name    LowCardinality(String),
    service        LowCardinality(String),
    host           LowCardinality(String),
    pod            LowCardinality(String),
    state_dim      LowCardinality(String),
    value_sum      AggregateFunction(sum, Float64),
    value_avg_num  AggregateFunction(sum, Float64),
    sample_count   AggregateFunction(sum, UInt64),
    value_max      AggregateFunction(max, Float64),
    value_min      AggregateFunction(min, Float64),
    value_last     AggregateFunction(argMax, Float64, DateTime64(3))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, host, pod, state_dim)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_rollup_5m_to_1h
TO observability.metrics_gauges_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service, host, pod, state_dim,
       sumMergeState(value_sum)       AS value_sum,
       sumMergeState(value_avg_num)   AS value_avg_num,
       sumMergeState(sample_count)    AS sample_count,
       maxMergeState(value_max)       AS value_max,
       minMergeState(value_min)       AS value_min,
       argMaxMergeState(value_last)   AS value_last
FROM observability.metrics_gauges_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service, host, pod, state_dim;

-- metrics_gauges_by_status_rollup 5m/1h
CREATE TABLE IF NOT EXISTS observability.metrics_gauges_by_status_rollup_5m (
    team_id          UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime CODEC(DoubleDelta, LZ4),
    metric_name      LowCardinality(String),
    service          LowCardinality(String),
    http_status_code LowCardinality(String),
    sample_count     AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, http_status_code)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_by_status_rollup_1m_to_5m
TO observability.metrics_gauges_by_status_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service, http_status_code,
       sumMergeState(sample_count) AS sample_count
FROM observability.metrics_gauges_by_status_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service, http_status_code;

CREATE TABLE IF NOT EXISTS observability.metrics_gauges_by_status_rollup_1h (
    team_id          UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts        DateTime CODEC(DoubleDelta, LZ4),
    metric_name      LowCardinality(String),
    service          LowCardinality(String),
    http_status_code LowCardinality(String),
    sample_count     AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, http_status_code)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_gauges_by_status_rollup_5m_to_1h
TO observability.metrics_gauges_by_status_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service, http_status_code,
       sumMergeState(sample_count) AS sample_count
FROM observability.metrics_gauges_by_status_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service, http_status_code;

-- db_histograms_rollup 5m/1h
CREATE TABLE IF NOT EXISTS observability.db_histograms_rollup_5m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    metric_name     LowCardinality(String),
    service         LowCardinality(String),
    db_system       LowCardinality(String),
    db_operation    LowCardinality(String),
    db_collection   LowCardinality(String),
    db_namespace    LowCardinality(String),
    pool_name       LowCardinality(String),
    error_type      LowCardinality(String),
    server_address  LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_histograms_rollup_1m_to_5m
TO observability.db_histograms_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.db_histograms_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address;

CREATE TABLE IF NOT EXISTS observability.db_histograms_rollup_1h (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    metric_name     LowCardinality(String),
    service         LowCardinality(String),
    db_system       LowCardinality(String),
    db_operation    LowCardinality(String),
    db_collection   LowCardinality(String),
    db_namespace    LowCardinality(String),
    pool_name       LowCardinality(String),
    error_type      LowCardinality(String),
    server_address  LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.db_histograms_rollup_5m_to_1h
TO observability.db_histograms_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.db_histograms_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service, db_system, db_operation, db_collection, db_namespace, pool_name, error_type, server_address;

-- messaging_histograms_rollup 5m/1h
CREATE TABLE IF NOT EXISTS observability.messaging_histograms_rollup_5m (
    team_id               UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts             DateTime CODEC(DoubleDelta, LZ4),
    metric_name           LowCardinality(String),
    service               LowCardinality(String),
    messaging_system      LowCardinality(String),
    messaging_destination LowCardinality(String),
    messaging_operation   LowCardinality(String),
    consumer_group        LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_histograms_rollup_1m_to_5m
TO observability.messaging_histograms_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.messaging_histograms_rollup_1m
GROUP BY team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group;

CREATE TABLE IF NOT EXISTS observability.messaging_histograms_rollup_1h (
    team_id               UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts             DateTime CODEC(DoubleDelta, LZ4),
    metric_name           LowCardinality(String),
    service               LowCardinality(String),
    messaging_system      LowCardinality(String),
    messaging_destination LowCardinality(String),
    messaging_operation   LowCardinality(String),
    consumer_group        LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    hist_count        AggregateFunction(sum, UInt64),
    hist_sum          AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.messaging_histograms_rollup_5m_to_1h
TO observability.messaging_histograms_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(hist_count) AS hist_count,
       sumMergeState(hist_sum)   AS hist_sum
FROM observability.messaging_histograms_rollup_5m
GROUP BY team_id, bucket_ts, metric_name, service, messaging_system, messaging_destination, messaging_operation, consumer_group;

-- spans_topology_rollup 5m/1h
CREATE TABLE IF NOT EXISTS observability.spans_topology_rollup_5m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    client_service  LowCardinality(String),
    server_service  LowCardinality(String),
    operation       LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, client_service, server_service, operation)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_topology_rollup_1m_to_5m
TO observability.spans_topology_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       client_service, server_service, operation,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count) AS request_count,
       sumMergeState(error_count)   AS error_count
FROM observability.spans_topology_rollup_1m
GROUP BY team_id, bucket_ts, client_service, server_service, operation;

CREATE TABLE IF NOT EXISTS observability.spans_topology_rollup_1h (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    client_service  LowCardinality(String),
    server_service  LowCardinality(String),
    operation       LowCardinality(String),
    latency_ms_digest AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count     AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, client_service, server_service, operation)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_topology_rollup_5m_to_1h
TO observability.spans_topology_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       client_service, server_service, operation,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count) AS request_count,
       sumMergeState(error_count)   AS error_count
FROM observability.spans_topology_rollup_5m
GROUP BY team_id, bucket_ts, client_service, server_service, operation;

