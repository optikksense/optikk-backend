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
