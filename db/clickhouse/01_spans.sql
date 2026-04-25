-- Raw OTLP spans (single schema; formerly split across legacy 01 + spans_v2 cutover).
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
    attributes                            JSON(max_dynamic_paths = 100) CODEC(ZSTD(1)),
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
    -- http_status_bucket: computed once at raw insert; fans into spans_red, spans_peer,
    -- spans_errors. Replaces per-MV multiIf from the pre-rewrite schema.
    http_status_bucket        LowCardinality(String)
                                    MATERIALIZED multiIf(
                                        toUInt16OrZero(response_status_code) >= 500, '5xx',
                                        toUInt16OrZero(response_status_code) >= 400, '4xx',
                                        toUInt16OrZero(response_status_code) >= 300, '3xx',
                                        toUInt16OrZero(response_status_code) >= 200, '2xx',
                                        has_error, 'err',
                                        'other'
                                    ) CODEC(ZSTD(1)),
    INDEX idx_service_name          service_name            TYPE bloom_filter(0.01)      GRANULARITY 4,
    INDEX idx_trace_id              trace_id                TYPE bloom_filter(0.01)      GRANULARITY 4,
    INDEX idx_trace_id_token        trace_id                TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4,
    INDEX idx_span_id               span_id                 TYPE bloom_filter(0.01)      GRANULARITY 4,
    INDEX idx_span_name             name                    TYPE bloom_filter(0.01)      GRANULARITY 4,
    INDEX idx_has_error             has_error               TYPE set(2)                  GRANULARITY 4,
    INDEX idx_mat_http_route        mat_http_route          TYPE bloom_filter(0.01)      GRANULARITY 4,
    INDEX idx_mat_http_status_code  mat_http_status_code    TYPE bloom_filter(0.01)      GRANULARITY 4,
    INDEX idx_mat_db_system         mat_db_system           TYPE bloom_filter(0.01)      GRANULARITY 4,
    INDEX idx_mat_db_name           mat_db_name             TYPE bloom_filter(0.01)      GRANULARITY 4,
    INDEX idx_mat_rpc_service       mat_rpc_service         TYPE bloom_filter(0.01)      GRANULARITY 4,
    INDEX idx_mat_peer_service      mat_peer_service        TYPE bloom_filter(0.01)      GRANULARITY 4,
    INDEX idx_mat_exception_type    mat_exception_type      TYPE bloom_filter(0.01)      GRANULARITY 4,
    INDEX idx_mat_host_name         mat_host_name           TYPE bloom_filter(0.01)      GRANULARITY 4,
    INDEX idx_mat_k8s_pod_name      mat_k8s_pod_name        TYPE bloom_filter(0.01)      GRANULARITY 4,
    INDEX idx_mat_service_version        mat_service_version        TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_mat_deployment_environment mat_deployment_environment TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, ts_bucket_start, service_name, name, timestamp)
TTL timestamp + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    non_replicated_deduplication_window = 1000,
    ttl_only_drop_parts = 1;
