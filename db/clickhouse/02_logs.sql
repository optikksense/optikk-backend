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
