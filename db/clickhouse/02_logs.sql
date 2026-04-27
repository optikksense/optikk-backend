-- INVARIANT: bucket values are computed Go-side (internal/infra/timebucket); no CH bucket functions in this file.
CREATE TABLE IF NOT EXISTS observability.logs (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket            UInt32 CODEC(Delta(4), LZ4),
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
    fingerprint          String CODEC(ZSTD(1)),
    scope_name           String CODEC(ZSTD(1)),
    scope_version        String CODEC(ZSTD(1)),
    service              LowCardinality(String) CODEC(ZSTD(1)),
    host                 LowCardinality(String) CODEC(ZSTD(1)),
    pod                  LowCardinality(String) CODEC(ZSTD(1)),
    container            LowCardinality(String) CODEC(ZSTD(1)),
    environment          LowCardinality(String) CODEC(ZSTD(1)),
    severity_bucket      UInt8 MATERIALIZED multiIf(
        severity_number >= 21, toUInt8(5),
        severity_number >= 17, toUInt8(4),
        severity_number >= 13, toUInt8(3),
        severity_number >= 9,  toUInt8(2),
        severity_number >= 5,  toUInt8(1),
        toUInt8(0)
    ),
    INDEX idx_fingerprint  fingerprint  TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_trace_id     trace_id     TYPE bloom_filter(0.01) GRANULARITY 1
) ENGINE = MergeTree()
PARTITION BY (toYYYYMMDD(timestamp), toHour(timestamp))
ORDER BY (team_id, ts_bucket, fingerprint, severity_bucket, timestamp, trace_id, span_id)
TTL timestamp + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    non_replicated_deduplication_window = 100000,
    ttl_only_drop_parts = 1;
