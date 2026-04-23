-- traces_index — per-trace summary from internal/ingestion/spans/indexer.
-- Explorer trace-list reads from here
-- instead of raw spans, cutting row count by ~2 orders of magnitude. The
-- indexer has at-least-once semantics (trace rows can be re-emitted as late
-- spans arrive), so the engine is ReplacingMergeTree keyed on
-- (team_id, trace_id) with `last_seen_ms` as the version column.

CREATE TABLE IF NOT EXISTS observability.traces_index (
    team_id            UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket_start    UInt64 CODEC(DoubleDelta, LZ4),
    trace_id           String CODEC(ZSTD(1)),
    start_ms           UInt64 CODEC(DoubleDelta, LZ4),
    end_ms             UInt64 CODEC(DoubleDelta, LZ4),
    last_seen_ms       UInt64 CODEC(DoubleDelta, LZ4),
    duration_ns        UInt64 CODEC(T64, ZSTD(1)),
    root_service       LowCardinality(String) CODEC(ZSTD(1)),
    root_operation     LowCardinality(String) CODEC(ZSTD(1)),
    root_status        LowCardinality(String) CODEC(ZSTD(1)),
    root_http_method   LowCardinality(String) CODEC(ZSTD(1)),
    root_http_status   UInt16 CODEC(T64, ZSTD(1)),
    span_count         UInt32 CODEC(T64, ZSTD(1)),
    has_error          Bool   CODEC(T64, ZSTD(1)),
    error_count        UInt32 CODEC(T64, ZSTD(1)),
    service_set        Array(LowCardinality(String)) CODEC(ZSTD(1)),
    peer_service_set   Array(LowCardinality(String)) CODEC(ZSTD(1)),
    error_fingerprint  String CODEC(ZSTD(1)),
    environment        LowCardinality(String) CODEC(ZSTD(1)),
    truncated          Bool   CODEC(T64, ZSTD(1)),
    INDEX idx_trace_id         trace_id         TYPE bloom_filter(0.01)      GRANULARITY 4,
    INDEX idx_trace_id_token   trace_id         TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4,
    INDEX idx_has_error        has_error        TYPE set(2)                  GRANULARITY 4,
    INDEX idx_root_http_method root_http_method TYPE set(16)                 GRANULARITY 4,
    INDEX idx_root_status      root_status      TYPE set(8)                  GRANULARITY 4,
    INDEX idx_environment      environment      TYPE set(32)                 GRANULARITY 4,
    INDEX idx_error_fp         error_fingerprint TYPE bloom_filter(0.01)     GRANULARITY 4
) ENGINE = ReplacingMergeTree(last_seen_ms)
PARTITION BY toYYYYMMDD(toDateTime(ts_bucket_start))
ORDER BY (team_id, ts_bucket_start, root_service, start_ms, trace_id)
TTL toDateTime(ts_bucket_start) + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;
