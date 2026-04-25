-- Phase 7 perf: spans_by_trace_index — per-trace span projection.
-- Collapses the 100M-row `observability.spans` scan behind every trace
-- drill-down (flamegraph, critical-path, error-path, span-kind breakdown,
-- spans list, service-map, attributes) into a narrow scan keyed on
-- (team_id, trace_id, span_id). The source is an append-only MergeTree so
-- Replacing/AggregatingMergeTree isn't needed; a plain MergeTree keyed on
-- (team_id, trace_id) gives us both partition elimination and a cheap
-- range scan for all spans in one trace.

CREATE TABLE IF NOT EXISTS observability.spans_by_trace_index (
    team_id            UInt32         CODEC(T64, ZSTD(1)),
    trace_id           String         CODEC(ZSTD(1)),
    span_id            String         CODEC(ZSTD(1)),
    parent_span_id     String         CODEC(ZSTD(1)),
    ts_bucket_start    UInt64         CODEC(DoubleDelta, LZ4),
    timestamp          DateTime64(9)  CODEC(DoubleDelta, LZ4),
    duration_nano      UInt64         CODEC(T64, ZSTD(1)),
    service_name       LowCardinality(String) CODEC(ZSTD(1)),
    name               LowCardinality(String) CODEC(ZSTD(1)),
    kind_string        LowCardinality(String) CODEC(ZSTD(1)),
    status_code_string LowCardinality(String) CODEC(ZSTD(1)),
    has_error          Bool           CODEC(T64, ZSTD(1)),
    http_method        LowCardinality(String) CODEC(ZSTD(1)),
    response_status_code LowCardinality(String) CODEC(ZSTD(1)),
    exception_type     LowCardinality(String) CODEC(ZSTD(1)),
    exception_message  String         CODEC(ZSTD(1)),
    exception_stacktrace String       CODEC(ZSTD(1)),
    links              String         CODEC(ZSTD(1)),
    attributes         JSON(max_dynamic_paths = 100) CODEC(ZSTD(1)),
    INDEX idx_span_id span_id TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(toDateTime(ts_bucket_start))
ORDER BY (team_id, trace_id, span_id)
TTL toDateTime(ts_bucket_start) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;

-- Populate continuously from the raw spans ingest. One row per span.
CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_to_spans_by_trace_index
TO observability.spans_by_trace_index AS
SELECT
    team_id,
    trace_id,
    span_id,
    parent_span_id,
    ts_bucket_start,
    timestamp,
    duration_nano,
    service_name,
    name,
    kind_string,
    status_code_string,
    has_error,
    http_method,
    response_status_code,
    exception_type,
    exception_message,
    exception_stacktrace,
    links,
    attributes
FROM observability.spans;
