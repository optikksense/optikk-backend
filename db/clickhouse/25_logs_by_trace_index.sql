-- Phase 7 perf: logs_by_trace_index — per-trace log projection for the
-- tracedetail Logs tab. The base logs table (ORDER BY team_id, timestamp)
-- doesn't have a narrow trace_id path — today `GetTraceLogs` / `GetSpanLogs`
-- bloom-filter-scan the whole date partition. This MV keyed on
-- (team_id, trace_id, span_id, timestamp) collapses the scan to a narrow
-- keyset range.

CREATE TABLE IF NOT EXISTS observability.logs_by_trace_index (
    team_id            UInt32 CODEC(T64, ZSTD(1)),
    trace_id           String CODEC(ZSTD(1)),
    span_id            String CODEC(ZSTD(1)),
    timestamp          DateTime64(9) CODEC(DoubleDelta, LZ4),
    observed_timestamp UInt64 CODEC(DoubleDelta, LZ4),
    ts_bucket_start    UInt32 CODEC(Delta(4), LZ4),
    severity_text      LowCardinality(String) CODEC(ZSTD(1)),
    severity_number    UInt8 DEFAULT 0,
    body               String CODEC(ZSTD(2)),
    trace_flags        UInt32 DEFAULT 0,
    service            LowCardinality(String) CODEC(ZSTD(1)),
    host               LowCardinality(String) CODEC(ZSTD(1)),
    pod                LowCardinality(String) CODEC(ZSTD(1)),
    container          LowCardinality(String) CODEC(ZSTD(1)),
    environment        LowCardinality(String) CODEC(ZSTD(1)),
    attributes_string  Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    attributes_number  Map(LowCardinality(String), Float64) CODEC(ZSTD(1)),
    attributes_bool    Map(LowCardinality(String), Bool) CODEC(ZSTD(1)),
    scope_name         String CODEC(ZSTD(1)),
    scope_version      String CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(toDateTime(ts_bucket_start))
ORDER BY (team_id, trace_id, span_id, timestamp)
TTL toDateTime(ts_bucket_start) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_to_logs_by_trace_index
TO observability.logs_by_trace_index AS
SELECT
    team_id,
    trace_id,
    span_id,
    timestamp,
    observed_timestamp,
    ts_bucket_start,
    severity_text,
    severity_number,
    body,
    trace_flags,
    service,
    host,
    pod,
    container,
    environment,
    attributes_string,
    attributes_number,
    attributes_bool,
    scope_name,
    scope_version
FROM observability.logs
WHERE trace_id != '' AND trace_id != '00000000000000000000000000000000';
