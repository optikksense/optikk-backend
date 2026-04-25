-- Phase 7 perf: root_spans_index — only root spans, keyed for the
-- "Related traces" lookup on the trace-detail drawer. The Related query
-- filters raw spans by (service_name, name, timestamp, is-root), where
-- is-root is a function predicate (`parent_span_id = '' OR = all-zero`).
-- Function predicates aren't indexable, so today the query scans a wide
-- slice of the 100M-row spans table. This MV stores only root rows, keyed
-- on (team_id, service_name, name, ts_bucket_start, timestamp), giving the
-- query a narrow keyset range scan.

CREATE TABLE IF NOT EXISTS observability.root_spans_index (
    team_id          UInt32         CODEC(T64, ZSTD(1)),
    service_name     LowCardinality(String) CODEC(ZSTD(1)),
    name             LowCardinality(String) CODEC(ZSTD(1)),
    ts_bucket_start  UInt64         CODEC(DoubleDelta, LZ4),
    timestamp        DateTime64(9)  CODEC(DoubleDelta, LZ4),
    trace_id         String         CODEC(ZSTD(1)),
    span_id          String         CODEC(ZSTD(1)),
    duration_nano    UInt64         CODEC(T64, ZSTD(1)),
    status_code_string LowCardinality(String) CODEC(ZSTD(1)),
    has_error        Bool           CODEC(T64, ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(toDateTime(ts_bucket_start))
ORDER BY (team_id, service_name, name, ts_bucket_start, timestamp)
TTL toDateTime(ts_bucket_start) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_to_root_spans_index
TO observability.root_spans_index AS
SELECT
    team_id,
    service_name,
    name,
    ts_bucket_start,
    timestamp,
    trace_id,
    span_id,
    duration_nano,
    status_code_string,
    has_error
FROM observability.spans
WHERE parent_span_id = '' OR parent_span_id = '0000000000000000';
