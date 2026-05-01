-- INVARIANT: bucket values are computed Go-side (internal/infra/timebucket); no CH bucket functions in this file.
-- Reverse-key projection of observability.logs. Used by the trace_logs module
-- ([internal/modules/logs/trace_logs/](../../internal/modules/logs/trace_logs/))
-- to resolve (team_id, trace_id) → (ts_bucket bounds, fingerprint set, log_id list)
-- in O(one granule) before scanning the raw logs table. Leading PK = trace_id
-- so trace-id-keyed lookups land on a single granule. log_id is included so
-- callers can construct deep links to individual logs without re-fetching.

CREATE TABLE IF NOT EXISTS observability.trace_index (
    trace_id    String         CODEC(ZSTD(1)),
    team_id     UInt32         CODEC(T64, ZSTD(1)),
    ts_bucket   UInt32         CODEC(DoubleDelta, LZ4),
    fingerprint String         CODEC(ZSTD(1)),
    timestamp   DateTime64(9)  CODEC(DoubleDelta, LZ4),
    log_id      String         CODEC(ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(toDateTime(ts_bucket))
ORDER BY (trace_id, team_id, ts_bucket, fingerprint, timestamp, log_id)
TTL toDateTime(ts_bucket) + INTERVAL 30 DAY DELETE
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_to_trace_index
TO observability.trace_index AS
SELECT
    trace_id,
    team_id,
    ts_bucket,
    fingerprint,
    timestamp,
    log_id
FROM observability.logs
WHERE trace_id != '';
