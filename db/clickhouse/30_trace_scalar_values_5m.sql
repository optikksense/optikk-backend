-- trace_scalar_values_5m — pre-summarised top values per
-- (team_id, field_name) in 5-min buckets for the `/traces/suggest` scalar
-- path. Mirror of trace_attribute_values_5m for the @-prefixed path; replaces
-- a `positionCaseInsensitive(<col>, prefix)` scan over raw traces_index.
--
-- One row per (team_id, field_name, bucket_ts, value) — the MV fans out each
-- traces_index row into one tuple per scalar field via ARRAY JOIN.

CREATE TABLE IF NOT EXISTS observability.trace_scalar_values_5m (
    team_id    UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts  DateTime CODEC(DoubleDelta, LZ4),
    field_name LowCardinality(String) CODEC(ZSTD(1)),
    value      String CODEC(ZSTD(1)),
    count_agg  AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, field_name, bucket_ts, value)
TTL bucket_ts + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.traces_index_to_trace_scalar_values_5m
TO observability.trace_scalar_values_5m AS
SELECT
    team_id,
    toStartOfInterval(toDateTime(intDiv(ts_bucket_start, 1000)), toIntervalMinute(5)) AS bucket_ts,
    fv.1                                                                              AS field_name,
    fv.2                                                                              AS value,
    sumState(toUInt64(1))                                                             AS count_agg
FROM observability.traces_index
ARRAY JOIN [
    ('service',     root_service::String),
    ('operation',   root_operation::String),
    ('http_method', root_http_method::String),
    ('http_status', toString(root_http_status)),
    ('status',      root_status::String),
    ('environment', environment::String)
] AS fv
WHERE fv.2 != ''
GROUP BY team_id, bucket_ts, field_name, value;
