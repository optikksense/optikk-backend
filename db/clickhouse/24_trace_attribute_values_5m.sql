-- Phase 7 perf: trace_attribute_values_5m — pre-summarised top values per
-- (team_id, attribute_key) in 5-min buckets. Powers the `/traces/suggest`
-- @attribute autocomplete path which previously scanned a full time-range
-- slice of raw spans with `positionCaseInsensitive()` — un-indexable and
-- typically 100–500 ms. A narrow keyed range scan on this MV + prefix filter
-- is <20 ms.
--
-- Materialised via AggregatingMergeTree so a sumMerge on `count_agg` at
-- read time collapses the per-bucket counts. TTL matches the spans retention.

CREATE TABLE IF NOT EXISTS observability.trace_attribute_values_5m (
    team_id       UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts     DateTime CODEC(DoubleDelta, LZ4),
    attribute_key LowCardinality(String) CODEC(ZSTD(1)),
    value         String CODEC(ZSTD(1)),
    count_agg     AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, attribute_key, bucket_ts, value)
TTL bucket_ts + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;

-- Populate continuously: explode each span's attributes map and bucket the
-- resulting (key, value) tuples at 5-min granularity.
CREATE MATERIALIZED VIEW IF NOT EXISTS observability.spans_to_trace_attribute_values_5m
TO observability.trace_attribute_values_5m AS
SELECT
    team_id,
    toStartOfInterval(toDateTime(intDiv(ts_bucket_start, 1000)), toIntervalMinute(5)) AS bucket_ts,
    kv.1                                                                               AS attribute_key,
    kv.2                                                                               AS value,
    sumState(toUInt64(1))                                                              AS count_agg
FROM observability.spans
ARRAY JOIN JSONExtractKeysAndValues(CAST(attributes, 'String'), 'String') AS kv
GROUP BY team_id, bucket_ts, attribute_key, value;
