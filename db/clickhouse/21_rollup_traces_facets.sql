-- traces_facets_rollup_5m — HLL facet sketches for the traces explorer
-- facet rail. Keyed by (team_id, bucket_ts, root_service, root_http_method,
-- root_status). Stores trace_id + peer_service cardinality estimates and a
-- sum of trace counts so wide-window (7d+) facets answer without scanning
-- traces_index. Sourced from traces_index at 5m granularity.

CREATE TABLE IF NOT EXISTS observability.traces_facets_rollup_5m (
    team_id           UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts         DateTime CODEC(DoubleDelta, LZ4),
    root_service      LowCardinality(String),
    root_http_method  LowCardinality(String),
    root_status       LowCardinality(String),
    trace_count       AggregateFunction(sum, UInt64),
    error_count       AggregateFunction(sum, UInt64),
    trace_id_hll      AggregateFunction(uniqHLL12, String),
    peer_service_hll  AggregateFunction(uniqHLL12, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, root_service, root_http_method, root_status)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.traces_index_to_facets_rollup_5m
TO observability.traces_facets_rollup_5m AS
SELECT
    team_id,
    toStartOfInterval(toDateTime(intDiv(start_ms, 1000)), toIntervalMinute(5)) AS bucket_ts,
    root_service                                                                AS root_service,
    root_http_method                                                            AS root_http_method,
    root_status                                                                 AS root_status,
    sumState(toUInt64(1))                                                       AS trace_count,
    sumState(toUInt64(has_error))                                               AS error_count,
    uniqHLL12State(trace_id)                                                    AS trace_id_hll,
    uniqHLL12State(arrayJoin(peer_service_set))                                 AS peer_service_hll
FROM observability.traces_index
GROUP BY team_id, bucket_ts, root_service, root_http_method, root_status;
