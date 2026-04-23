-- logs_facets_rollup_5m — facet sketches from observability.logs.

CREATE TABLE IF NOT EXISTS observability.logs_facets_rollup_5m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    severity_bucket UInt8,
    service         LowCardinality(String),
    environment     LowCardinality(String),
    log_count       AggregateFunction(sum, UInt64),
    host_hll        AggregateFunction(uniqHLL12, String),
    pod_hll         AggregateFunction(uniqHLL12, String),
    trace_id_hll    AggregateFunction(uniqHLL12, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, severity_bucket, service, environment)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_to_facets_rollup_5m
TO observability.logs_facets_rollup_5m AS
SELECT
    team_id,
    toStartOfInterval(timestamp, toIntervalMinute(5)) AS bucket_ts,
    severity_bucket                                    AS severity_bucket,
    service                                            AS service,
    environment                                        AS environment,
    sumState(toUInt64(1))                              AS log_count,
    uniqHLL12State(host)                               AS host_hll,
    uniqHLL12State(pod)                                AS pod_hll,
    uniqHLL12State(trace_id)                           AS trace_id_hll
FROM observability.logs
GROUP BY team_id, bucket_ts, severity_bucket, service, environment;
