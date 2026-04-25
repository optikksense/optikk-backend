-- logs_facets — HLL sketches for facet-rail cardinality estimates on the
-- logs explorer. Previously single-tier (5m only); now extended to all
-- three tiers so PickTier has uniform behaviour across families.

CREATE TABLE IF NOT EXISTS observability.logs_facets_1m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    service         LowCardinality(String),
    environment     LowCardinality(String),
    severity_bucket UInt8,
    log_count       AggregateFunction(sum, UInt64),
    host_hll        AggregateFunction(uniqHLL12, String),
    pod_hll         AggregateFunction(uniqHLL12, String),
    trace_id_hll    AggregateFunction(uniqHLL12, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service, environment, severity_bucket)
TTL bucket_ts + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_facets_raw_to_1m
TO observability.logs_facets_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp) AS bucket_ts,
    service,
    environment,
    severity_bucket,
    sumState(toUInt64(1))   AS log_count,
    uniqHLL12State(host)    AS host_hll,
    uniqHLL12State(pod)     AS pod_hll,
    uniqHLL12State(trace_id) AS trace_id_hll
FROM observability.logs
GROUP BY team_id, bucket_ts, service, environment, severity_bucket;

CREATE TABLE IF NOT EXISTS observability.logs_facets_5m (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    service         LowCardinality(String),
    environment     LowCardinality(String),
    severity_bucket UInt8,
    log_count       AggregateFunction(sum, UInt64),
    host_hll        AggregateFunction(uniqHLL12, String),
    pod_hll         AggregateFunction(uniqHLL12, String),
    trace_id_hll    AggregateFunction(uniqHLL12, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service, environment, severity_bucket)
TTL bucket_ts + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_facets_1m_to_5m
TO observability.logs_facets_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    service, environment, severity_bucket,
    sumMergeState(log_count)     AS log_count,
    uniqHLL12MergeState(host_hll)     AS host_hll,
    uniqHLL12MergeState(pod_hll)      AS pod_hll,
    uniqHLL12MergeState(trace_id_hll) AS trace_id_hll
FROM observability.logs_facets_1m
GROUP BY team_id, bucket_ts, service, environment, severity_bucket;

CREATE TABLE IF NOT EXISTS observability.logs_facets_1h (
    team_id         UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts       DateTime CODEC(DoubleDelta, LZ4),
    service         LowCardinality(String),
    environment     LowCardinality(String),
    severity_bucket UInt8,
    log_count       AggregateFunction(sum, UInt64),
    host_hll        AggregateFunction(uniqHLL12, String),
    pod_hll         AggregateFunction(uniqHLL12, String),
    trace_id_hll    AggregateFunction(uniqHLL12, String)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, service, environment, severity_bucket)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.logs_facets_5m_to_1h
TO observability.logs_facets_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    service, environment, severity_bucket,
    sumMergeState(log_count)     AS log_count,
    uniqHLL12MergeState(host_hll)     AS host_hll,
    uniqHLL12MergeState(pod_hll)      AS pod_hll,
    uniqHLL12MergeState(trace_id_hll) AS trace_id_hll
FROM observability.logs_facets_5m
GROUP BY team_id, bucket_ts, service, environment, severity_bucket;
