-- Add `state_dim` to metrics_hist_{1m,5m,1h} so histogram-class metrics that
-- need an extra dimension (today: jvm.gc.duration → jvm.gc.name) can roll up
-- without falling back to a raw-metrics scan. Pattern mirrors metrics_gauges
-- and metrics_k8s.
--
-- Existing rows default state_dim = '' and continue to read correctly via
-- sumMerge / quantilesTDigestWeightedMerge across the dim.
--
-- New raw → 1m feed populates state_dim from the raw materialized column.
-- 1m → 5m and 5m → 1h merges propagate it.
--
-- Optional one-shot backfill (run manually if the dashboard needs historical
-- collector breakdown):
--
--   INSERT INTO observability.metrics_hist_1m
--   SELECT team_id, toStartOfMinute(timestamp) AS bucket_ts, metric_name, service, state_dim,
--          quantilesTDigestWeightedState(0.5, 0.95, 0.99)(if(hist_count>0, hist_sum/hist_count, 0.0), hist_count),
--          sumState(hist_count), sumState(hist_sum)
--   FROM observability.metrics
--   WHERE metric_type = 'Histogram' AND hist_count > 0
--     AND timestamp > now() - INTERVAL 1 HOUR
--   GROUP BY team_id, bucket_ts, metric_name, service, state_dim;

ALTER TABLE observability.metrics_hist_1m
    ADD COLUMN IF NOT EXISTS state_dim LowCardinality(String) DEFAULT '' AFTER service,
    MODIFY ORDER BY (team_id, bucket_ts, metric_name, service, state_dim);

ALTER TABLE observability.metrics_hist_5m
    ADD COLUMN IF NOT EXISTS state_dim LowCardinality(String) DEFAULT '' AFTER service,
    MODIFY ORDER BY (team_id, bucket_ts, metric_name, service, state_dim);

ALTER TABLE observability.metrics_hist_1h
    ADD COLUMN IF NOT EXISTS state_dim LowCardinality(String) DEFAULT '' AFTER service,
    MODIFY ORDER BY (team_id, bucket_ts, metric_name, service, state_dim);

DROP TABLE IF EXISTS observability.metrics_hist_raw_to_1m;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_hist_raw_to_1m
TO observability.metrics_hist_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp) AS bucket_ts,
    metric_name,
    service,
    state_dim,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(
        if(hist_count > 0, hist_sum / hist_count, 0.0),
        hist_count
    )                                     AS latency_ms_digest,
    sumState(hist_count)                  AS hist_count,
    sumState(hist_sum)                    AS hist_sum
FROM observability.metrics
WHERE metric_type = 'Histogram' AND hist_count > 0
GROUP BY team_id, bucket_ts, metric_name, service, state_dim;

DROP TABLE IF EXISTS observability.metrics_hist_1m_to_5m;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_hist_1m_to_5m
TO observability.metrics_hist_5m AS
SELECT
    team_id,
    toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
    metric_name, service, state_dim,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(hist_count) AS hist_count,
    sumMergeState(hist_sum)   AS hist_sum
FROM observability.metrics_hist_1m
GROUP BY team_id, bucket_ts, metric_name, service, state_dim;

DROP TABLE IF EXISTS observability.metrics_hist_5m_to_1h;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_hist_5m_to_1h
TO observability.metrics_hist_1h AS
SELECT
    team_id,
    toStartOfHour(bucket_ts) AS bucket_ts,
    metric_name, service, state_dim,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
    sumMergeState(hist_count) AS hist_count,
    sumMergeState(hist_sum)   AS hist_sum
FROM observability.metrics_hist_5m
GROUP BY team_id, bucket_ts, metric_name, service, state_dim;
