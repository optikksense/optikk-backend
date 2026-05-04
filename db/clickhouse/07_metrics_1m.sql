CREATE TABLE IF NOT EXISTS observability.metrics_1m (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket            UInt32 CODEC(DoubleDelta, LZ4),
    timestamp            DateTime CODEC(DoubleDelta, LZ4),
    metric_name          LowCardinality(String),
    fingerprint          String CODEC(ZSTD(1)),
    attr_hash            UInt64 CODEC(T64, ZSTD(1)),

    -- Scalar (Gauge / Sum) — four aggregations per row.
    val_min              SimpleAggregateFunction(min, Float64)     CODEC(Gorilla, ZSTD(1)),
    val_max              SimpleAggregateFunction(max, Float64)     CODEC(Gorilla, ZSTD(1)),
    val_sum              SimpleAggregateFunction(sum, Float64)     CODEC(Gorilla, ZSTD(1)),
    val_count            SimpleAggregateFunction(sum, UInt64)      CODEC(T64, ZSTD(1)),

    -- Histogram — Prometheus-style quantile state for server-side
    -- p50/p95/p99 reads via quantilesPrometheusHistogramMerge, plus
    -- per-data-point sum/count for avg-duration panels. Bucket arrays
    -- live on raw `observability.metrics`; the MV reads them there to
    -- build `latency_state` and does not project them into the rollup.
    hist_sum             SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    hist_count           SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    latency_state        AggregateFunction(quantilePrometheusHistogram, Float64, UInt64) CODEC(ZSTD(1)),

    service              LowCardinality(String) CODEC(ZSTD(1)),
    host                 LowCardinality(String) CODEC(ZSTD(1)),
    environment          LowCardinality(String) CODEC(ZSTD(1)),
    k8s_namespace        LowCardinality(String) CODEC(ZSTD(1)),
    http_method          LowCardinality(String) CODEC(ZSTD(1)),
    http_status_code     UInt16 CODEC(T64, ZSTD(1)),
    resource             JSON(max_dynamic_paths=100) CODEC(ZSTD(1)),
    attributes           JSON(max_dynamic_paths=100) CODEC(ZSTD(1))
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (team_id, ts_bucket, fingerprint, metric_name, attr_hash, timestamp)
TTL timestamp + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_1m_mv
TO observability.metrics_1m AS
SELECT
    team_id,
    toUInt32(intDiv(toUnixTimestamp(toStartOfMinute(timestamp)), 300) * 300) AS ts_bucket,
    toStartOfMinute(timestamp)                                                AS timestamp,
    metric_name,
    fingerprint,
    cityHash64(toJSONString(attributes))                                      AS attr_hash,
    attributes,
    resource,

    -- Scalar (Gauge / Sum) — fire once per source row, conditional on type.
    minIf(value,     metric_type IN ('Gauge', 'Sum')) AS val_min,
    maxIf(value,     metric_type IN ('Gauge', 'Sum')) AS val_max,
    sumIf(value,     metric_type IN ('Gauge', 'Sum')) AS val_sum,
    countIf(value,   metric_type IN ('Gauge', 'Sum')) AS val_count,

    -- Per-data-point histogram totals.
    sumIf(hist_sum,   metric_type = 'Histogram') AS hist_sum,
    sumIf(hist_count, metric_type = 'Histogram') AS hist_count,

    -- Prometheus-style quantile state. The -Array combinator feeds each
    -- (bucket_upper_bound, cumulative_count) pair from the per-data-point
    -- arrays into the aggregate; -StateIf gates on Histogram rows only.
    -- Bucket arrays are read from raw `observability.metrics` and not
    -- projected into the rollup — `latency_state` carries everything
    -- readers need.
    quantilePrometheusHistogramArrayStateIf(
        hist_buckets,
        arrayCumSum(hist_counts),
        metric_type = 'Histogram'
    ) AS latency_state,

    service,
    host,
    environment,
    k8s_namespace,
    http_method,
    http_status_code
FROM observability.metrics
GROUP BY
    team_id,
    ts_bucket,
    timestamp,
    metric_name,
    fingerprint,
    attr_hash,
    attributes,
    resource,
    service,
    host,
    environment,
    k8s_namespace,
    http_method,
    http_status_code;
