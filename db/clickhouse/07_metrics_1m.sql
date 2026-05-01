CREATE TABLE IF NOT EXISTS observability.metrics_1m (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    ts_bucket            UInt32 CODEC(DoubleDelta, LZ4),
    timestamp            DateTime CODEC(DoubleDelta, LZ4),
    metric_name          LowCardinality(String),
    fingerprint          String CODEC(ZSTD(1)),
    attr_hash            UInt64 CODEC(T64, ZSTD(1)),

    -- Scalar (Gauge / Sum) — five aggregations per row.
    val_min              SimpleAggregateFunction(min, Float64)     CODEC(Gorilla, ZSTD(1)),
    val_max              SimpleAggregateFunction(max, Float64)     CODEC(Gorilla, ZSTD(1)),
    val_sum              SimpleAggregateFunction(sum, Float64)     CODEC(Gorilla, ZSTD(1)),
    val_count            SimpleAggregateFunction(sum, UInt64)      CODEC(T64, ZSTD(1)),
    val_last             SimpleAggregateFunction(anyLast, Float64) CODEC(Gorilla, ZSTD(1)),

    -- Histogram — element-wise merged bucket counts + a representative
    -- bucket-bounds array. Percentiles interpolated Go-side via
    -- internal/shared/quantile.FromHistogram. SimpleAggregateFunction means
    -- readers project these columns directly (no -Merge call); the engine
    -- merges parts via sumForEach / max.
    hist_buckets         SimpleAggregateFunction(max,        Array(Float64)) CODEC(ZSTD(1)),
    hist_counts          AggregateFunction(sumForEach, Array(UInt64)) CODEC(ZSTD(1)),
    hist_sum             SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    hist_count           SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),

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
    anyLastIf(value, metric_type IN ('Gauge', 'Sum')) AS val_last,

    -- Histogram — element-wise bucket-count merge + representative bounds.
    -- Non-Histogram source rows return [] from *If, identity under sumForEach
    -- and lex-less than any real bounds under max — so the rollup row's
    -- bucket-bounds and bucket-counts come from Histogram contributions only.
    sumForEachStateIf(hist_counts, metric_type = 'Histogram') AS hist_counts,
    maxIf(hist_buckets,       metric_type = 'Histogram') AS hist_buckets,

    -- Per-data-point histogram totals.
    sumIf(hist_sum,   metric_type = 'Histogram') AS hist_sum,
    sumIf(hist_count, metric_type = 'Histogram') AS hist_count,

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
