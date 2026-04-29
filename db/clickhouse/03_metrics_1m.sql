CREATE TABLE IF NOT EXISTS observability.metrics_1m (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    metric_name          LowCardinality(String),
    metric_type          LowCardinality(String),
    temporality          LowCardinality(String) DEFAULT 'Unspecified',
    is_monotonic         Bool CODEC(T64, ZSTD(1)),
    unit                 LowCardinality(String) DEFAULT '',
    description          LowCardinality(String) DEFAULT '',
    fingerprint          String CODEC(ZSTD(3)),
    timestamp            DateTime64(3) CODEC(DoubleDelta, LZ4),
    ts_bucket            UInt32 CODEC(DoubleDelta, LZ4),

    -- Unified aggregates (merges gauges and histograms automatically)
    val_sum              SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    val_count            SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    val_min              SimpleAggregateFunction(min, Float64) CODEC(Gorilla, ZSTD(1)),
    val_max              SimpleAggregateFunction(max, Float64) CODEC(Gorilla, ZSTD(1)),

    hist_sum             SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    hist_count           SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
    hist_buckets         SimpleAggregateFunction(any, Array(Float64)) CODEC(ZSTD(1)),
    hist_counts          SimpleAggregateFunction(anyHeavy, Array(UInt64)) CODEC(T64, ZSTD(1)),


    service              LowCardinality(String) CODEC(ZSTD(1)),
    host                 LowCardinality(String) CODEC(ZSTD(1)),
    environment          LowCardinality(String) CODEC(ZSTD(1)),
    k8s_namespace        LowCardinality(String) CODEC(ZSTD(1)),
    http_method          LowCardinality(String) CODEC(ZSTD(1)),
    http_status_code     UInt16 CODEC(T64, ZSTD(1)),
    
    resource             SimpleAggregateFunction(anyHeavy, JSON(max_dynamic_paths=100)) CODEC(ZSTD(1)),
    attributes           SimpleAggregateFunction(anyHeavy, JSON(max_dynamic_paths=100)) CODEC(ZSTD(1))
) ENGINE = AggregatingMergeTree()
PARTITION BY (toYYYYMMDD(timestamp), toHour(timestamp))
ORDER BY (team_id, ts_bucket, metric_name, fingerprint, timestamp)
TTL timestamp + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192, enable_mixed_granularity_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_1m_mv
TO observability.metrics_1m AS
SELECT
    team_id, metric_name, metric_type, temporality, is_monotonic, unit, description, fingerprint,
    toStartOfMinute(timestamp) AS timestamp, ts_bucket,

    -- Pre-calculate the unified states at ingestion time (zero read-time cost)
    sum(if(metric_type = 'Histogram', hist_sum, value)) AS val_sum,
    toUInt64(sum(if(metric_type = 'Histogram', hist_count, 1))) AS val_count,
    min(if(metric_type = 'Histogram', hist_sum / nullIf(hist_count, 0), value)) AS val_min,
    max(if(metric_type = 'Histogram', hist_sum / nullIf(hist_count, 0), value)) AS val_max,

    sum(hist_sum) AS hist_sum,
    toUInt64(sum(hist_count)) AS hist_count,
    any(hist_buckets) AS hist_buckets,
    anyHeavy(hist_counts) AS hist_counts,


    service, host, environment, k8s_namespace, http_method, http_status_code,
    anyHeavy(resource) AS resource, anyHeavy(attributes) AS attributes
FROM observability.metrics
GROUP BY
    team_id, ts_bucket, metric_name, fingerprint, metric_type, temporality, is_monotonic, unit, description, 
    service, host, environment, k8s_namespace, http_method, http_status_code, toStartOfMinute(timestamp);
