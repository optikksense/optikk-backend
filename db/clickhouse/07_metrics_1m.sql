-- -- 1-minute rollup table (AggregatingMergeTree)
-- CREATE TABLE IF NOT EXISTS observability.metrics_1m
-- (
--     team_id              UInt32 CODEC(T64, ZSTD(1)),
--     metric_name          LowCardinality(String),
--     metric_type          LowCardinality(String),
--     temporality          LowCardinality(String) DEFAULT 'Unspecified',
--     is_monotonic         Bool CODEC(T64, ZSTD(1)),
--     unit                 LowCardinality(String) DEFAULT '',
--     description          LowCardinality(String) DEFAULT '',
--     fingerprint          String CODEC(ZSTD(3)),
--     timestamp            DateTime64(3) CODEC(DoubleDelta, LZ4),

--     -- Gauge / Delta-sum values (only for non‑monotonic or delta counters)
--     gauge_sum            SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
--     gauge_count          SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
--     gauge_min            SimpleAggregateFunction(min, Float64) CODEC(Gorilla, ZSTD(1)),
--     gauge_max            SimpleAggregateFunction(max, Float64) CODEC(Gorilla, ZSTD(1)),

--     -- Cumulative monotonic counter (last observed value per interval)
--     counter_last         SimpleAggregateFunction(anyLast, Float64) CODEC(Gorilla, ZSTD(1)),

--     -- Histogram (exact bucket merge via sumMap state)
--     hist_sum             SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
--     hist_count           SimpleAggregateFunction(sum, UInt64) CODEC(T64, ZSTD(1)),
--     hist_map_state       AggregateFunction(sumMap, Float64, UInt64) CODEC(ZSTD(3)),
--     hist_min             SimpleAggregateFunction(min, Float64) CODEC(Gorilla, ZSTD(1)),
--     hist_max             SimpleAggregateFunction(max, Float64) CODEC(Gorilla, ZSTD(1)),

--     -- Dimensions
--     service              LowCardinality(String) CODEC(ZSTD(1)),
--     host                 LowCardinality(String) CODEC(ZSTD(1)),
--     environment          LowCardinality(String) CODEC(ZSTD(1)),
--     k8s_namespace        LowCardinality(String) CODEC(ZSTD(1)),
--     http_method          LowCardinality(String) CODEC(ZSTD(1)),
--     http_status_code     UInt16 CODEC(T64, ZSTD(1)),

--     resource             SimpleAggregateFunction(anyLast, JSON(max_dynamic_paths=100)) CODEC(ZSTD(1)),
--     attributes           SimpleAggregateFunction(anyLast, JSON(max_dynamic_paths=100)) CODEC(ZSTD(1))
-- )
-- ENGINE = AggregatingMergeTree()
-- PARTITION BY toYYYYMMDD(timestamp)
-- ORDER BY (team_id, metric_name, fingerprint, timestamp)
-- TTL timestamp + INTERVAL 90 DAY DELETE
-- SETTINGS
--     index_granularity = 8192,
--     enable_mixed_granularity_parts = 1;

-- CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_1m_mv
-- TO observability.metrics_1m
-- AS
-- SELECT
--     team_id,
--     metric_name,
--     metric_type,
--     any(temporality) AS temporality,            -- constant per metric
--     any(is_monotonic) AS is_monotonic,
--     any(unit) AS unit,
--     any(description) AS description,
--     fingerprint,
--     toStartOfMinute(timestamp) AS timestamp,

--     -- ------------------------------------------------------------------
--     -- Gauge / Delta (including delta counters → sum of deltas)
--     -- ------------------------------------------------------------------
--     sumIf(value, metric_type IN ('Gauge', 'Summonotonic')) AS gauge_sum,
--     countIf(value, metric_type IN ('Gauge', 'Summonotonic')) AS gauge_count,
--     minIf(value, metric_type IN ('Gauge', 'Summonotonic')) AS gauge_min,
--     maxIf(value, metric_type IN ('Gauge', 'Summonotonic')) AS gauge_max,

--     -- Cumulative counter → last value to later compute rate over windows
--     anyLastIf(value, metric_type IN ('SumCumulative')) AS counter_last,

--     -- ------------------------------------------------------------------
--     -- Histogram (correct merge of bucket distributions)
--     -- ------------------------------------------------------------------
--     sumIf(hist_sum, metric_type = 'Histogram') AS hist_sum,
--     sumIf(hist_count, metric_type = 'Histogram') AS hist_count,
--     sumMapStateIf(
--         arrayMap(x -> x::Float64, hist_buckets),   -- bucket boundaries as keys
--         arrayMap(x -> x::UInt64, hist_counts)      -- bucket counts as values
--         metric_type = 'Histogram'
--     ) AS hist_map_state,
--     minIf(arrayFirst(x -> x, hist_counts) ? hist_buckets[1] : NULL,
--           metric_type = 'Histogram') AS hist_min,  -- first non‑empty bucket lower bound
--     maxIf(arrayLast(x -> x, hist_counts) ? hist_buckets[-1] : NULL,
--           metric_type = 'Histogram') AS hist_max,  -- last non‑empty bucket upper bound

--     -- ------------------------------------------------------------------
--     -- Dimensions
--     -- ------------------------------------------------------------------
--     any(service) AS service,
--     any(host) AS host,
--     any(environment) AS environment,
--     any(k8s_namespace) AS k8s_namespace,
--     any(http_method) AS http_method,
--     any(http_status_code) AS http_status_code,
--     anyLast(resource) AS resource,
--     anyLast(attributes) AS attributes

-- FROM observability.metrics
-- WHERE metric_type IN ('Gauge', 'SumDelta', 'SumCumulative', 'Histogram')
-- GROUP BY
--     team_id,
--     metric_name,
--     fingerprint,
--     metric_type,         -- treat different types in the same metric group separately
--     toStartOfMinute(timestamp);