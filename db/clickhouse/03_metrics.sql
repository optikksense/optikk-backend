CREATE TABLE IF NOT EXISTS observability.metrics (
    team_id              UInt32 CODEC(T64, ZSTD(1)),
    env                  LowCardinality(String) DEFAULT 'default',
    metric_name          LowCardinality(String),
    metric_type          LowCardinality(String),
    temporality          LowCardinality(String) DEFAULT 'Unspecified',
    is_monotonic         Bool CODEC(T64, ZSTD(1)),
    unit                 LowCardinality(String) DEFAULT '',
    description          LowCardinality(String) DEFAULT '',
    resource_fingerprint String CODEC(ZSTD(3)),
    timestamp            DateTime64(3) CODEC(DoubleDelta, LZ4),
    value                Float64 CODEC(Gorilla, ZSTD(1)),
    hist_sum             Float64 CODEC(Gorilla, ZSTD(1)),
    hist_count           UInt64 CODEC(T64, ZSTD(1)),
    hist_buckets         Array(Float64) CODEC(ZSTD(1)),
    hist_counts          Array(UInt64) CODEC(T64, ZSTD(1)),
    attributes           JSON(max_dynamic_paths=100) CODEC(ZSTD(1)),
    INDEX idx_fingerprint      resource_fingerprint TYPE bloom_filter  GRANULARITY 4
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (team_id, toStartOfHour(timestamp), resource_fingerprint, metric_name, temporality, timestamp)
TTL timestamp + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    enable_mixed_granularity_parts = 1,
    non_replicated_deduplication_window = 1000;
