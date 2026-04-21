CREATE TABLE IF NOT EXISTS observability.alert_events (
    ts             DateTime64(3) CODEC(DoubleDelta, LZ4),
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    alert_id       Int64 CODEC(T64, ZSTD(1)),
    instance_key   String CODEC(ZSTD(1)),
    kind           LowCardinality(String),
    from_state     LowCardinality(String),
    to_state       LowCardinality(String),
    values         String CODEC(ZSTD(1)),
    actor_user_id  Int64 CODEC(T64, ZSTD(1)),
    message        String CODEC(ZSTD(1)),
    deploy_refs    String CODEC(ZSTD(1)),
    transition_id  Int64 CODEC(T64, ZSTD(1))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (team_id, alert_id, ts)
SETTINGS index_granularity = 8192;
