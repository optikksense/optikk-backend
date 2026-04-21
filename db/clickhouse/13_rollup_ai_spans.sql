-- ai_spans_rollup — GenAI / LLM spans per (system, model, operation, service).
-- Powers: ai module aggregate panels (latency, error rate, token volume).

CREATE TABLE IF NOT EXISTS observability.ai_spans_rollup_1m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    ai_system      LowCardinality(String),
    ai_model       LowCardinality(String),
    ai_operation   LowCardinality(String),
    service_name   LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    input_tokens_sum   AggregateFunction(sum, UInt64),
    output_tokens_sum  AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, ai_system, ai_model, ai_operation, service_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.ai_spans_to_rollup_1m
TO observability.ai_spans_rollup_1m AS
SELECT
    team_id,
    toStartOfMinute(timestamp)                                                                 AS bucket_ts,
    attributes.`gen_ai.system`::String                                                         AS ai_system,
    coalesce(nullIf(attributes.`gen_ai.request.model`::String, ''),
             attributes.`gen_ai.response.model`::String)                                       AS ai_model,
    attributes.`gen_ai.operation.name`::String                                                 AS ai_operation,
    service_name                                                                               AS service_name,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_nano / 1000000.0, toUInt64(1))     AS latency_ms_digest,
    sumState(toUInt64(1))                                                                      AS request_count,
    sumState(toUInt64(has_error OR toUInt16OrZero(response_status_code) >= 500))               AS error_count,
    sumState(toUInt64OrZero(attributes.`gen_ai.usage.input_tokens`::String))                   AS input_tokens_sum,
    sumState(toUInt64OrZero(attributes.`gen_ai.usage.output_tokens`::String))                  AS output_tokens_sum
FROM observability.spans
WHERE attributes.`gen_ai.system`::String != '';

CREATE TABLE IF NOT EXISTS observability.ai_spans_rollup_5m (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    ai_system      LowCardinality(String),
    ai_model       LowCardinality(String),
    ai_operation   LowCardinality(String),
    service_name   LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    input_tokens_sum   AggregateFunction(sum, UInt64),
    output_tokens_sum  AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, ai_system, ai_model, ai_operation, service_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.ai_spans_rollup_1m_to_5m
TO observability.ai_spans_rollup_5m AS
SELECT team_id,
       toStartOfInterval(bucket_ts, toIntervalMinute(5)) AS bucket_ts,
       ai_system, ai_model, ai_operation, service_name,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)     AS request_count,
       sumMergeState(error_count)       AS error_count,
       sumMergeState(input_tokens_sum)  AS input_tokens_sum,
       sumMergeState(output_tokens_sum) AS output_tokens_sum
FROM observability.ai_spans_rollup_1m
GROUP BY team_id, bucket_ts, ai_system, ai_model, ai_operation, service_name;

CREATE TABLE IF NOT EXISTS observability.ai_spans_rollup_1h (
    team_id        UInt32 CODEC(T64, ZSTD(1)),
    bucket_ts      DateTime CODEC(DoubleDelta, LZ4),
    ai_system      LowCardinality(String),
    ai_model       LowCardinality(String),
    ai_operation   LowCardinality(String),
    service_name   LowCardinality(String),
    latency_ms_digest  AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), Float64, UInt64),
    request_count      AggregateFunction(sum, UInt64),
    error_count        AggregateFunction(sum, UInt64),
    input_tokens_sum   AggregateFunction(sum, UInt64),
    output_tokens_sum  AggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (team_id, bucket_ts, ai_system, ai_model, ai_operation, service_name)
TTL bucket_ts + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.ai_spans_rollup_5m_to_1h
TO observability.ai_spans_rollup_1h AS
SELECT team_id,
       toStartOfHour(bucket_ts) AS bucket_ts,
       ai_system, ai_model, ai_operation, service_name,
       quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(latency_ms_digest) AS latency_ms_digest,
       sumMergeState(request_count)     AS request_count,
       sumMergeState(error_count)       AS error_count,
       sumMergeState(input_tokens_sum)  AS input_tokens_sum,
       sumMergeState(output_tokens_sum) AS output_tokens_sum
FROM observability.ai_spans_rollup_5m
GROUP BY team_id, bucket_ts, ai_system, ai_model, ai_operation, service_name;
