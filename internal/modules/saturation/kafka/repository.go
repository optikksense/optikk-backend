package kafka

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const messagingRollupPrefix = "observability.messaging_histograms_rollup"

// rollupLatencyBucket returns a bucket expression that works on the rollup's
// `bucket_ts` column. Matches the raw-path `timebucket.Expression` step.
func rollupLatencyBucket(startMs, endMs int64) string {
	return timebucket.ExprForColumnTime(startMs, endMs, "bucket_ts")
}

// timeBucketExpr returns the adaptive time-bucket expression for the metrics table timestamp column.
func timeBucketExpr(startMs, endMs int64) string {
	return timebucket.Expression(startMs, endMs)
}

// bucketSecs returns the bucket width in seconds matching the adaptive strategy.
func bucketSecs(startMs, endMs int64) float64 {
	durationSecs := (endMs - startMs) / 1000
	if durationSecs <= 3600 {
		return 60.0
	} else if durationSecs <= 86400 {
		return 300.0
	}
	return 3600.0
}

// NOTE: the histogram-latency methods (`GetPublishLatencyByTopic`,
// `GetReceiveLatencyByTopic`, `GetProcessLatencyByGroup`,
// `GetClientOperationDuration`) read the `messaging_histograms_rollup_*`
// cascade.
//
// The remaining methods below intentionally stay on raw `observability.metrics`
// for one of three reasons:
//
//  1. Counter metrics (publish/receive/process message counts, error counts):
//     `messaging_histograms_rollup` filters `metric_type = 'Histogram'` so
//     counter rows never land there. Migrating requires a new rollup shape.
//
//  2. Gauge metrics (consumer lag, broker connections, rebalance counts,
//     assigned partitions): same metric_type filter — not in the rollup.
//
//  3. Dims outside the rollup's key set (broker / server.address, partition,
//     node_id, error.type, client-id): the rollup would collapse these, so
//     broker-scoped / partition-scoped / error-typed queries can't use it.
//
// Consolidating these requires either a new messaging-counters rollup or an
// extension of `messaging_histograms_rollup` key columns. Deferred to a
// follow-up.

func (r *ClickHouseRepository) GetKafkaSummaryStats(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) (KafkaSummaryStats, error) {
	durationSecs := float64(endMs-startMs) / 1000.0
	if durationSecs <= 0 {
		durationSecs = 1.0
	}
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    sumIf(%[1]s, %[2]s IN @producerMetrics) / @durationSecs  AS publish_rate,
		    sumIf(%[1]s, %[2]s IN @consumerMetrics) / @durationSecs  AS receive_rate,
		    maxIf(%[1]s, %[2]s IN @lagMetrics AND isFinite(%[1]s)) AS max_lag,
		    quantileTDigestWeightedIf(0.95)(
		        hist_sum / nullIf(hist_count, 0),
		        hist_count,
		        %[3]s AND metric_type = 'Histogram'
		    ) AS publish_p95,
		    quantileTDigestWeightedIf(0.95)(
		        hist_sum / nullIf(hist_count, 0),
		        hist_count,
		        %[4]s AND metric_type = 'Histogram'
		    ) AS receive_p95
		FROM %[5]s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %[6]s
		  AND (%[2]s IN (@producerMetrics, @consumerMetrics, @lagMetrics) OR %[3]s OR %[4]s)
	`,
		ColValue, ColMetricName,
		publishDurationCondition(),
		receiveDurationCondition(),
		TableMetrics,
		filterSQL,
	)

	args := append(r.baseParams(teamID, startMs, endMs), clickhouse.Named("durationSecs", durationSecs))
	args = append(args, filterArgs...)
	var result KafkaSummaryStats
	return result, r.db.QueryRow(dbutil.OverviewCtx(ctx), query, args...).ScanStruct(&result)
}

func (r *ClickHouseRepository) GetProduceRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	topic := topicExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic,
		    sum(%s) / @bucketSecs AS rate_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN @producerMetrics
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, topic, ColValue, TableMetrics, filterSQL, ColMetricName)

	args := append(r.baseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []TopicRatePoint
	return out, r.db.Select(ctx, &out, query, args...)
}

// GetPublishLatencyByTopic reads publish-duration percentiles from
// `messaging_histograms_rollup_*` grouped by topic (= `messaging_destination`).
// Semantic note: the rollup's `messaging_destination` comes from the canonical
// `messaging.destination.name` attribute only — topic aliases captured by
// `topicExpr()` are lost. If ingestion writes only non-canonical topic keys,
// this query returns empty. Verify at rollup.
func (r *ClickHouseRepository) GetPublishLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicLatencyPoint, error) {
	table, _ := rollup.TierTableFor(messagingRollupPrefix, startMs, endMs)
	bucket := rollupLatencyBucket(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                   AS time_bucket,
		    messaging_destination                                                AS topic,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1  AS p50,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2  AS p95,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3  AS p99
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND (metric_name = @publishDuration
		       OR (metric_name = @opDuration AND lower(messaging_operation) IN @publishOps))
		  %s
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, table, rollupTopicGroupFilter(f))

	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	var out []TopicLatencyPoint
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

func (r *ClickHouseRepository) GetConsumeRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	topic := topicExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic,
		    sum(%s) / @bucketSecs AS rate_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN @consumerMetrics
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, topic, ColValue, TableMetrics, filterSQL, ColMetricName)

	args := append(r.baseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []TopicRatePoint
	return out, r.db.Select(ctx, &out, query, args...)
}

// GetReceiveLatencyByTopic — rollup-backed, see comment on GetPublishLatencyByTopic.
func (r *ClickHouseRepository) GetReceiveLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicLatencyPoint, error) {
	table, _ := rollup.TierTableFor(messagingRollupPrefix, startMs, endMs)
	bucket := rollupLatencyBucket(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                   AS time_bucket,
		    messaging_destination                                                AS topic,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1  AS p50,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2  AS p95,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3  AS p99
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND (metric_name = @receiveDuration
		       OR (metric_name = @opDuration AND lower(messaging_operation) IN @receiveOps))
		  %s
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, table, rollupTopicGroupFilter(f))

	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	var out []TopicLatencyPoint
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

func (r *ClickHouseRepository) GetConsumeRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	group := consumerGroupExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    sum(%s) / @bucketSecs AS rate_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN @consumerMetrics
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, ColValue, TableMetrics, filterSQL, ColMetricName)

	args := append(r.baseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []GroupRatePoint
	return out, r.db.Select(ctx, &out, query, args...)
}

func (r *ClickHouseRepository) GetProcessRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	group := consumerGroupExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    sum(%s) / @bucketSecs AS rate_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN @processMetrics
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, ColValue, TableMetrics, filterSQL, ColMetricName)

	args := append(r.baseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []GroupRatePoint
	return out, r.db.Select(ctx, &out, query, args...)
}

// GetProcessLatencyByGroup — rollup-backed. Semantic note: rollup's
// `consumer_group` is populated from `messaging.kafka.consumer.group` only;
// other aliases coalesced by `consumerGroupExpr()` are lost.
func (r *ClickHouseRepository) GetProcessLatencyByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupLatencyPoint, error) {
	table, _ := rollup.TierTableFor(messagingRollupPrefix, startMs, endMs)
	bucket := rollupLatencyBucket(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                   AS time_bucket,
		    consumer_group                                                       AS consumer_group,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1  AS p50,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2  AS p95,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3  AS p99
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND (metric_name = @processDuration
		       OR (metric_name = @opDuration AND lower(messaging_operation) IN @processOps))
		  %s
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, table, rollupTopicGroupFilter(f))

	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	var out []GroupLatencyPoint
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

func (r *ClickHouseRepository) GetConsumerLagByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]LagPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	group := consumerGroupExpr()
	topic := topicExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    %s AS topic,
		    avgIf(%s, %s IN @lagMetrics AND isFinite(%s)) AS lag
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN @lagMetrics
		GROUP BY time_bucket, consumer_group, topic
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, topic,
		ColValue, ColMetricName, ColValue,
		TableMetrics,
		filterSQL,
		ColMetricName,
	)

	args := append(r.baseParams(teamID, startMs, endMs), filterArgs...)
	var out []LagPoint
	return out, r.db.Select(ctx, &out, query, args...)
}

func (r *ClickHouseRepository) GetConsumerLagPerPartition(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]PartitionLag, error) {
	topic := topicExpr()
	partition := topicPartitionExpr()
	group := consumerGroupExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)
	lagExpr := "avgIf(value, isFinite(value))"

	query := fmt.Sprintf(`
		SELECT
		    %s AS topic,
		    toInt64OrZero(%s) AS partition,
		    %s AS consumer_group,
		    toInt64(if(isFinite(%s), round(%s), 0)) AS lag
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN @lagMetrics
		GROUP BY topic, partition, consumer_group
		ORDER BY lag DESC
		LIMIT 200
	`, topic, partition, group, lagExpr, lagExpr,
		TableMetrics,
		filterSQL,
		ColMetricName,
	)

	args := append(r.baseParams(teamID, startMs, endMs), filterArgs...)
	var out []PartitionLag
	return out, r.db.Select(ctx, &out, query, args...)
}

func (r *ClickHouseRepository) GetRebalanceSignals(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]RebalancePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	group := consumerGroupExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %[1]s AS time_bucket,
		    %[2]s AS consumer_group,
		    sumIf(%[3]s, %[4]s = '%[5]s') / @bucketSecs AS rebalance_rate,
		    sumIf(%[3]s, %[4]s = '%[6]s') / @bucketSecs AS join_rate,
		    sumIf(%[3]s, %[4]s = '%[7]s') / @bucketSecs AS sync_rate,
		    sumIf(%[3]s, %[4]s = '%[8]s') / @bucketSecs AS heartbeat_rate,
		    sumIf(%[3]s, %[4]s = '%[9]s') / @bucketSecs AS failed_heartbeat_rate,
		    avgIf(%[3]s, %[4]s = '%[10]s' AND isFinite(%[3]s)) AS assigned_partitions
		FROM %[11]s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %[12]s
		  AND %[4]s IN @rebalanceMetrics
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`,
		bucket, group, ColValue, ColMetricName,
		MetricRebalanceCount,
		MetricJoinCount,
		MetricSyncCount,
		MetricHeartbeatCount,
		MetricFailedHeartbeatCount,
		MetricAssignedPartitions,
		TableMetrics,
		filterSQL,
	)

	args := append(r.baseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []RebalancePoint
	return out, r.db.Select(ctx, &out, query, args...)
}

func (r *ClickHouseRepository) GetE2ELatency(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]E2ELatencyPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	topic := topicExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %[1]s AS time_bucket,
		    %[2]s AS topic,
		    quantileTDigestWeightedIf(0.95)(
		        hist_sum / nullIf(hist_count, 0), hist_count,
		    %[3]s AND metric_type = 'Histogram'
		    ) AS publish_p95,
		    quantileTDigestWeightedIf(0.95)(
		        hist_sum / nullIf(hist_count, 0), hist_count,
		        %[4]s AND metric_type = 'Histogram'
		    ) AS receive_p95,
		    quantileTDigestWeightedIf(0.95)(
		        hist_sum / nullIf(hist_count, 0), hist_count,
		        %[5]s AND metric_type = 'Histogram'
		    ) AS process_p95
		FROM %[6]s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %[7]s
		  AND (%[3]s OR %[4]s OR %[5]s)
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`,
		bucket, topic,
		publishDurationCondition(),
		receiveDurationCondition(),
		processDurationCondition(),
		TableMetrics,
		filterSQL,
	)

	args := append(r.baseParams(teamID, startMs, endMs), filterArgs...)
	var out []E2ELatencyPoint
	return out, r.db.Select(ctx, &out, query, args...)
}

func (r *ClickHouseRepository) GetPublishErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return r.getErrorRates(ctx, teamID, startMs, endMs, MetricPublishMessages, "topic", "GetPublishErrors", f)
}

func (r *ClickHouseRepository) GetConsumeErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return r.getGroupErrorRates(ctx, teamID, startMs, endMs, MetricReceiveMessages, "GetConsumeErrors", f)
}

func (r *ClickHouseRepository) GetProcessErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return r.getGroupErrorRates(ctx, teamID, startMs, endMs, MetricProcessMessages, "GetProcessErrors", f)
}

func (r *ClickHouseRepository) GetClientOpErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	errType := attrString(AttrErrorType)
	opName := operationExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS operation_name,
		    %s AS error_type,
		    sum(%s) / @bucketSecs AS error_rate
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s = '%s'
		  AND %s != ''
		GROUP BY time_bucket, operation_name, error_type
		ORDER BY time_bucket ASC, error_rate DESC
	`, bucket, opName, errType, ColValue, TableMetrics,
		filterSQL,
		ColMetricName, MetricClientOperationDuration,
		errType,
	)

	args := append(r.baseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []ErrorRatePoint
	return out, r.db.Select(ctx, &out, query, args...)
}

func (r *ClickHouseRepository) GetBrokerConnections(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]BrokerConnectionPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	broker := attrString(AttrServerAddress)
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS broker,
		    avg(%s) AS connections
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s = '%s'
		GROUP BY time_bucket, broker
		ORDER BY time_bucket ASC, broker ASC
	`, bucket, broker, ColValue, TableMetrics,
		filterSQL,
		ColMetricName, MetricClientConnections,
	)

	args := append(r.baseParams(teamID, startMs, endMs), filterArgs...)
	var out []BrokerConnectionPoint
	return out, r.db.Select(ctx, &out, query, args...)
}

// GetClientOperationDuration — rollup-backed. operation_name comes from the
// canonical `messaging.operation` attribute on the rollup.
func (r *ClickHouseRepository) GetClientOperationDuration(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]ClientOpDurationPoint, error) {
	table, _ := rollup.TierTableFor(messagingRollupPrefix, startMs, endMs)
	bucket := rollupLatencyBucket(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                   AS time_bucket,
		    messaging_operation                                                  AS operation_name,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).1  AS p50,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2  AS p95,
		    quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).3  AS p99
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @opDuration
		  %s
		GROUP BY time_bucket, operation_name
		ORDER BY time_bucket ASC, operation_name ASC
	`, bucket, table, rollupTopicGroupFilter(f))

	args := append(r.rollupBaseParams(teamID, startMs, endMs), rollupTopicGroupArgs(f)...)
	var out []ClientOpDurationPoint
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

func (r *ClickHouseRepository) getErrorRates(ctx context.Context, teamID int64, startMs, endMs int64, metricName, _ string, caller string, f KafkaFilters) ([]ErrorRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	errType := attrString(AttrErrorType)
	topic := topicExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic,
		    %s AS error_type,
		    sum(%s) / @bucketSecs AS error_rate
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s = '%s'
		  AND %s != ''
		GROUP BY time_bucket, topic, error_type
		ORDER BY time_bucket ASC, error_rate DESC
	`, bucket, topic, errType, ColValue, TableMetrics,
		filterSQL,
		ColMetricName, metricName,
		errType,
	)

	args := append(r.baseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []ErrorRatePoint
	if err := r.db.Select(ctx, &out, query, args...); err != nil {
		return nil, fmt.Errorf("%s: %w", caller, err)
	}
	return out, nil
}

func (r *ClickHouseRepository) getGroupErrorRates(ctx context.Context, teamID int64, startMs, endMs int64, metricName, caller string, f KafkaFilters) ([]ErrorRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	errType := attrString(AttrErrorType)
	group := consumerGroupExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    %s AS error_type,
		    sum(%s) / @bucketSecs AS error_rate
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s = '%s'
		  AND %s != ''
		GROUP BY time_bucket, consumer_group, error_type
		ORDER BY time_bucket ASC, error_rate DESC
	`, bucket, group, errType, ColValue, TableMetrics,
		filterSQL,
		ColMetricName, metricName,
		errType,
	)

	args := append(r.baseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []ErrorRatePoint
	if err := r.db.Select(ctx, &out, query, args...); err != nil {
		return nil, fmt.Errorf("%s: %w", caller, err)
	}
	return out, nil
}

func (r *ClickHouseRepository) GetConsumerMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricNames []string) ([]ConsumerMetricSample, error) {
	if len(metricNames) == 0 {
		return []ConsumerMetricSample{}, nil
	}

	bucket := timeBucketExpr(startMs, endMs)
	group := consumerGroupExpr()
	nodeID := nodeIDExpr()
	filterSQL, filterArgs := kafkaInventoryFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    %s AS node_id,
		    %s AS metric_name,
		    avgIf(%s, isFinite(%s)) AS value
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN @metricNames
		  AND %s != ''
		GROUP BY time_bucket, consumer_group, node_id, metric_name
		ORDER BY time_bucket ASC, consumer_group ASC, metric_name ASC
	`, bucket, group, nodeID, ColMetricName, ColValue, ColValue, TableMetrics, filterSQL, ColMetricName, group)

	args := append(r.baseParams(teamID, startMs, endMs), clickhouse.Named("metricNames", metricNames))
	args = append(args, filterArgs...)
	var out []ConsumerMetricSample
	return out, r.db.Select(ctx, &out, query, args...)
}

func (r *ClickHouseRepository) GetTopicMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricNames []string) ([]TopicMetricSample, error) {
	if len(metricNames) == 0 {
		return []TopicMetricSample{}, nil
	}

	bucket := timeBucketExpr(startMs, endMs)
	topic := topicExpr()
	group := consumerGroupExpr()
	filterSQL, filterArgs := kafkaInventoryFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic,
		    %s AS consumer_group,
		    %s AS metric_name,
		    avgIf(%s, isFinite(%s)) AS value
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN @metricNames
		  AND %s != ''
		GROUP BY time_bucket, topic, consumer_group, metric_name
		ORDER BY time_bucket ASC, topic ASC, consumer_group ASC, metric_name ASC
	`, bucket, topic, group, ColMetricName, ColValue, ColValue, TableMetrics, filterSQL, ColMetricName, topic)

	args := append(r.baseParams(teamID, startMs, endMs), clickhouse.Named("metricNames", metricNames))
	args = append(args, filterArgs...)
	var out []TopicMetricSample
	return out, r.db.Select(ctx, &out, query, args...)
}
