package kafka

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

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

// ── 1. Summary stat cards ─────────────────────────────────────────────────────

func (r *ClickHouseRepository) GetKafkaSummaryStats(teamID int64, startMs, endMs int64, f KafkaFilters) (KafkaSummaryStats, error) {
	durationSecs := float64(endMs-startMs) / 1000.0
	if durationSecs <= 0 {
		durationSecs = 1.0
	}
	filterSQL, filterArgs := kafkaFilterClauses(f)
	producerClause := MetricSetToInClause(ProducerMetrics)
	consumerClause := MetricSetToInClause(ConsumerMetrics)
	lagClause := MetricSetToInClause(ConsumerLagMetrics)

	query := fmt.Sprintf(`
		SELECT
		    sumIf(%[1]s, %[2]s IN (%[3]s)) / @durationSecs  AS publish_rate,
		    sumIf(%[1]s, %[2]s IN (%[4]s)) / @durationSecs  AS receive_rate,
		    maxIf(%[1]s, %[2]s IN (%[5]s) AND isFinite(%[1]s)) AS max_lag,
		    quantileExactWeightedIf(0.95)(
		        hist_sum / nullIf(hist_count, 0),
		        hist_count,
		        %[6]s AND metric_type = 'Histogram'
		    ) AS publish_p95,
		    quantileExactWeightedIf(0.95)(
		        hist_sum / nullIf(hist_count, 0),
		        hist_count,
		        %[7]s AND metric_type = 'Histogram'
		    ) AS receive_p95
		FROM %[8]s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %[9]s
		  AND (%[2]s IN (%[3]s, %[4]s, %[5]s) OR %[6]s OR %[7]s)
	`,
		ColValue, ColMetricName,
		producerClause,
		consumerClause,
		lagClause,
		publishDurationCondition(),
		receiveDurationCondition(),
		TableMetrics,
		filterSQL,
	)

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), clickhouse.Named("durationSecs", durationSecs))
	args = append(args, filterArgs...)
	var result KafkaSummaryStats
	return result, r.db.QueryRow(context.Background(), &result, query, args...)
}

// ── 2. Produce rate by topic ──────────────────────────────────────────────────

func (r *ClickHouseRepository) GetProduceRateByTopic(teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	clause := MetricSetToInClause(ProducerMetrics)
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
		  AND %s IN (%s)
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, topic, ColValue, TableMetrics, filterSQL, ColMetricName, clause)

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []TopicRatePoint
	return out, r.db.Select(context.Background(), &out, query, args...)
}

// ── 3. Publish latency by topic ───────────────────────────────────────────────

func (r *ClickHouseRepository) GetPublishLatencyByTopic(teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicLatencyPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	topic := topicExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, topic, TableMetrics, filterSQL, publishDurationCondition())

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), filterArgs...)
	var out []TopicLatencyPoint
	return out, r.db.Select(context.Background(), &out, query, args...)
}

// ── 4. Consume rate by topic ──────────────────────────────────────────────────

func (r *ClickHouseRepository) GetConsumeRateByTopic(teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	clause := MetricSetToInClause(ConsumerMetrics)
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
		  AND %s IN (%s)
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, topic, ColValue, TableMetrics, filterSQL, ColMetricName, clause)

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []TopicRatePoint
	return out, r.db.Select(context.Background(), &out, query, args...)
}

// ── 5. Receive latency by topic ───────────────────────────────────────────────

func (r *ClickHouseRepository) GetReceiveLatencyByTopic(teamID int64, startMs, endMs int64, f KafkaFilters) ([]TopicLatencyPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	topic := topicExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, topic, TableMetrics, filterSQL, receiveDurationCondition())

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), filterArgs...)
	var out []TopicLatencyPoint
	return out, r.db.Select(context.Background(), &out, query, args...)
}

// ── 6. Consume rate by consumer group ────────────────────────────────────────

func (r *ClickHouseRepository) GetConsumeRateByGroup(teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	clause := MetricSetToInClause(ConsumerMetrics)
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
		  AND %s IN (%s)
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, ColValue, TableMetrics, filterSQL, ColMetricName, clause)

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []GroupRatePoint
	return out, r.db.Select(context.Background(), &out, query, args...)
}

// ── 7. Process rate by consumer group ────────────────────────────────────────

func (r *ClickHouseRepository) GetProcessRateByGroup(teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	clause := MetricSetToInClause(ProcessMetrics)
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
		  AND %s IN (%s)
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, ColValue, TableMetrics, filterSQL, ColMetricName, clause)

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []GroupRatePoint
	return out, r.db.Select(context.Background(), &out, query, args...)
}

// ── 8. Process latency by consumer group ─────────────────────────────────────

func (r *ClickHouseRepository) GetProcessLatencyByGroup(teamID int64, startMs, endMs int64, f KafkaFilters) ([]GroupLatencyPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	group := consumerGroupExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, TableMetrics, filterSQL, processDurationCondition())

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), filterArgs...)
	var out []GroupLatencyPoint
	return out, r.db.Select(context.Background(), &out, query, args...)
}

// ── 9. Consumer lag by group ──────────────────────────────────────────────────

func (r *ClickHouseRepository) GetConsumerLagByGroup(teamID int64, startMs, endMs int64, f KafkaFilters) ([]LagPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	clause := MetricSetToInClause(ConsumerLagMetrics)
	group := consumerGroupExpr()
	topic := topicExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    %s AS topic,
		    avgIf(%s, %s IN (%s) AND isFinite(%s)) AS lag
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN (%s)
		GROUP BY time_bucket, consumer_group, topic
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, topic,
		ColValue, ColMetricName, clause, ColValue,
		TableMetrics,
		filterSQL,
		ColMetricName, clause,
	)

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), filterArgs...)
	var out []LagPoint
	return out, r.db.Select(context.Background(), &out, query, args...)
}

// ── 10. Consumer lag per partition ────────────────────────────────────────────

func (r *ClickHouseRepository) GetConsumerLagPerPartition(teamID int64, startMs, endMs int64, f KafkaFilters) ([]PartitionLag, error) {
	clause := MetricSetToInClause(ConsumerLagMetrics)
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
		  AND %s IN (%s)
		GROUP BY topic, partition, consumer_group
		ORDER BY lag DESC
		LIMIT 200
	`, topic, partition, group, lagExpr, lagExpr,
		TableMetrics,
		filterSQL,
		ColMetricName, clause,
	)

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), filterArgs...)
	var out []PartitionLag
	return out, r.db.Select(context.Background(), &out, query, args...)
}

// ── 11. Rebalance signals by group ────────────────────────────────────────────

func (r *ClickHouseRepository) GetRebalanceSignals(teamID int64, startMs, endMs int64, f KafkaFilters) ([]RebalancePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	group := consumerGroupExpr()
	allClause := MetricSetToInClause(RebalanceMetrics)
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
		  %[13]s
		  AND %[4]s IN (%[12]s)
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
		allClause,
		filterSQL,
	)

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []RebalancePoint
	return out, r.db.Select(context.Background(), &out, query, args...)
}

// ── 12. End-to-end latency p95 by topic ──────────────────────────────────────

func (r *ClickHouseRepository) GetE2ELatency(teamID int64, startMs, endMs int64, f KafkaFilters) ([]E2ELatencyPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	topic := topicExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %[1]s AS time_bucket,
		    %[2]s AS topic,
		    quantileExactWeightedIf(0.95)(
		        hist_sum / nullIf(hist_count, 0), hist_count,
		    %[3]s AND metric_type = 'Histogram'
		    ) AS publish_p95,
		    quantileExactWeightedIf(0.95)(
		        hist_sum / nullIf(hist_count, 0), hist_count,
		        %[4]s AND metric_type = 'Histogram'
		    ) AS receive_p95,
		    quantileExactWeightedIf(0.95)(
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

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), filterArgs...)
	var out []E2ELatencyPoint
	return out, r.db.Select(context.Background(), &out, query, args...)
}

// ── 13. Publish errors by error type ─────────────────────────────────────────

func (r *ClickHouseRepository) GetPublishErrors(teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return r.getErrorRates(teamID, startMs, endMs, MetricPublishMessages, "topic", "GetPublishErrors", f)
}

// ── 14. Consume errors by error type ─────────────────────────────────────────

func (r *ClickHouseRepository) GetConsumeErrors(teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return r.getGroupErrorRates(teamID, startMs, endMs, MetricReceiveMessages, "GetConsumeErrors", f)
}

// ── 15. Process errors by error type ─────────────────────────────────────────

func (r *ClickHouseRepository) GetProcessErrors(teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
	return r.getGroupErrorRates(teamID, startMs, endMs, MetricProcessMessages, "GetProcessErrors", f)
}

// ── 16. Client operation errors ───────────────────────────────────────────────

func (r *ClickHouseRepository) GetClientOpErrors(teamID int64, startMs, endMs int64, f KafkaFilters) ([]ErrorRatePoint, error) {
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

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []ErrorRatePoint
	return out, r.db.Select(context.Background(), &out, query, args...)
}

// ── 17. Broker connections ────────────────────────────────────────────────────

func (r *ClickHouseRepository) GetBrokerConnections(teamID int64, startMs, endMs int64, f KafkaFilters) ([]BrokerConnectionPoint, error) {
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

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), filterArgs...)
	var out []BrokerConnectionPoint
	return out, r.db.Select(context.Background(), &out, query, args...)
}

// ── 18. Client operation duration ─────────────────────────────────────────────

func (r *ClickHouseRepository) GetClientOperationDuration(teamID int64, startMs, endMs int64, f KafkaFilters) ([]ClientOpDurationPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	opName := operationExpr()
	filterSQL, filterArgs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS operation_name,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, operation_name
		ORDER BY time_bucket ASC, operation_name ASC
	`, bucket, opName, TableMetrics, filterSQL, ColMetricName, MetricClientOperationDuration)

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), filterArgs...)
	var out []ClientOpDurationPoint
	return out, r.db.Select(context.Background(), &out, query, args...)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

func (r *ClickHouseRepository) getErrorRates(teamID int64, startMs, endMs int64, metricName, _ string, caller string, f KafkaFilters) ([]ErrorRatePoint, error) {
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

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []ErrorRatePoint
	if err := r.db.Select(context.Background(), &out, query, args...); err != nil {
		return nil, fmt.Errorf("%s: %w", caller, err)
	}
	return out, nil
}

func (r *ClickHouseRepository) getGroupErrorRates(teamID int64, startMs, endMs int64, metricName, caller string, f KafkaFilters) ([]ErrorRatePoint, error) {
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

	args := append(database.SimpleBaseParams(teamID, startMs, endMs), clickhouse.Named("bucketSecs", bs))
	args = append(args, filterArgs...)
	var out []ErrorRatePoint
	if err := r.db.Select(context.Background(), &out, query, args...); err != nil {
		return nil, fmt.Errorf("%s: %w", caller, err)
	}
	return out, nil
}
