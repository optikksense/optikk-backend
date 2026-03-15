package kafka

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
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

func (r *ClickHouseRepository) GetKafkaSummaryStats(teamID int64, startMs, endMs int64) (KafkaSummaryStats, error) {
	durationSecs := float64(endMs-startMs) / 1000.0
	if durationSecs <= 0 {
		durationSecs = 1.0
	}
	producerClause := MetricSetToInClause(ProducerMetrics)
	consumerClause := MetricSetToInClause(ConsumerMetrics)
	lagClause := MetricSetToInClause(ConsumerLagMetrics)

	query := fmt.Sprintf(`
		SELECT
		    sumIf(%[1]s, %[2]s IN (%[3]s)) / ?  AS publish_rate,
		    sumIf(%[1]s, %[2]s IN (%[4]s)) / ?  AS receive_rate,
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
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND (%[2]s IN (%[3]s, %[4]s, %[5]s) OR %[6]s OR %[7]s)
	`,
		ColValue, ColMetricName,
		producerClause,
		consumerClause,
		lagClause,
		publishDurationCondition(),
		receiveDurationCondition(),
		TableMetrics,
	)

	row, err := dbutil.QueryMap(r.db, query,
		durationSecs, durationSecs,
		uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs),
	)
	if err != nil {
		return KafkaSummaryStats{}, fmt.Errorf("GetKafkaSummaryStats: %w", err)
	}
	return KafkaSummaryStats{
		PublishRatePerSec: dbutil.Float64FromAny(row["publish_rate"]),
		ReceiveRatePerSec: dbutil.Float64FromAny(row["receive_rate"]),
		MaxLag:            dbutil.Float64FromAny(row["max_lag"]),
		PublishP95Ms:      dbutil.Float64FromAny(row["publish_p95"]),
		ReceiveP95Ms:      dbutil.Float64FromAny(row["receive_p95"]),
	}, nil
}

// ── 2. Produce rate by topic ──────────────────────────────────────────────────

func (r *ClickHouseRepository) GetProduceRateByTopic(teamID int64, startMs, endMs int64) ([]TopicRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	clause := MetricSetToInClause(ProducerMetrics)
	topic := topicExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic,
		    sum(%s) / ? AS rate_per_sec
		FROM %s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, topic, ColValue, TableMetrics, ColMetricName, clause)

	rows, err := dbutil.QueryMaps(r.db, query, bs, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetProduceRateByTopic: %w", err)
	}
	out := make([]TopicRatePoint, len(rows))
	for i, row := range rows {
		out[i] = TopicRatePoint{
			Timestamp:  dbutil.StringFromAny(row["time_bucket"]),
			Topic:      dbutil.StringFromAny(row["topic"]),
			RatePerSec: dbutil.Float64FromAny(row["rate_per_sec"]),
		}
	}
	return out, nil
}

// ── 3. Publish latency by topic ───────────────────────────────────────────────

func (r *ClickHouseRepository) GetPublishLatencyByTopic(teamID int64, startMs, endMs int64) ([]TopicLatencyPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	topic := topicExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99
		FROM %s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %s
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, topic, TableMetrics, publishDurationCondition())

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetPublishLatencyByTopic: %w", err)
	}
	out := make([]TopicLatencyPoint, len(rows))
	for i, row := range rows {
		out[i] = TopicLatencyPoint{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Topic:     dbutil.StringFromAny(row["topic"]),
			P50Ms:     dbutil.Float64FromAny(row["p50"]),
			P95Ms:     dbutil.Float64FromAny(row["p95"]),
			P99Ms:     dbutil.Float64FromAny(row["p99"]),
		}
	}
	return out, nil
}

// ── 4. Consume rate by topic ──────────────────────────────────────────────────

func (r *ClickHouseRepository) GetConsumeRateByTopic(teamID int64, startMs, endMs int64) ([]TopicRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	clause := MetricSetToInClause(ConsumerMetrics)
	topic := topicExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic,
		    sum(%s) / ? AS rate_per_sec
		FROM %s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, topic, ColValue, TableMetrics, ColMetricName, clause)

	rows, err := dbutil.QueryMaps(r.db, query, bs, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetConsumeRateByTopic: %w", err)
	}
	out := make([]TopicRatePoint, len(rows))
	for i, row := range rows {
		out[i] = TopicRatePoint{
			Timestamp:  dbutil.StringFromAny(row["time_bucket"]),
			Topic:      dbutil.StringFromAny(row["topic"]),
			RatePerSec: dbutil.Float64FromAny(row["rate_per_sec"]),
		}
	}
	return out, nil
}

// ── 5. Receive latency by topic ───────────────────────────────────────────────

func (r *ClickHouseRepository) GetReceiveLatencyByTopic(teamID int64, startMs, endMs int64) ([]TopicLatencyPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	topic := topicExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99
		FROM %s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %s
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, topic, TableMetrics, receiveDurationCondition())

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetReceiveLatencyByTopic: %w", err)
	}
	out := make([]TopicLatencyPoint, len(rows))
	for i, row := range rows {
		out[i] = TopicLatencyPoint{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Topic:     dbutil.StringFromAny(row["topic"]),
			P50Ms:     dbutil.Float64FromAny(row["p50"]),
			P95Ms:     dbutil.Float64FromAny(row["p95"]),
			P99Ms:     dbutil.Float64FromAny(row["p99"]),
		}
	}
	return out, nil
}

// ── 6. Consume rate by consumer group ────────────────────────────────────────

func (r *ClickHouseRepository) GetConsumeRateByGroup(teamID int64, startMs, endMs int64) ([]GroupRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	clause := MetricSetToInClause(ConsumerMetrics)
	group := consumerGroupExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    sum(%s) / ? AS rate_per_sec
		FROM %s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, ColValue, TableMetrics, ColMetricName, clause)

	rows, err := dbutil.QueryMaps(r.db, query, bs, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetConsumeRateByGroup: %w", err)
	}
	out := make([]GroupRatePoint, len(rows))
	for i, row := range rows {
		out[i] = GroupRatePoint{
			Timestamp:     dbutil.StringFromAny(row["time_bucket"]),
			ConsumerGroup: dbutil.StringFromAny(row["consumer_group"]),
			RatePerSec:    dbutil.Float64FromAny(row["rate_per_sec"]),
		}
	}
	return out, nil
}

// ── 7. Process rate by consumer group ────────────────────────────────────────

func (r *ClickHouseRepository) GetProcessRateByGroup(teamID int64, startMs, endMs int64) ([]GroupRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	clause := MetricSetToInClause(ProcessMetrics)
	group := consumerGroupExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    sum(%s) / ? AS rate_per_sec
		FROM %s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, ColValue, TableMetrics, ColMetricName, clause)

	rows, err := dbutil.QueryMaps(r.db, query, bs, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetProcessRateByGroup: %w", err)
	}
	out := make([]GroupRatePoint, len(rows))
	for i, row := range rows {
		out[i] = GroupRatePoint{
			Timestamp:     dbutil.StringFromAny(row["time_bucket"]),
			ConsumerGroup: dbutil.StringFromAny(row["consumer_group"]),
			RatePerSec:    dbutil.Float64FromAny(row["rate_per_sec"]),
		}
	}
	return out, nil
}

// ── 8. Process latency by consumer group ─────────────────────────────────────

func (r *ClickHouseRepository) GetProcessLatencyByGroup(teamID int64, startMs, endMs int64) ([]GroupLatencyPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	group := consumerGroupExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99
		FROM %s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %s
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, TableMetrics, processDurationCondition())

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetProcessLatencyByGroup: %w", err)
	}
	out := make([]GroupLatencyPoint, len(rows))
	for i, row := range rows {
		out[i] = GroupLatencyPoint{
			Timestamp:     dbutil.StringFromAny(row["time_bucket"]),
			ConsumerGroup: dbutil.StringFromAny(row["consumer_group"]),
			P50Ms:         dbutil.Float64FromAny(row["p50"]),
			P95Ms:         dbutil.Float64FromAny(row["p95"]),
			P99Ms:         dbutil.Float64FromAny(row["p99"]),
		}
	}
	return out, nil
}

// ── 9. Consumer lag by group ──────────────────────────────────────────────────

func (r *ClickHouseRepository) GetConsumerLagByGroup(teamID int64, startMs, endMs int64) ([]LagPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	clause := MetricSetToInClause(ConsumerLagMetrics)
	group := consumerGroupExpr()
	topic := topicExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    %s AS topic,
		    avgIf(%s, %s IN (%s) AND isFinite(%s)) AS lag
		FROM %s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY time_bucket, consumer_group, topic
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, topic,
		ColValue, ColMetricName, clause, ColValue,
		TableMetrics,
		ColMetricName, clause,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetConsumerLagByGroup: %w", err)
	}
	out := make([]LagPoint, len(rows))
	for i, row := range rows {
		out[i] = LagPoint{
			Timestamp:     dbutil.StringFromAny(row["time_bucket"]),
			ConsumerGroup: dbutil.StringFromAny(row["consumer_group"]),
			Topic:         dbutil.StringFromAny(row["topic"]),
			Lag:           dbutil.Float64FromAny(row["lag"]),
		}
	}
	return out, nil
}

// ── 10. Consumer lag per partition ────────────────────────────────────────────

func (r *ClickHouseRepository) GetConsumerLagPerPartition(teamID int64, startMs, endMs int64) ([]PartitionLag, error) {
	clause := MetricSetToInClause(ConsumerLagMetrics)
	topic := attrString(AttrMessagingDestinationName)
	partition := attrString(AttrMessagingKafkaDestinationPartition)
	group := consumerGroupExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s AS topic,
		    toInt64(%s) AS partition,
		    %s AS consumer_group,
		    toInt64(avg(value)) AS lag
		FROM %s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY topic, partition, consumer_group
		ORDER BY lag DESC
		LIMIT 200
	`, topic, partition, group,
		TableMetrics,
		ColMetricName, clause,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetConsumerLagPerPartition: %w", err)
	}
	out := make([]PartitionLag, len(rows))
	for i, row := range rows {
		out[i] = PartitionLag{
			Topic:         dbutil.StringFromAny(row["topic"]),
			Partition:     dbutil.Int64FromAny(row["partition"]),
			ConsumerGroup: dbutil.StringFromAny(row["consumer_group"]),
			Lag:           dbutil.Int64FromAny(row["lag"]),
		}
	}
	return out, nil
}

// ── 11. Rebalance signals by group ────────────────────────────────────────────

func (r *ClickHouseRepository) GetRebalanceSignals(teamID int64, startMs, endMs int64) ([]RebalancePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	group := consumerGroupExpr()
	allClause := MetricSetToInClause(RebalanceMetrics)

	query := fmt.Sprintf(`
		SELECT
		    %[1]s AS time_bucket,
		    %[2]s AS consumer_group,
		    sumIf(%[3]s, %[4]s = '%[5]s') / ? AS rebalance_rate,
		    sumIf(%[3]s, %[4]s = '%[6]s') / ? AS join_rate,
		    sumIf(%[3]s, %[4]s = '%[7]s') / ? AS sync_rate,
		    sumIf(%[3]s, %[4]s = '%[8]s') / ? AS heartbeat_rate,
		    sumIf(%[3]s, %[4]s = '%[9]s') / ? AS failed_heartbeat_rate,
		    avgIf(%[3]s, %[4]s = '%[10]s' AND isFinite(%[3]s)) AS assigned_partitions
		FROM %[11]s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
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
	)

	rows, err := dbutil.QueryMaps(r.db, query,
		bs, bs, bs, bs, bs,
		uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs),
	)
	if err != nil {
		return nil, fmt.Errorf("GetRebalanceSignals: %w", err)
	}
	out := make([]RebalancePoint, len(rows))
	for i, row := range rows {
		out[i] = RebalancePoint{
			Timestamp:           dbutil.StringFromAny(row["time_bucket"]),
			ConsumerGroup:       dbutil.StringFromAny(row["consumer_group"]),
			RebalanceRate:       dbutil.Float64FromAny(row["rebalance_rate"]),
			JoinRate:            dbutil.Float64FromAny(row["join_rate"]),
			SyncRate:            dbutil.Float64FromAny(row["sync_rate"]),
			HeartbeatRate:       dbutil.Float64FromAny(row["heartbeat_rate"]),
			FailedHeartbeatRate: dbutil.Float64FromAny(row["failed_heartbeat_rate"]),
			AssignedPartitions:  dbutil.Float64FromAny(row["assigned_partitions"]),
		}
	}
	return out, nil
}

// ── 12. End-to-end latency p95 by topic ──────────────────────────────────────

func (r *ClickHouseRepository) GetE2ELatency(teamID int64, startMs, endMs int64) ([]E2ELatencyPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	topic := topicExpr()

	query := fmt.Sprintf(`
		SELECT
		    %[1]s AS time_bucket,
		    %[2]s AS topic,
		    quantileExactWeightedIf(0.95)(
		        hist_sum / nullIf(hist_count, 0), hist_count,
		        %[3]s = '%[4]s' AND metric_type = 'Histogram'
		    ) AS publish_p95,
		    quantileExactWeightedIf(0.95)(
		        hist_sum / nullIf(hist_count, 0), hist_count,
		        %[3]s = '%[5]s' AND metric_type = 'Histogram'
		    ) AS receive_p95,
		    quantileExactWeightedIf(0.95)(
		        hist_sum / nullIf(hist_count, 0), hist_count,
		        %[3]s = '%[6]s' AND metric_type = 'Histogram'
		    ) AS process_p95
		FROM %[7]s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %[3]s IN ('%[4]s', '%[5]s', '%[6]s')
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`,
		bucket, topic, ColMetricName,
		MetricPublishDuration,
		MetricReceiveDuration,
		MetricProcessDuration,
		TableMetrics,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetE2ELatency: %w", err)
	}
	out := make([]E2ELatencyPoint, len(rows))
	for i, row := range rows {
		out[i] = E2ELatencyPoint{
			Timestamp:    dbutil.StringFromAny(row["time_bucket"]),
			Topic:        dbutil.StringFromAny(row["topic"]),
			PublishP95Ms: dbutil.Float64FromAny(row["publish_p95"]),
			ReceiveP95Ms: dbutil.Float64FromAny(row["receive_p95"]),
			ProcessP95Ms: dbutil.Float64FromAny(row["process_p95"]),
		}
	}
	return out, nil
}

// ── 13. Publish errors by error type ─────────────────────────────────────────

func (r *ClickHouseRepository) GetPublishErrors(teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	return r.getErrorRates(teamID, startMs, endMs, MetricPublishMessages, "topic", "GetPublishErrors")
}

// ── 14. Consume errors by error type ─────────────────────────────────────────

func (r *ClickHouseRepository) GetConsumeErrors(teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	return r.getGroupErrorRates(teamID, startMs, endMs, MetricReceiveMessages, "GetConsumeErrors")
}

// ── 15. Process errors by error type ─────────────────────────────────────────

func (r *ClickHouseRepository) GetProcessErrors(teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	return r.getGroupErrorRates(teamID, startMs, endMs, MetricProcessMessages, "GetProcessErrors")
}

// ── 16. Client operation errors ───────────────────────────────────────────────

func (r *ClickHouseRepository) GetClientOpErrors(teamID int64, startMs, endMs int64) ([]ErrorRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	errType := attrString(AttrErrorType)
	opName := operationExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS operation_name,
		    %s AS error_type,
		    sum(%s) / ? AS error_rate
		FROM %s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %s = '%s'
		  AND %s != ''
		GROUP BY time_bucket, operation_name, error_type
		ORDER BY time_bucket ASC, error_rate DESC
	`, bucket, opName, errType, ColValue, TableMetrics,
		ColMetricName, MetricClientOperationDuration,
		errType,
	)

	rows, err := dbutil.QueryMaps(r.db, query, bs, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetClientOpErrors: %w", err)
	}
	out := make([]ErrorRatePoint, len(rows))
	for i, row := range rows {
		out[i] = ErrorRatePoint{
			Timestamp:     dbutil.StringFromAny(row["time_bucket"]),
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			ErrorType:     dbutil.StringFromAny(row["error_type"]),
			ErrorRate:     dbutil.Float64FromAny(row["error_rate"]),
		}
	}
	return out, nil
}

// ── 17. Broker connections ────────────────────────────────────────────────────

func (r *ClickHouseRepository) GetBrokerConnections(teamID int64, startMs, endMs int64) ([]BrokerConnectionPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	broker := attrString(AttrServerAddress)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS broker,
		    avg(%s) AS connections
		FROM %s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY time_bucket, broker
		ORDER BY time_bucket ASC, broker ASC
	`, bucket, broker, ColValue, TableMetrics,
		ColMetricName, MetricClientConnections,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetBrokerConnections: %w", err)
	}
	out := make([]BrokerConnectionPoint, len(rows))
	for i, row := range rows {
		out[i] = BrokerConnectionPoint{
			Timestamp:   dbutil.StringFromAny(row["time_bucket"]),
			Broker:      dbutil.StringFromAny(row["broker"]),
			Connections: dbutil.Float64FromAny(row["connections"]),
		}
	}
	return out, nil
}

// ── 18. Client operation duration ─────────────────────────────────────────────

func (r *ClickHouseRepository) GetClientOperationDuration(teamID int64, startMs, endMs int64) ([]ClientOpDurationPoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	opName := operationExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS operation_name,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99
		FROM %s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, operation_name
		ORDER BY time_bucket ASC, operation_name ASC
	`, bucket, opName, TableMetrics, ColMetricName, MetricClientOperationDuration)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetClientOperationDuration: %w", err)
	}
	out := make([]ClientOpDurationPoint, len(rows))
	for i, row := range rows {
		out[i] = ClientOpDurationPoint{
			Timestamp:     dbutil.StringFromAny(row["time_bucket"]),
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			P50Ms:         dbutil.Float64FromAny(row["p50"]),
			P95Ms:         dbutil.Float64FromAny(row["p95"]),
			P99Ms:         dbutil.Float64FromAny(row["p99"]),
		}
	}
	return out, nil
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// getErrorRates returns error rates per topic+error_type for a given counter metric.
func (r *ClickHouseRepository) getErrorRates(teamID int64, startMs, endMs int64, metricName, _ string, caller string) ([]ErrorRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	errType := attrString(AttrErrorType)
	topic := topicExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic,
		    %s AS error_type,
		    sum(%s) / ? AS error_rate
		FROM %s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %s = '%s'
		  AND %s != ''
		GROUP BY time_bucket, topic, error_type
		ORDER BY time_bucket ASC, error_rate DESC
	`, bucket, topic, errType, ColValue, TableMetrics,
		ColMetricName, metricName,
		errType,
	)

	rows, err := dbutil.QueryMaps(r.db, query, bs, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", caller, err)
	}
	out := make([]ErrorRatePoint, len(rows))
	for i, row := range rows {
		out[i] = ErrorRatePoint{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Topic:     dbutil.StringFromAny(row["topic"]),
			ErrorType: dbutil.StringFromAny(row["error_type"]),
			ErrorRate: dbutil.Float64FromAny(row["error_rate"]),
		}
	}
	return out, nil
}

// getGroupErrorRates returns error rates per consumer_group+error_type for a given counter metric.
func (r *ClickHouseRepository) getGroupErrorRates(teamID int64, startMs, endMs int64, metricName, caller string) ([]ErrorRatePoint, error) {
	bucket := timeBucketExpr(startMs, endMs)
	bs := bucketSecs(startMs, endMs)
	errType := attrString(AttrErrorType)
	group := consumerGroupExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    %s AS error_type,
		    sum(%s) / ? AS error_rate
		FROM %s
		WHERE team_id = ?
		  AND timestamp BETWEEN ? AND ?
		  AND %s = '%s'
		  AND %s != ''
		GROUP BY time_bucket, consumer_group, error_type
		ORDER BY time_bucket ASC, error_rate DESC
	`, bucket, group, errType, ColValue, TableMetrics,
		ColMetricName, metricName,
		errType,
	)

	rows, err := dbutil.QueryMaps(r.db, query, bs, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", caller, err)
	}
	out := make([]ErrorRatePoint, len(rows))
	for i, row := range rows {
		out[i] = ErrorRatePoint{
			Timestamp:     dbutil.StringFromAny(row["time_bucket"]),
			ConsumerGroup: dbutil.StringFromAny(row["consumer_group"]),
			ErrorType:     dbutil.StringFromAny(row["error_type"]),
			ErrorRate:     dbutil.Float64FromAny(row["error_rate"]),
		}
	}
	return out, nil
}
