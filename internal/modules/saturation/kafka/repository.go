package kafka

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

func queueNameExpr() string {
	return fmt.Sprintf(
		"multiIf(JSONExtractString(%[1]s, '%[2]s') != '', JSONExtractString(%[1]s, '%[2]s'), JSONExtractString(%[1]s, '%[3]s') != '', JSONExtractString(%[1]s, '%[3]s'), JSONExtractString(%[1]s, '%[4]s') != '', JSONExtractString(%[1]s, '%[4]s'), JSONExtractString(%[1]s, '%[5]s') != '', JSONExtractString(%[1]s, '%[5]s'), JSONExtractString(%[1]s, '%[6]s') != '', JSONExtractString(%[1]s, '%[6]s'), JSONExtractString(%[1]s, '%[7]s') != '', JSONExtractString(%[1]s, '%[7]s'), '%[8]s')",
		ColAttributes,
		AttrMessagingDestinationName,
		AttrMessagingKafkaDestination,
		AttrMessagingKafkaTopic,
		AttrMessagingDestination,
		AttrKafkaTopic,
		AttrTopic,
		DefaultUnknown,
	)
}

// TimeBucketExpression returns the SQL fragment to bucket times based on the interval.
func TimeBucketExpression(startMs, endMs int64) string {
	durationSecs := (endMs - startMs) / 1000
	if durationSecs <= 3600 {
		return "toStartOfMinute(timestamp)"
	} else if durationSecs <= 86400 {
		return "toStartOfFiveMinutes(timestamp)"
	}
	return "toStartOfHour(timestamp)"
}

// TimeBucketSeconds returns the bucket width in seconds for the given interval.
func TimeBucketSeconds(startMs, endMs int64) float64 {
	durationSecs := (endMs - startMs) / 1000
	if durationSecs <= 3600 {
		return 60.0
	} else if durationSecs <= 86400 {
		return 300.0
	}
	return 3600.0
}

// FormattedTimeBucketExpression returns the bucket string formatted as YYYY-MM-DD HH:mm:00
func FormattedTimeBucketExpression(startMs, endMs int64) string {
	return fmt.Sprintf("formatDateTime(%s, '%%Y-%%m-%%d %%H:%%i:00')", TimeBucketExpression(startMs, endMs))
}

// nullableFloat64FromAny converts an interface{} (usually *float64) to a float64.
func nullableFloat64FromAny(val interface{}) float64 {
	switch v := val.(type) {
	case *float64:
		if v != nil {
			return *v
		}
	case float64:
		return v
	case *float32:
		if v != nil {
			return float64(*v)
		}
	case float32:
		return float64(v)
	}
	return 0
}

func messagingSystemExpr() string {
	return fmt.Sprintf(
		"multiIf(JSONExtractString(%[1]s, '%[2]s') != '', JSONExtractString(%[1]s, '%[2]s'), JSONExtractString(%[1]s, '%[3]s') != '', '%[4]s', '%[5]s')",
		ColAttributes,
		AttrMessagingSystem,
		AttrMessagingKafkaTopic,
		MessagingSystemKafka,
		DefaultUnknown,
	)
}

func queueRateExpr(metricsClause string) string {
	return fmt.Sprintf(
		"maxIf(greatest(toFloat64(%[1]s), toFloat64(%[2]s), 0.0), %[3]s IN (%[4]s) AND (isFinite(toFloat64(%[1]s)) OR isFinite(toFloat64(%[2]s))))",
		ColCount,
		ColValue,
		ColMetricName,
		metricsClause,
	)
}

// ──────────────────────────────────────────────────────────────────────────────
// Kafka repository methods
//
// Design:
//   - Metric coverage is broad (defined in otel_conventions.go metric sets).
//   - Attribute extraction is direct: one JSONExtractString per dimension.
//   - No coalesce chains, no fallback heuristics, no pattern-matching.
//   - If an SDK omits the standard attribute, the row resolves to 'unknown'.
//     That is correct — the emitter owns its contract.
// ──────────────────────────────────────────────────────────────────────────────

// GetKafkaQueueLag returns average and max consumer lag per queue, bucketed over time.
func (r *ClickHouseRepository) GetKafkaQueueLag(teamUUID string, startMs, endMs int64) ([]KafkaQueueLag, error) {
	bucket := TimeBucketExpression(startMs, endMs)
	lagMetrics := MetricSetToInClause(KafkaConsumerLagMetrics)
	queueExpr := queueNameExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s                                              AS queue,
		    %s                                              AS minute_bucket,
		    avgIf(%s, %s IN (%s) AND isFinite(%s))          AS avg_consumer_lag,
		    maxIf(%s, %s IN (%s) AND isFinite(%s))          AS max_consumer_lag
		FROM metrics
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY queue, minute_bucket
		ORDER BY minute_bucket ASC, queue ASC`,
		queueExpr,
		bucket,
		ColValue, ColMetricName, lagMetrics, ColValue,
		ColValue, ColMetricName, lagMetrics, ColValue,
		ColTeamID,
		ColTimestamp,
		ColMetricName, lagMetrics,
	)

	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetKafkaQueueLag: %w", err)
	}

	out := make([]KafkaQueueLag, len(rows))
	for i, row := range rows {
		out[i] = KafkaQueueLag{
			Queue:          dbutil.StringFromAny(row["queue"]),
			Timestamp:      dbutil.StringFromAny(row["minute_bucket"]),
			AvgConsumerLag: dbutil.Float64FromAny(row["avg_consumer_lag"]),
			MaxConsumerLag: dbutil.Float64FromAny(row["max_consumer_lag"]),
		}
	}
	return out, nil
}

// GetKafkaProductionRate returns the per-bucket average publish rate (msg/s) per topic.
func (r *ClickHouseRepository) GetKafkaProductionRate(teamUUID string, startMs, endMs int64) ([]KafkaProductionRate, error) {
	bucket := TimeBucketExpression(startMs, endMs)
	bucketSecs := TimeBucketSeconds(startMs, endMs)
	producerMetrics := MetricSetToInClause(KafkaProducerMetrics)
	queueExpr := queueNameExpr()
	rateExpr := queueRateExpr(producerMetrics)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                             AS queue,
		    %s                                                             AS minute_bucket,
		    %s / ?                                                         AS avg_publish_rate
		FROM metrics
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY queue, minute_bucket
		ORDER BY minute_bucket ASC, queue ASC`,
		queueExpr,
		bucket,
		rateExpr,
		ColTeamID,
		ColTimestamp,
		ColMetricName, producerMetrics,
	)

	rows, err := dbutil.QueryMaps(r.db, query, bucketSecs, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetKafkaProductionRate: %w", err)
	}

	out := make([]KafkaProductionRate, len(rows))
	for i, row := range rows {
		out[i] = KafkaProductionRate{
			Topic:          dbutil.StringFromAny(row["topic"]),
			Timestamp:      dbutil.StringFromAny(row["minute_bucket"]),
			AvgPublishRate: nullableFloat64FromAny(row["avg_publish_rate"]),
		}
	}
	return out, nil
}

// GetKafkaConsumptionRate returns the per-bucket average receive rate (msg/s) per topic.
func (r *ClickHouseRepository) GetKafkaConsumptionRate(teamUUID string, startMs, endMs int64) ([]KafkaConsumptionRate, error) {
	bucket := TimeBucketExpression(startMs, endMs)
	bucketSecs := TimeBucketSeconds(startMs, endMs)
	consumerMetrics := MetricSetToInClause(KafkaConsumerMetrics)
	queueExpr := queueNameExpr()
	rateExpr := queueRateExpr(consumerMetrics)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                             AS queue,
		    %s                                                             AS minute_bucket,
		    %s / ?                                                         AS avg_receive_rate
		FROM metrics
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY queue, minute_bucket
		ORDER BY minute_bucket ASC, queue ASC`,
		queueExpr,
		bucket,
		rateExpr,
		ColTeamID,
		ColTimestamp,
		ColMetricName, consumerMetrics,
	)

	rows, err := dbutil.QueryMaps(r.db, query, bucketSecs, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetKafkaConsumptionRate: %w", err)
	}

	out := make([]KafkaConsumptionRate, len(rows))
	for i, row := range rows {
		out[i] = KafkaConsumptionRate{
			Topic:          dbutil.StringFromAny(row["topic"]),
			Timestamp:      dbutil.StringFromAny(row["minute_bucket"]),
			AvgReceiveRate: nullableFloat64FromAny(row["avg_receive_rate"]),
		}
	}
	return out, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Messaging-queue repository methods
// ──────────────────────────────────────────────────────────────────────────────

// GetQueueConsumerLag returns a consumer-lag timeseries keyed by
// (time_bucket, service, queue, messaging_system).
func (r *ClickHouseRepository) GetQueueConsumerLag(teamUUID string, startMs, endMs int64) ([]MqBucket, error) {
	bucket := FormattedTimeBucketExpression(startMs, endMs)
	lagMetrics := MetricSetToInClause(KafkaConsumerLagMetricsExtended)
	queueExpr := queueNameExpr()
	systemExpr := messagingSystemExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s                                                      AS time_bucket,
		    %s                                                      AS service_name,
		    %s                                                      AS queue_name,
		    %s                                                      AS messaging_system,
		    avgIf(%s, %s IN (%s) AND isFinite(%s))                  AS avg_consumer_lag
		FROM metrics
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY time_bucket, service_name, queue_name, messaging_system
		ORDER BY time_bucket ASC, service_name ASC, queue_name ASC`,
		bucket,
		ColServiceName,
		queueExpr,
		systemExpr,
		ColValue, ColMetricName, lagMetrics, ColValue,
		ColTeamID,
		ColTimestamp,
		ColMetricName, lagMetrics,
	)

	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetQueueConsumerLag: %w", err)
	}

	out := make([]MqBucket, len(rows))
	for i, row := range rows {
		out[i] = MqBucket{
			Timestamp:       dbutil.StringFromAny(row["time_bucket"]),
			ServiceName:     dbutil.StringFromAny(row["service_name"]),
			QueueName:       dbutil.StringFromAny(row["queue_name"]),
			MessagingSystem: dbutil.StringFromAny(row["messaging_system"]),
			AvgConsumerLag:  nullableFloat64FromAny(row["max_consumer_lag"]),
		}
	}
	return out, nil
}

// GetQueueTopicLag returns a queue-depth timeseries keyed by
// (time_bucket, service, queue, messaging_system).
func (r *ClickHouseRepository) GetQueueTopicLag(teamUUID string, startMs, endMs int64) ([]MqBucket, error) {
	bucket := FormattedTimeBucketExpression(startMs, endMs)
	depthMetrics := MetricSetToInClause(QueueDepthMetrics)
	queueExpr := queueNameExpr()
	systemExpr := messagingSystemExpr()

	query := fmt.Sprintf(`
		SELECT
		    %s                                                      AS time_bucket,
		    %s                                                      AS service_name,
		    %s                                                      AS queue_name,
		    %s                                                      AS messaging_system,
		    avgIf(%s, %s IN (%s) AND isFinite(%s))                  AS avg_queue_depth
		FROM metrics
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY time_bucket, service_name, queue_name, messaging_system
		ORDER BY time_bucket ASC, service_name ASC, queue_name ASC`,
		bucket,
		ColServiceName,
		queueExpr,
		systemExpr,
		ColValue, ColMetricName, depthMetrics, ColValue,
		ColTeamID,
		ColTimestamp,
		ColMetricName, depthMetrics,
	)

	rows, err := dbutil.QueryMaps(r.db, query, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, fmt.Errorf("GetQueueTopicLag: %w", err)
	}

	out := make([]MqBucket, len(rows))
	for i, row := range rows {
		out[i] = MqBucket{
			Timestamp:       dbutil.StringFromAny(row["time_bucket"]),
			ServiceName:     dbutil.StringFromAny(row["service_name"]),
			QueueName:       dbutil.StringFromAny(row["queue_name"]),
			MessagingSystem: dbutil.StringFromAny(row["messaging_system"]),
			AvgQueueDepth:   nullableFloat64FromAny(row["avg_queue_depth"]),
		}
	}
	return out, nil
}

// GetQueueTopQueues returns the top queues ranked by depth and lag, with
// publish/receive rates normalised to msg/s over the requested window.
func (r *ClickHouseRepository) GetQueueTopQueues(teamUUID string, startMs, endMs int64) ([]MqTopQueue, error) {
	durationSecs := float64(endMs-startMs) / 1000.0
	if durationSecs <= 0 {
		durationSecs = 1.0
	}

	allMetrics := MetricSetToInClause(AllQueueMetrics)
	depthMetrics := MetricSetToInClause(QueueDepthMetrics)
	lagMetrics := MetricSetToInClause(KafkaConsumerLagMetricsExtended)
	producerMetrics := MetricSetToInClause(KafkaProducerMetrics)
	consumerMetrics := MetricSetToInClause(KafkaConsumerMetrics)
	queueExpr := queueNameExpr()
	systemExpr := messagingSystemExpr()
	publishRateExpr := queueRateExpr(producerMetrics)
	receiveRateExpr := queueRateExpr(consumerMetrics)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                           AS queue_name,
		    %s                                                           AS service_name,
		    %s                                                           AS messaging_system,
		    avgIf(%s, %s IN (%s) AND isFinite(%s))                       AS avg_queue_depth,
		    maxIf(%s, %s IN (%s) AND isFinite(%s))                       AS max_consumer_lag,
		    %s / ?                                                       AS avg_publish_rate,
		    %s / ?                                                       AS avg_receive_rate,
		    toInt64(count())                                             AS sample_count
		FROM metrics
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN (%s)
		GROUP BY queue_name, service_name, messaging_system
		ORDER BY avg_queue_depth DESC, max_consumer_lag DESC, sample_count DESC
		LIMIT %d`,
		queueExpr,
		ColServiceName,
		systemExpr,
		ColValue, ColMetricName, depthMetrics, ColValue,
		ColValue, ColMetricName, lagMetrics, ColValue,
		publishRateExpr,
		receiveRateExpr,
		ColTeamID,
		ColTimestamp,
		ColMetricName, allMetrics,
		MaxTopQueues,
	)

	rows, err := dbutil.QueryMaps(r.db, query,
		durationSecs, durationSecs,
		teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs),
	)
	if err != nil {
		return nil, fmt.Errorf("GetQueueTopQueues: %w", err)
	}

	out := make([]MqTopQueue, len(rows))
	for i, row := range rows {
		out[i] = MqTopQueue{
			ServiceName:       dbutil.StringFromAny(row["service_name"]),
			QueueName:         dbutil.StringFromAny(row["queue_name"]),
			MessagingSystem:   dbutil.StringFromAny(row["messaging_system"]),
			AvgPublishRate:    nullableFloat64FromAny(row["avg_publish_rate"]),
			AvgReceiveRate:    nullableFloat64FromAny(row["avg_receive_rate"]),
			AvgConsumerLag:    nullableFloat64FromAny(row["avg_consumer_lag"]),
			ActiveConnections: dbutil.Int64FromAny(row["active_connections"]),
			SampleCount:       dbutil.Int64FromAny(row["sample_count"]),
		}
	}
	return out, nil
}
