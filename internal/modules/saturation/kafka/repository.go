package kafka

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

// Repository exposes raw, CH-native kafka data. All aggregation complexity
// (rates per second, averages, percentiles, pivots) lives in service.go —
// SQL here stays free of sumIf / countIf / maxIf / avgIf / quantileTDigest /
// if / multiIf / CASE WHEN / coalesce / nullIf / arrayJoin / toInt64 / toUInt*.
type Repository interface {
	GetSummaryRates(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]summaryRateRow, error)
	GetSummaryMaxLag(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) (summaryMaxLagRow, error)
	GetProduceRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]topicValueSumRow, error)
	GetPublishLatencyKeys(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]topicKeyRow, error)
	GetConsumeRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]topicValueSumRow, error)
	GetReceiveLatencyKeys(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]topicKeyRow, error)
	GetConsumeRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]groupValueSumRow, error)
	GetProcessRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]groupValueSumRow, error)
	GetProcessLatencyKeys(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]groupKeyRow, error)
	GetConsumerLagByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]groupTopicLagRow, error)
	GetConsumerLagPerPartition(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]partitionLagRawRow, error)
	GetRebalanceMetrics(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]rebalanceMetricRow, error)
	GetE2ELatencyKeys(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]topicKeyRow, error)
	GetPublishErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]errorRateRawRow, error)
	GetConsumeErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]errorRateRawRow, error)
	GetProcessErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]errorRateRawRow, error)
	GetClientOpErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]errorRateRawRow, error)
	GetBrokerConnections(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]brokerConnectionRawRow, error)
	GetClientOperationDurationKeys(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]operationKeyRow, error)
	GetConsumerMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricNames []string) ([]consumerMetricSampleRow, error)
	GetTopicMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricNames []string) ([]topicMetricSampleRow, error)
}

// bucketSecs returns the bucket width in seconds matching the adaptive
// timebucket strategy. Service uses it to convert sums to per-second rates.
func bucketSecs(startMs, endMs int64) float64 {
	durationSecs := (endMs - startMs) / 1000
	if durationSecs <= 3600 {
		return 60.0
	} else if durationSecs <= 86400 {
		return 300.0
	}
	return 3600.0
}

// timeBucketExpr exposes the adaptive bucket expression (kept as a helper so
// every query in this file references the same SQL fragment).
func timeBucketExpr(startMs, endMs int64) string {
	return timebucket.Expression(startMs, endMs)
}

// ---------------------------------------------------------------------------
// Summary card — two narrow queries replace the old mega-aggregation that
// leaned on sumIf / maxIf / quantileTDigestWeightedIf in a single SELECT.
// Percentiles come from the sketch in service.go.
// ---------------------------------------------------------------------------

// GetSummaryRates returns sum(value) grouped by metric family (producer,
// consumer). Service divides each family sum by windowSecs for rates.
func (r *ClickHouseRepository) GetSummaryRates(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]summaryRateRow, error) {
	fc, fargs := kafkaFilterClauses(f)

	// Two union branches, each tagged with a literal family string — no
	// sumIf / countIf. Service maps 'producer' -> publish rate, 'consumer' ->
	// receive rate.
	query := fmt.Sprintf(`
		SELECT metric_family, sum(value_sum) AS value_sum FROM (
		    SELECT 'producer' AS metric_family, %[1]s AS value_sum
		    FROM %[3]s
		    WHERE team_id = @teamID
		      AND timestamp BETWEEN @start AND @end
		      %[4]s
		      AND %[2]s IN @producerMetrics
		    UNION ALL
		    SELECT 'consumer' AS metric_family, %[1]s AS value_sum
		    FROM %[3]s
		    WHERE team_id = @teamID
		      AND timestamp BETWEEN @start AND @end
		      %[4]s
		      AND %[2]s IN @consumerMetrics
		)
		GROUP BY metric_family
	`, ColValue, ColMetricName, TableMetrics, fc)

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var out []summaryRateRow
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// GetSummaryMaxLag returns the scalar max across finite lag values in the
// window. isFinite stays a plain scalar WHERE predicate.
func (r *ClickHouseRepository) GetSummaryMaxLag(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) (summaryMaxLagRow, error) {
	fc, fargs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT max(%[1]s) AS max_lag
		FROM %[3]s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %[4]s
		  AND %[2]s IN @lagMetrics
		  AND isFinite(%[1]s)
	`, ColValue, ColMetricName, TableMetrics, fc)

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var row summaryMaxLagRow
	return row, r.db.QueryRow(dbutil.OverviewCtx(ctx), query, args...).ScanStruct(&row)
}

// ---------------------------------------------------------------------------
// Topic-level rates + latency key discovery.
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetProduceRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]topicValueSumRow, error) {
	return r.topicValueSum(ctx, teamID, startMs, endMs, f, "@producerMetrics")
}

func (r *ClickHouseRepository) GetConsumeRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]topicValueSumRow, error) {
	return r.topicValueSum(ctx, teamID, startMs, endMs, f, "@consumerMetrics")
}

// topicValueSum is the shared by-topic rate query. metricsParam is the @-named
// parameter holding the in-list of metric names (producer / consumer).
func (r *ClickHouseRepository) topicValueSum(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricsParam string) ([]topicValueSumRow, error) {
	bucket := timeBucketExpr(startMs, endMs)
	topic := topicExpr()
	fc, fargs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic,
		    sum(%s) AS value_sum
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN %s
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, topic, ColValue, TableMetrics, fc, ColMetricName, metricsParam)

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var out []topicValueSumRow
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// GetPublishLatencyKeys returns the (time_bucket, topic) pairs that have any
// publish-duration histogram data in the window. Service attaches percentile
// values from the KafkaTopicLatency sketch.
func (r *ClickHouseRepository) GetPublishLatencyKeys(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]topicKeyRow, error) {
	return r.topicLatencyKeys(ctx, teamID, startMs, endMs, f, publishDurationPredicate())
}

func (r *ClickHouseRepository) GetReceiveLatencyKeys(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]topicKeyRow, error) {
	return r.topicLatencyKeys(ctx, teamID, startMs, endMs, f, receiveDurationPredicate())
}

func (r *ClickHouseRepository) topicLatencyKeys(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, predicate string) ([]topicKeyRow, error) {
	bucket := timeBucketExpr(startMs, endMs)
	topic := topicExpr()
	fc, fargs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, topic, TableMetrics, fc, predicate)

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var out []topicKeyRow
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// ---------------------------------------------------------------------------
// Consumer-group rates + latency keys.
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetConsumeRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]groupValueSumRow, error) {
	return r.groupValueSum(ctx, teamID, startMs, endMs, f, "@consumerMetrics")
}

func (r *ClickHouseRepository) GetProcessRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]groupValueSumRow, error) {
	return r.groupValueSum(ctx, teamID, startMs, endMs, f, "@processMetrics")
}

func (r *ClickHouseRepository) groupValueSum(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricsParam string) ([]groupValueSumRow, error) {
	bucket := timeBucketExpr(startMs, endMs)
	group := consumerGroupExpr()
	fc, fargs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    sum(%s) AS value_sum
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN %s
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, ColValue, TableMetrics, fc, ColMetricName, metricsParam)

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var out []groupValueSumRow
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

func (r *ClickHouseRepository) GetProcessLatencyKeys(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]groupKeyRow, error) {
	bucket := timeBucketExpr(startMs, endMs)
	group := consumerGroupExpr()
	fc, fargs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, consumer_group
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, TableMetrics, fc, processDurationPredicate())

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var out []groupKeyRow
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// ---------------------------------------------------------------------------
// Consumer lag.
// ---------------------------------------------------------------------------

// GetConsumerLagByGroup returns raw sum+count per (time_bucket, group, topic).
// Service computes sum/count for the average. isFinite stays a WHERE filter.
func (r *ClickHouseRepository) GetConsumerLagByGroup(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]groupTopicLagRow, error) {
	bucket := timeBucketExpr(startMs, endMs)
	group := consumerGroupExpr()
	topic := topicExpr()
	fc, fargs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    %s AS topic,
		    sum(%s) AS value_sum,
		    count()  AS value_count
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN @lagMetrics
		  AND isFinite(%s)
		GROUP BY time_bucket, consumer_group, topic
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, topic, ColValue, TableMetrics, fc, ColMetricName, ColValue)

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var out []groupTopicLagRow
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// GetConsumerLagPerPartition returns raw sum+count per (topic, partition,
// consumer_group). Partition stays a string here (the underlying attribute
// is a raw String); service parses to int64. Lag is rounded in Go.
func (r *ClickHouseRepository) GetConsumerLagPerPartition(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]partitionLagRawRow, error) {
	topic := topicExpr()
	partition := topicPartitionExpr()
	group := consumerGroupExpr()
	fc, fargs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS topic,
		    %s AS partition,
		    %s AS consumer_group,
		    sum(%s) AS value_sum,
		    count()  AS value_count
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN @lagMetrics
		  AND isFinite(%s)
		GROUP BY topic, partition, consumer_group
		ORDER BY value_sum DESC
		LIMIT 200
	`, topic, partition, group, ColValue, TableMetrics, fc, ColMetricName, ColValue)

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var out []partitionLagRawRow
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// ---------------------------------------------------------------------------
// Rebalance signals — a single pivot-ready query. Service groups the six
// metric_name variants into the public RebalancePoint fields.
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetRebalanceMetrics(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]rebalanceMetricRow, error) {
	bucket := timeBucketExpr(startMs, endMs)
	group := consumerGroupExpr()
	fc, fargs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    %s AS metric_name,
		    sum(%s) AS value_sum,
		    count()  AS value_count
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN @rebalanceMetrics
		  AND isFinite(%s)
		GROUP BY time_bucket, consumer_group, metric_name
		ORDER BY time_bucket ASC, consumer_group ASC
	`, bucket, group, ColMetricName, ColValue, TableMetrics, fc, ColMetricName, ColValue)

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var out []rebalanceMetricRow
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// ---------------------------------------------------------------------------
// End-to-end latency — topic keys only; the sketch fills percentiles.
// ---------------------------------------------------------------------------

// GetE2ELatencyKeys returns every (time_bucket, topic) tuple with any
// publish/receive/process histogram activity. Service attaches the merged
// KafkaTopicLatency p95 for each topic to all three fields (publish/receive/
// process share the same sketch today — known limitation).
func (r *ClickHouseRepository) GetE2ELatencyKeys(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]topicKeyRow, error) {
	bucket := timeBucketExpr(startMs, endMs)
	topic := topicExpr()
	fc, fargs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND (%s OR %s OR %s)
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, topic
		ORDER BY time_bucket ASC, topic ASC
	`, bucket, topic, TableMetrics, fc,
		publishDurationPredicate(),
		receiveDurationPredicate(),
		processDurationPredicate(),
	)

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var out []topicKeyRow
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// ---------------------------------------------------------------------------
// Error rates — repository returns raw value_sum by error.type; service
// divides by bucketSecs.
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetPublishErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]errorRateRawRow, error) {
	return r.topicErrorRates(ctx, teamID, startMs, endMs, f, MetricPublishMessages, "GetPublishErrors")
}

func (r *ClickHouseRepository) GetConsumeErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]errorRateRawRow, error) {
	return r.groupErrorRates(ctx, teamID, startMs, endMs, f, MetricReceiveMessages, "GetConsumeErrors")
}

func (r *ClickHouseRepository) GetProcessErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]errorRateRawRow, error) {
	return r.groupErrorRates(ctx, teamID, startMs, endMs, f, MetricProcessMessages, "GetProcessErrors")
}

func (r *ClickHouseRepository) topicErrorRates(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricName, caller string) ([]errorRateRawRow, error) {
	bucket := timeBucketExpr(startMs, endMs)
	errType := attrString(AttrErrorType)
	topic := topicExpr()
	fc, fargs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic,
		    ''  AS consumer_group,
		    ''  AS operation_name,
		    %s AS error_type,
		    sum(%s) AS value_sum
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s = '%s'
		  AND %s != ''
		GROUP BY time_bucket, topic, error_type
		ORDER BY time_bucket ASC, value_sum DESC
	`, bucket, topic, errType, ColValue, TableMetrics,
		fc, ColMetricName, metricName, errType)

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var out []errorRateRawRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...); err != nil {
		return nil, fmt.Errorf("%s: %w", caller, err)
	}
	return out, nil
}

func (r *ClickHouseRepository) groupErrorRates(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricName, caller string) ([]errorRateRawRow, error) {
	bucket := timeBucketExpr(startMs, endMs)
	errType := attrString(AttrErrorType)
	group := consumerGroupExpr()
	fc, fargs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    ''  AS topic,
		    %s AS consumer_group,
		    ''  AS operation_name,
		    %s AS error_type,
		    sum(%s) AS value_sum
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s = '%s'
		  AND %s != ''
		GROUP BY time_bucket, consumer_group, error_type
		ORDER BY time_bucket ASC, value_sum DESC
	`, bucket, group, errType, ColValue, TableMetrics,
		fc, ColMetricName, metricName, errType)

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var out []errorRateRawRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...); err != nil {
		return nil, fmt.Errorf("%s: %w", caller, err)
	}
	return out, nil
}

func (r *ClickHouseRepository) GetClientOpErrors(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]errorRateRawRow, error) {
	bucket := timeBucketExpr(startMs, endMs)
	errType := attrString(AttrErrorType)
	opName := operationExpr()
	fc, fargs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    ''  AS topic,
		    ''  AS consumer_group,
		    %s AS operation_name,
		    %s AS error_type,
		    sum(%s) AS value_sum
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s = '%s'
		  AND %s != ''
		GROUP BY time_bucket, operation_name, error_type
		ORDER BY time_bucket ASC, value_sum DESC
	`, bucket, opName, errType, ColValue, TableMetrics,
		fc, ColMetricName, MetricClientOperationDuration, errType)

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var out []errorRateRawRow
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// ---------------------------------------------------------------------------
// Broker connections — raw sum+count; service computes sum/count for avg.
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetBrokerConnections(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]brokerConnectionRawRow, error) {
	bucket := timeBucketExpr(startMs, endMs)
	broker := attrString(AttrServerAddress)
	fc, fargs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS broker,
		    sum(%s) AS value_sum,
		    count()  AS value_count
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s = '%s'
		  AND isFinite(%s)
		GROUP BY time_bucket, broker
		ORDER BY time_bucket ASC, broker ASC
	`, bucket, broker, ColValue, TableMetrics,
		fc, ColMetricName, MetricClientConnections, ColValue)

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var out []brokerConnectionRawRow
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// GetClientOperationDurationKeys returns the (time_bucket, operation_name)
// tuples that carry client operation duration histograms. The current
// sketch topology keys by topic|client_id — there is no per-operation sketch —
// so service returns zeros for the percentile fields. Known limitation;
// tracked for a follow-up dedicated sketch kind.
func (r *ClickHouseRepository) GetClientOperationDurationKeys(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters) ([]operationKeyRow, error) {
	bucket := timeBucketExpr(startMs, endMs)
	opName := operationExpr()
	fc, fargs := kafkaFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS operation_name
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, operation_name
		ORDER BY time_bucket ASC, operation_name ASC
	`, bucket, opName, TableMetrics, fc, ColMetricName, MetricClientOperationDuration)

	args := append(r.baseParams(teamID, startMs, endMs), fargs...)
	var out []operationKeyRow
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

// ---------------------------------------------------------------------------
// Inventory samples — raw sum+count; service computes sum/count for avg.
// ---------------------------------------------------------------------------

func (r *ClickHouseRepository) GetConsumerMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricNames []string) ([]consumerMetricSampleRow, error) {
	if len(metricNames) == 0 {
		return nil, nil
	}

	bucket := timeBucketExpr(startMs, endMs)
	group := consumerGroupExpr()
	nodeID := nodeIDExpr()
	fc, fargs := kafkaInventoryFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS consumer_group,
		    %s AS node_id,
		    %s AS metric_name,
		    sum(%s) AS value_sum,
		    count()  AS value_count
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN @metricNames
		  AND %s != ''
		  AND isFinite(%s)
		GROUP BY time_bucket, consumer_group, node_id, metric_name
		ORDER BY time_bucket ASC, consumer_group ASC, metric_name ASC
	`, bucket, group, nodeID, ColMetricName, ColValue, TableMetrics, fc, ColMetricName, group, ColValue)

	args := append(r.baseParams(teamID, startMs, endMs), clickhouse.Named("metricNames", metricNames))
	args = append(args, fargs...)
	var out []consumerMetricSampleRow
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}

func (r *ClickHouseRepository) GetTopicMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, f KafkaFilters, metricNames []string) ([]topicMetricSampleRow, error) {
	if len(metricNames) == 0 {
		return nil, nil
	}

	bucket := timeBucketExpr(startMs, endMs)
	topic := topicExpr()
	group := consumerGroupExpr()
	fc, fargs := kafkaInventoryFilterClauses(f)

	query := fmt.Sprintf(`
		SELECT
		    %s AS time_bucket,
		    %s AS topic,
		    %s AS consumer_group,
		    %s AS metric_name,
		    sum(%s) AS value_sum,
		    count()  AS value_count
		FROM %s
		WHERE team_id = @teamID
		  AND timestamp BETWEEN @start AND @end
		  %s
		  AND %s IN @metricNames
		  AND %s != ''
		  AND isFinite(%s)
		GROUP BY time_bucket, topic, consumer_group, metric_name
		ORDER BY time_bucket ASC, topic ASC, consumer_group ASC, metric_name ASC
	`, bucket, topic, group, ColMetricName, ColValue, TableMetrics, fc, ColMetricName, topic, ColValue)

	args := append(r.baseParams(teamID, startMs, endMs), clickhouse.Named("metricNames", metricNames))
	args = append(args, fargs...)
	var out []topicMetricSampleRow
	return out, r.db.Select(dbutil.OverviewCtx(ctx), &out, query, args...)
}
