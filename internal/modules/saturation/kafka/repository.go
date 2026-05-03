package kafka

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

// ============================================================================
// Summary stats — counter / gauge / histogram aggregations
// ============================================================================

const counterAggQuery = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    toFloat64(sum(value)) AS sum_value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND lower(attributes.'messaging.system'::String) = 'kafka'`

func (r *ClickHouseRepository) queryCounterAgg(ctx context.Context, op string, teamID int64, startMs, endMs int64, metricNames []string) (CounterAggRow, error) {
	args := withMetricNames(metricArgs(teamID, startMs, endMs), metricNames)
	var row CounterAggRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, op, &row, counterAggQuery, args...)
}

func (r *ClickHouseRepository) QueryPublishMessageCount(ctx context.Context, teamID int64, startMs, endMs int64) (CounterAggRow, error) {
	return r.queryCounterAgg(ctx, "kafka.QueryPublishMessageCount", teamID, startMs, endMs, ProducerMetrics)
}

func (r *ClickHouseRepository) QueryReceiveMessageCount(ctx context.Context, teamID int64, startMs, endMs int64) (CounterAggRow, error) {
	return r.queryCounterAgg(ctx, "kafka.QueryReceiveMessageCount", teamID, startMs, endMs, ConsumerMetrics)
}

func (r *ClickHouseRepository) QueryMaxConsumerLag(ctx context.Context, teamID int64, startMs, endMs int64) (GaugeMaxRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    toFloat64(max(value)) AS max_value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), ConsumerLagMetrics)
	var row GaugeMaxRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryMaxConsumerLag", &row, query, args...)
}

const histogramAggQuery = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    sum(hist_sum)            AS sum_hist_sum,
		    sum(hist_count)          AS sum_hist_count,
		    max(hist_buckets)        AS hist_buckets,
		    sumForEach(hist_counts)  AS hist_counts
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND lower(attributes.'messaging.system'::String) = 'kafka'`

func (r *ClickHouseRepository) queryHistogramAgg(ctx context.Context, op, metricName string, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	var row HistogramAggRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, op, &row, histogramAggQuery, args...)
}

func (r *ClickHouseRepository) QueryPublishDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	return r.queryHistogramAgg(ctx, "kafka.QueryPublishDurationHistogram", MetricPublishDuration, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryReceiveDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	return r.queryHistogramAgg(ctx, "kafka.QueryReceiveDurationHistogram", MetricReceiveDuration, teamID, startMs, endMs)
}

// ============================================================================
// Counter rate panels — sum value per (display_bucket, dim)
// ============================================================================

const counterSeriesByTopicQuery = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    timestamp                                                AS timestamp,
		    attributes.'messaging.destination.name'::String          AS topic,
		    val_sum / val_count AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.destination.name'::String != ''
		  AND lower(attributes.'messaging.system'::String) = 'kafka'
		ORDER BY timestamp`

func (r *ClickHouseRepository) queryCounterSeriesByTopic(ctx context.Context, op string, teamID int64, startMs, endMs int64, metricNames []string) ([]TopicCounterRow, error) {
	args := withMetricNames(metricArgs(teamID, startMs, endMs), metricNames)
	var rows []TopicCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, op, &rows, counterSeriesByTopicQuery, args...)
}

func (r *ClickHouseRepository) QueryPublishRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicCounterRow, error) {
	return r.queryCounterSeriesByTopic(ctx, "kafka.QueryPublishRateByTopic", teamID, startMs, endMs, ProducerMetrics)
}

func (r *ClickHouseRepository) QueryConsumeRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicCounterRow, error) {
	return r.queryCounterSeriesByTopic(ctx, "kafka.QueryConsumeRateByTopic", teamID, startMs, endMs, ConsumerMetrics)
}

const counterSeriesByGroupQuery = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    timestamp                                                AS timestamp,
		    attributes.'messaging.consumer.group.name'::String       AS consumer_group,
		    val_sum / val_count AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.consumer.group.name'::String != ''
		  AND lower(attributes.'messaging.system'::String) = 'kafka'
		ORDER BY timestamp`

func (r *ClickHouseRepository) queryCounterSeriesByGroup(ctx context.Context, op string, teamID int64, startMs, endMs int64, metricNames []string) ([]GroupCounterRow, error) {
	args := withMetricNames(metricArgs(teamID, startMs, endMs), metricNames)
	var rows []GroupCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, op, &rows, counterSeriesByGroupQuery, args...)
}

func (r *ClickHouseRepository) QueryConsumeRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupCounterRow, error) {
	return r.queryCounterSeriesByGroup(ctx, "kafka.QueryConsumeRateByGroup", teamID, startMs, endMs, ConsumerMetrics)
}

func (r *ClickHouseRepository) QueryProcessRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupCounterRow, error) {
	return r.queryCounterSeriesByGroup(ctx, "kafka.QueryProcessRateByGroup", teamID, startMs, endMs, ProcessMetrics)
}

// ============================================================================
// Latency panels — histogram series merged SQL-side per (display_bucket, dim)
// ============================================================================

const histogramSeriesByTopicQuery = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN (@metricName, @opDuration)
		)
		SELECT
		    toDateTime(intDiv(toUnixTimestamp(timestamp), @stepSec) * @stepSec) AS timestamp,
		    attributes.'messaging.destination.name'::String                     AS topic,
		    max(hist_buckets)                                                   AS hist_buckets,
		    sumForEach(hist_counts)                                             AS hist_counts
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE (metric_name = @metricName
		       OR (metric_name = @opDuration
		           AND lower(attributes.'messaging.operation.name'::String) IN @opAliases))
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.destination.name'::String != ''
		  AND lower(attributes.'messaging.system'::String) = 'kafka'
		GROUP BY timestamp, topic
		ORDER BY timestamp, topic`

func (r *ClickHouseRepository) queryHistogramSeriesByTopic(ctx context.Context, op, metricName string, opAliases []string, teamID int64, startMs, endMs int64) ([]TopicHistogramRow, error) {
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	args = append(args, clickhouse.Named("opDuration", MetricClientOperationDuration))
	args = withOpAliases(args, opAliases)
	args = withStepSec(args, startMs, endMs)
	var rows []TopicHistogramRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, op, &rows, histogramSeriesByTopicQuery, args...)
}

func (r *ClickHouseRepository) QueryPublishLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicHistogramRow, error) {
	return r.queryHistogramSeriesByTopic(ctx, "kafka.QueryPublishLatencyByTopic", MetricPublishDuration, publishOperationAliases, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryReceiveLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicHistogramRow, error) {
	return r.queryHistogramSeriesByTopic(ctx, "kafka.QueryReceiveLatencyByTopic", MetricReceiveDuration, receiveOperationAliases, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryProcessLatencyByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupHistogramRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN (@metricName, @opDuration)
		)
		SELECT
		    toDateTime(intDiv(toUnixTimestamp(timestamp), @stepSec) * @stepSec) AS timestamp,
		    attributes.'messaging.consumer.group.name'::String                  AS consumer_group,
		    max(hist_buckets)                                                   AS hist_buckets,
		    sumForEach(hist_counts)                                             AS hist_counts
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE (metric_name = @metricName
		       OR (metric_name = @opDuration
		           AND lower(attributes.'messaging.operation.name'::String) IN @opAliases))
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.consumer.group.name'::String != ''
		  AND lower(attributes.'messaging.system'::String) = 'kafka'
		GROUP BY timestamp, consumer_group
		ORDER BY timestamp, consumer_group`
	args := withMetricName(metricArgs(teamID, startMs, endMs), MetricProcessDuration)
	args = append(args, clickhouse.Named("opDuration", MetricClientOperationDuration))
	args = withOpAliases(args, processOperationAliases)
	args = withStepSec(args, startMs, endMs)
	var rows []GroupHistogramRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryProcessLatencyByGroup", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryClientOperationDurationByOp(ctx context.Context, teamID int64, startMs, endMs int64) ([]OperationHistogramRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    toDateTime(intDiv(toUnixTimestamp(timestamp), @stepSec) * @stepSec) AS timestamp,
		    attributes.'messaging.operation.name'::String                       AS operation_name,
		    max(hist_buckets)                                                   AS hist_buckets,
		    sumForEach(hist_counts)                                             AS hist_counts
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.operation.name'::String != ''
		  AND lower(attributes.'messaging.system'::String) = 'kafka'
		GROUP BY timestamp, operation_name
		ORDER BY timestamp, operation_name`
	args := withMetricName(metricArgs(teamID, startMs, endMs), MetricClientOperationDuration)
	args = withStepSec(args, startMs, endMs)
	var rows []OperationHistogramRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryClientOperationDurationByOp", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryE2ELatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicMetricHistogramRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    toDateTime(intDiv(toUnixTimestamp(timestamp), @stepSec) * @stepSec) AS timestamp,
		    attributes.'messaging.destination.name'::String                     AS topic,
		    metric_name                                                         AS metric_name,
		    max(hist_buckets)                                                   AS hist_buckets,
		    sumForEach(hist_counts)                                             AS hist_counts
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.destination.name'::String != ''
		  AND lower(attributes.'messaging.system'::String) = 'kafka'
		GROUP BY timestamp, topic, metric_name
		ORDER BY timestamp, topic, metric_name`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), []string{MetricPublishDuration, MetricReceiveDuration, MetricProcessDuration})
	args = withStepSec(args, startMs, endMs)
	var rows []TopicMetricHistogramRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryE2ELatencyByTopic", &rows, query, args...)
}

// ============================================================================
// Lag panels
// ============================================================================

func (r *ClickHouseRepository) QueryConsumerLagByGroupTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupTopicGaugeRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    timestamp                                                AS timestamp,
		    attributes.'messaging.consumer.group.name'::String       AS consumer_group,
		    attributes.'messaging.destination.name'::String          AS topic,
		    val_sum / val_count AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.consumer.group.name'::String != ''
		ORDER BY timestamp`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), ConsumerLagMetrics)
	var rows []GroupTopicGaugeRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryConsumerLagByGroupTopic", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryPartitionLagSnapshot(ctx context.Context, teamID int64, startMs, endMs int64) ([]PartitionLagSnapshotRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'messaging.destination.name'::String          AS topic,
		    attributes.'messaging.kafka.destination.partition'::String AS partition,
		    attributes.'messaging.consumer.group.name'::String       AS consumer_group,
		    toFloat64(argMax(value, timestamp))                      AS lag,
		    max(timestamp)                                           AS timestamp
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.destination.name'::String != ''
		  AND attributes.'messaging.kafka.destination.partition'::String != ''
		GROUP BY topic, partition, consumer_group
		ORDER BY lag DESC
		LIMIT @lagTopN`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), ConsumerLagMetrics)
	args = append(args, clickhouse.Named("lagTopN", uint64(partitionLagTopN)))
	var rows []PartitionLagSnapshotRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryPartitionLagSnapshot", &rows, query, args...)
}

// ============================================================================
// Rebalance signals
// ============================================================================

func (r *ClickHouseRepository) QueryRebalanceSignals(ctx context.Context, teamID int64, startMs, endMs int64) ([]RebalanceMetricRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    timestamp                                                AS timestamp,
		    attributes.'messaging.consumer.group.name'::String       AS consumer_group,
		    metric_name                                              AS metric_name,
		    val_sum / val_count AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.consumer.group.name'::String != ''
		ORDER BY timestamp`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), RebalanceMetrics)
	var rows []RebalanceMetricRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryRebalanceSignals", &rows, query, args...)
}

// ============================================================================
// Error panels
// ============================================================================

func (r *ClickHouseRepository) QueryPublishErrorsByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicErrorCounterRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    timestamp                                                AS timestamp,
		    attributes.'messaging.destination.name'::String          AS topic,
		    attributes.'error.type'::String                          AS error_type,
		    val_sum / val_count AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.destination.name'::String != ''
		  AND attributes.'error.type'::String != ''
		  AND lower(attributes.'messaging.system'::String) = 'kafka'
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), MetricPublishMessages)
	var rows []TopicErrorCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryPublishErrorsByTopic", &rows, query, args...)
}

const counterErrorsByGroupQuery = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    timestamp                                                AS timestamp,
		    attributes.'messaging.consumer.group.name'::String       AS consumer_group,
		    attributes.'error.type'::String                          AS error_type,
		    val_sum / val_count AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.consumer.group.name'::String != ''
		  AND attributes.'error.type'::String != ''
		  AND lower(attributes.'messaging.system'::String) = 'kafka'
		ORDER BY timestamp`

func (r *ClickHouseRepository) queryCounterErrorsByGroup(ctx context.Context, op, metricName string, teamID int64, startMs, endMs int64) ([]GroupErrorCounterRow, error) {
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	var rows []GroupErrorCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, op, &rows, counterErrorsByGroupQuery, args...)
}

func (r *ClickHouseRepository) QueryReceiveErrorsByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupErrorCounterRow, error) {
	return r.queryCounterErrorsByGroup(ctx, "kafka.QueryReceiveErrorsByGroup", MetricReceiveMessages, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryProcessErrorsByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupErrorCounterRow, error) {
	return r.queryCounterErrorsByGroup(ctx, "kafka.QueryProcessErrorsByGroup", MetricProcessMessages, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryClientOpErrorsByOperation(ctx context.Context, teamID int64, startMs, endMs int64) ([]OperationErrorCounterRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    timestamp                                                AS timestamp,
		    attributes.'messaging.operation.name'::String            AS operation_name,
		    attributes.'error.type'::String                          AS error_type,
		    toFloat64(hist_count)                                    AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.operation.name'::String != ''
		  AND attributes.'error.type'::String != ''
		  AND lower(attributes.'messaging.system'::String) = 'kafka'
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), MetricClientOperationDuration)
	var rows []OperationErrorCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryClientOpErrorsByOperation", &rows, query, args...)
}

// ============================================================================
// Broker connections
// ============================================================================

func (r *ClickHouseRepository) QueryBrokerConnectionsSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]BrokerGaugeRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT
		    timestamp                                AS timestamp,
		    attributes.'server.address'::String      AS broker,
		    val_sum / val_count AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'server.address'::String != ''
		  AND lower(attributes.'messaging.system'::String) = 'kafka'
		ORDER BY timestamp`
	args := withMetricName(metricArgs(teamID, startMs, endMs), MetricClientConnections)
	var rows []BrokerGaugeRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryBrokerConnectionsSeries", &rows, query, args...)
}

// ============================================================================
// Catalog samples — intentionally generic (caller picks the metric set per
// topic/group fan-out for the kafka-explorer catalog views).
// ============================================================================

func (r *ClickHouseRepository) QueryConsumerMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]ConsumerMetricSample, error) {
	if len(metricNames) == 0 {
		return []ConsumerMetricSample{}, nil
	}
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'messaging.consumer.group.name'::String       AS consumer_group,
		    attributes.'node-id'::String                             AS node_id,
		    metric_name                                              AS metric_name,
		    toFloat64(argMax(value, timestamp))                      AS value,
		    formatDateTime(max(timestamp), '%Y-%m-%d %H:%M:%S')      AS timestamp
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.consumer.group.name'::String != ''
		GROUP BY consumer_group, node_id, metric_name
		ORDER BY consumer_group, node_id, metric_name`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), metricNames)
	var rows []ConsumerMetricSample
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryConsumerMetricSamples", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryTopicMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]TopicMetricSample, error) {
	if len(metricNames) == 0 {
		return []TopicMetricSample{}, nil
	}
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    attributes.'messaging.destination.name'::String          AS topic,
		    attributes.'messaging.consumer.group.name'::String       AS consumer_group,
		    metric_name                                              AS metric_name,
		    toFloat64(argMax(value, timestamp))                      AS value,
		    formatDateTime(max(timestamp), '%Y-%m-%d %H:%M:%S')      AS timestamp
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.destination.name'::String != ''
		GROUP BY topic, consumer_group, metric_name
		ORDER BY topic, consumer_group, metric_name`
	args := withMetricNames(metricArgs(teamID, startMs, endMs), metricNames)
	var rows []TopicMetricSample
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryTopicMetricSamples", &rows, query, args...)
}
