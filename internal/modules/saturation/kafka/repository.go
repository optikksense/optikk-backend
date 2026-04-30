package kafka

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

// Each method opens a `WITH active_fps AS (... metrics_resource ...)` CTE so
// the main `observability.metrics` scan PREWHEREs on (team_id, ts_bucket,
// fingerprint) — the first three slots of the metrics PK. One CH call per
// method; service.go composes panels that need multiple metrics
// (summary-stats, e2e-latency, rebalance-signals).
//
// All attribute reads use OTel semconv 1.30+ canonical paths. Empty strings
// from missing JSON keys are filtered out via `... != ''` predicates.
//   topic            → attributes.'messaging.destination.name'
//   consumer_group   → attributes.'messaging.consumer.group.name'
//   operation_name   → attributes.'messaging.operation.name'
//   partition        → attributes.'messaging.kafka.destination.partition'
//   messaging.system → attributes.'messaging.system'
//   broker           → attributes.'server.address'
//   error.type       → attributes.'error.type'
//   node_id          → attributes.'node-id'

func (r *ClickHouseRepository) QueryCounterSeriesByTopic(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]TopicCounterRow, error) {
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
	args := withMetricNames(metricArgs(teamID, startMs, endMs), metricNames)
	var rows []TopicCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryCounterSeriesByTopic", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryCounterSeriesByGroup(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]GroupCounterRow, error) {
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
	args := withMetricNames(metricArgs(teamID, startMs, endMs), metricNames)
	var rows []GroupCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryCounterSeriesByGroup", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryCounterErrorsByTopic(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]TopicErrorCounterRow, error) {
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
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	var rows []TopicErrorCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryCounterErrorsByTopic", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryCounterErrorsByGroup(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]GroupErrorCounterRow, error) {
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
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	var rows []GroupErrorCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryCounterErrorsByGroup", &rows, query, args...)
}

// QueryHistogramCountErrorsByOperation reads `hist_count` (not `value`) from
// the unified messaging.client.operation.duration histogram — for the
// errored-op count per (operation_name, error_type).
func (r *ClickHouseRepository) QueryHistogramCountErrorsByOperation(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]OperationErrorCounterRow, error) {
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
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	var rows []OperationErrorCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryHistogramCountErrorsByOperation", &rows, query, args...)
}

// QueryHistogramSeriesByTopic absorbs both the canonical duration metric AND
// the unified messaging.client.operation.duration filtered by
// `messaging.operation.name` IN @opAliases — some instrumentations emit only
// the unified one.
func (r *ClickHouseRepository) QueryHistogramSeriesByTopic(ctx context.Context, teamID int64, startMs, endMs int64, metricName string, opAliases []string) ([]TopicHistogramRow, error) {
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
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	args = append(args, clickhouse.Named("opDuration", MetricClientOperationDuration))
	args = withOpAliases(args, opAliases)
	args = withStepSec(args, startMs, endMs)
	var rows []TopicHistogramRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryHistogramSeriesByTopic", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryHistogramSeriesByGroup(ctx context.Context, teamID int64, startMs, endMs int64, metricName string, opAliases []string) ([]GroupHistogramRow, error) {
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
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	args = append(args, clickhouse.Named("opDuration", MetricClientOperationDuration))
	args = withOpAliases(args, opAliases)
	args = withStepSec(args, startMs, endMs)
	var rows []GroupHistogramRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryHistogramSeriesByGroup", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryHistogramSeriesByOperation(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]OperationHistogramRow, error) {
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
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	args = withStepSec(args, startMs, endMs)
	var rows []OperationHistogramRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryHistogramSeriesByOperation", &rows, query, args...)
}

// QueryHistogramSeriesByMetricAndTopic projects metric_name into the row so
// the service can fold the 3 e2e metrics (publish/receive/process duration)
// into a single per-(timestamp, topic) record.
func (r *ClickHouseRepository) QueryHistogramSeriesByMetricAndTopic(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]TopicMetricHistogramRow, error) {
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
	args := withMetricNames(metricArgs(teamID, startMs, endMs), metricNames)
	args = withStepSec(args, startMs, endMs)
	var rows []TopicMetricHistogramRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryHistogramSeriesByMetricAndTopic", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryHistogramAgg(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) (HistogramAggRow, error) {
	const query = `
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
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	var row HistogramAggRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryHistogramAgg", &row, query, args...)
}

func (r *ClickHouseRepository) QueryCounterAgg(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) (CounterAggRow, error) {
	const query = `
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
	args := withMetricNames(metricArgs(teamID, startMs, endMs), metricNames)
	var row CounterAggRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryCounterAgg", &row, query, args...)
}

// QueryGaugeMax — lag metric names are messaging.kafka.* so the metric_name
// itself scopes to kafka; no messaging.system pin needed.
func (r *ClickHouseRepository) QueryGaugeMax(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) (GaugeMaxRow, error) {
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
	args := withMetricNames(metricArgs(teamID, startMs, endMs), metricNames)
	var row GaugeMaxRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryGaugeMax", &row, query, args...)
}

func (r *ClickHouseRepository) QueryGaugeSeriesByGroupTopic(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]GroupTopicGaugeRow, error) {
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
	args := withMetricNames(metricArgs(teamID, startMs, endMs), metricNames)
	var rows []GroupTopicGaugeRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryGaugeSeriesByGroupTopic", &rows, query, args...)
}

// QueryPartitionLagSnapshot uses argMax to pull the latest gauge value per
// (topic, partition, group) tuple, ordered by lag DESC and capped at the
// table-view limit. Partition is returned as a string (parsed to int64 in
// the service).
func (r *ClickHouseRepository) QueryPartitionLagSnapshot(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]PartitionLagSnapshotRow, error) {
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
	args := withMetricNames(metricArgs(teamID, startMs, endMs), metricNames)
	args = append(args, clickhouse.Named("lagTopN", uint64(partitionLagTopN)))
	var rows []PartitionLagSnapshotRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryPartitionLagSnapshot", &rows, query, args...)
}

func (r *ClickHouseRepository) QueryGaugeSeriesByBroker(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]BrokerGaugeRow, error) {
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
	args := withMetricName(metricArgs(teamID, startMs, endMs), metricName)
	var rows []BrokerGaugeRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryGaugeSeriesByBroker", &rows, query, args...)
}

// QueryRebalanceSignals projects metric_name so the service can fold all 6
// rebalance metrics into one RebalancePoint per (display_bucket, group).
func (r *ClickHouseRepository) QueryRebalanceSignals(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]RebalanceMetricRow, error) {
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
	args := withMetricNames(metricArgs(teamID, startMs, endMs), metricNames)
	var rows []RebalanceMetricRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryRebalanceSignals", &rows, query, args...)
}

// QueryConsumerMetricSamples returns the latest sample per
// (consumer_group, node_id, metric_name) within the window — argMax pushes
// "latest by timestamp" into SQL so callers don't scan all samples client-side.
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

// QueryTopicMetricSamples returns the latest sample per
// (topic, consumer_group, metric_name) within the window.
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
