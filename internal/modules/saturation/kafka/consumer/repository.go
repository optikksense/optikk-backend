package consumer

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka/filter"
)

// partitionLagTopN caps lag-per-partition response rows.
const partitionLagTopN = 200

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
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

func (r *Repository) QueryConsumeRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicCounterRow, error) {
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), filter.ConsumerMetrics)
	var rows []TopicCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryConsumeRateByTopic", &rows, counterSeriesByTopicQuery, args...)
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

func (r *Repository) queryCounterSeriesByGroup(ctx context.Context, op string, teamID int64, startMs, endMs int64, metricNames []string) ([]GroupCounterRow, error) {
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), metricNames)
	var rows []GroupCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, op, &rows, counterSeriesByGroupQuery, args...)
}

func (r *Repository) QueryConsumeRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupCounterRow, error) {
	return r.queryCounterSeriesByGroup(ctx, "kafka.QueryConsumeRateByGroup", teamID, startMs, endMs, filter.ConsumerMetrics)
}

func (r *Repository) QueryProcessRateByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupCounterRow, error) {
	return r.queryCounterSeriesByGroup(ctx, "kafka.QueryProcessRateByGroup", teamID, startMs, endMs, filter.ProcessMetrics)
}

// ============================================================================
// Latency panels — histogram series merged SQL-side per (display_bucket, dim)
// ============================================================================

func (r *Repository) QueryReceiveLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicHistogramRow, error) {
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN (@metricName, @opDuration)
		)
		SELECT timestamp,
		       topic,
		       qs[1] AS p50,
		       qs[2] AS p95,
		       qs[3] AS p99
		FROM (
		    SELECT ` + timebucket.DisplayGrainSQL(endMs-startMs) + `        AS timestamp,
		           attributes.'messaging.destination.name'::String           AS topic,
		           quantilesPrometheusHistogramMerge(0.5, 0.95, 0.99)(latency_state) AS qs
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
		)
		ORDER BY timestamp, topic`
	args := filter.WithMetricName(filter.MetricArgs(teamID, startMs, endMs), filter.MetricReceiveDuration)
	args = append(args, clickhouse.Named("opDuration", filter.MetricClientOperationDuration))
	args = filter.WithOpAliases(args, filter.ReceiveOperationAliases)
	var rows []TopicHistogramRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryReceiveLatencyByTopic", &rows, query, args...)
}

func (r *Repository) QueryProcessLatencyByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupHistogramRow, error) {
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN (@metricName, @opDuration)
		)
		SELECT timestamp,
		       consumer_group,
		       qs[1] AS p50,
		       qs[2] AS p95,
		       qs[3] AS p99
		FROM (
		    SELECT ` + timebucket.DisplayGrainSQL(endMs-startMs) + `        AS timestamp,
		           attributes.'messaging.consumer.group.name'::String       AS consumer_group,
		           quantilesPrometheusHistogramMerge(0.5, 0.95, 0.99)(latency_state) AS qs
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
		)
		ORDER BY timestamp, consumer_group`
	args := filter.WithMetricName(filter.MetricArgs(teamID, startMs, endMs), filter.MetricProcessDuration)
	args = append(args, clickhouse.Named("opDuration", filter.MetricClientOperationDuration))
	args = filter.WithOpAliases(args, filter.ProcessOperationAliases)
	var rows []GroupHistogramRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryProcessLatencyByGroup", &rows, query, args...)
}

// ============================================================================
// Error panels
// ============================================================================

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

func (r *Repository) queryCounterErrorsByGroup(ctx context.Context, op, metricName string, teamID int64, startMs, endMs int64) ([]GroupErrorCounterRow, error) {
	args := filter.WithMetricName(filter.MetricArgs(teamID, startMs, endMs), metricName)
	var rows []GroupErrorCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, op, &rows, counterErrorsByGroupQuery, args...)
}

func (r *Repository) QueryReceiveErrorsByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupErrorCounterRow, error) {
	return r.queryCounterErrorsByGroup(ctx, "kafka.QueryReceiveErrorsByGroup", filter.MetricReceiveMessages, teamID, startMs, endMs)
}

func (r *Repository) QueryProcessErrorsByGroup(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupErrorCounterRow, error) {
	return r.queryCounterErrorsByGroup(ctx, "kafka.QueryProcessErrorsByGroup", filter.MetricProcessMessages, teamID, startMs, endMs)
}

// ============================================================================
// Lag panels
// ============================================================================

func (r *Repository) QueryConsumerLagByGroupTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupTopicGaugeRow, error) {
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
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), filter.ConsumerLagMetrics)
	var rows []GroupTopicGaugeRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryConsumerLagByGroupTopic", &rows, query, args...)
}

func (r *Repository) QueryPartitionLagSnapshot(ctx context.Context, teamID int64, startMs, endMs int64) ([]PartitionLagSnapshotRow, error) {
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
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), filter.ConsumerLagMetrics)
	args = append(args, clickhouse.Named("lagTopN", uint64(partitionLagTopN)))
	var rows []PartitionLagSnapshotRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryPartitionLagSnapshot", &rows, query, args...)
}

// ============================================================================
// Rebalance signals
// ============================================================================

func (r *Repository) QueryRebalanceSignals(ctx context.Context, teamID int64, startMs, endMs int64) ([]RebalanceMetricRow, error) {
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
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), filter.RebalanceMetrics)
	var rows []RebalanceMetricRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryRebalanceSignals", &rows, query, args...)
}
