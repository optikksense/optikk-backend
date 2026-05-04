package client

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka/filter"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
}

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

func (r *Repository) queryCounterAgg(ctx context.Context, op string, teamID int64, startMs, endMs int64, metricNames []string) (CounterAggRow, error) {
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), metricNames)
	var row CounterAggRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, op, &row, counterAggQuery, args...)
}

func (r *Repository) QueryPublishMessageCount(ctx context.Context, teamID int64, startMs, endMs int64) (CounterAggRow, error) {
	return r.queryCounterAgg(ctx, "kafka.QueryPublishMessageCount", teamID, startMs, endMs, filter.ProducerMetrics)
}

func (r *Repository) QueryReceiveMessageCount(ctx context.Context, teamID int64, startMs, endMs int64) (CounterAggRow, error) {
	return r.queryCounterAgg(ctx, "kafka.QueryReceiveMessageCount", teamID, startMs, endMs, filter.ConsumerMetrics)
}

func (r *Repository) QueryMaxConsumerLag(ctx context.Context, teamID int64, startMs, endMs int64) (GaugeMaxRow, error) {
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
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), filter.ConsumerLagMetrics)
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
		SELECT sum_hist_sum,
		       sum_hist_count,
		       qs[1] AS p50,
		       qs[2] AS p95,
		       qs[3] AS p99
		FROM (
		    SELECT sum(hist_sum)                                                  AS sum_hist_sum,
		           sum(hist_count)                                                AS sum_hist_count,
		           quantilesPrometheusHistogramMerge(0.5, 0.95, 0.99)(latency_state) AS qs
		    FROM observability.metrics_1m
		    PREWHERE team_id        = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND fingerprint   IN active_fps
		    WHERE metric_name = @metricName
		      AND timestamp BETWEEN @start AND @end
		      AND lower(attributes.'messaging.system'::String) = 'kafka'
		)`

func (r *Repository) queryHistogramAgg(ctx context.Context, op, metricName string, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	args := filter.WithMetricName(filter.MetricArgs(teamID, startMs, endMs), metricName)
	var row HistogramAggRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, op, &row, histogramAggQuery, args...)
}

func (r *Repository) QueryPublishDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	return r.queryHistogramAgg(ctx, "kafka.QueryPublishDurationHistogram", filter.MetricPublishDuration, teamID, startMs, endMs)
}

func (r *Repository) QueryReceiveDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	return r.queryHistogramAgg(ctx, "kafka.QueryReceiveDurationHistogram", filter.MetricReceiveDuration, teamID, startMs, endMs)
}

// ============================================================================
// E2E latency
// ============================================================================

func (r *Repository) QueryE2ELatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicMetricHistogramRow, error) {
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT timestamp,
		       topic,
		       metric_name,
		       quantilePrometheusHistogramMerge(0.95)(latency_state) AS p95
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND attributes.'messaging.destination.name'::String != ''
		  AND lower(attributes.'messaging.system'::String) = 'kafka'
		GROUP BY ` + timebucket.DisplayGrainSQL(endMs-startMs) + ` AS timestamp,
		         attributes.'messaging.destination.name'::String AS topic,
		         metric_name
		ORDER BY timestamp, topic, metric_name`
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), []string{filter.MetricPublishDuration, filter.MetricReceiveDuration, filter.MetricProcessDuration})
	var rows []TopicMetricHistogramRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryE2ELatencyByTopic", &rows, query, args...)
}

// ============================================================================
// Broker connections + client-op duration
// ============================================================================

func (r *Repository) QueryBrokerConnectionsSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]BrokerGaugeRow, error) {
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
	args := filter.WithMetricName(filter.MetricArgs(teamID, startMs, endMs), filter.MetricClientConnections)
	var rows []BrokerGaugeRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryBrokerConnectionsSeries", &rows, query, args...)
}

func (r *Repository) QueryClientOperationDurationByOp(ctx context.Context, teamID int64, startMs, endMs int64) ([]OperationHistogramRow, error) {
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT timestamp,
		       operation_name,
		       qs[1] AS p50,
		       qs[2] AS p95,
		       qs[3] AS p99
		FROM (
		    SELECT ` + timebucket.DisplayGrainSQL(endMs-startMs) + `        AS timestamp,
		           attributes.'messaging.operation.name'::String            AS operation_name,
		           quantilesPrometheusHistogramMerge(0.5, 0.95, 0.99)(latency_state) AS qs
		    FROM observability.metrics_1m
		    PREWHERE team_id        = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND fingerprint   IN active_fps
		    WHERE metric_name = @metricName
		      AND timestamp BETWEEN @start AND @end
		      AND attributes.'messaging.operation.name'::String != ''
		      AND lower(attributes.'messaging.system'::String) = 'kafka'
		    GROUP BY timestamp, operation_name
		)
		ORDER BY timestamp, operation_name`
	args := filter.WithMetricName(filter.MetricArgs(teamID, startMs, endMs), filter.MetricClientOperationDuration)
	var rows []OperationHistogramRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryClientOperationDurationByOp", &rows, query, args...)
}

func (r *Repository) QueryClientOpErrorsByOperation(ctx context.Context, teamID int64, startMs, endMs int64) ([]OperationErrorCounterRow, error) {
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
	args := filter.WithMetricName(filter.MetricArgs(teamID, startMs, endMs), filter.MetricClientOperationDuration)
	var rows []OperationErrorCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryClientOpErrorsByOperation", &rows, query, args...)
}
