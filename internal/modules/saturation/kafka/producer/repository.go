package producer

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

func (r *Repository) QueryPublishRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicCounterRow, error) {
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
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), filter.ProducerMetrics)
	var rows []TopicCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryPublishRateByTopic", &rows, query, args...)
}

// QueryPublishLatencyByTopic emits per-(display_bucket, topic) percentiles
// merged server-side. Time bucketing dispatches to the specific
// toStartOf{Minute,FiveMinutes,Hour,Day} via timebucket.DisplayGrainSQL.
func (r *Repository) QueryPublishLatencyByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicHistogramRow, error) {
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
	args := filter.WithMetricName(filter.MetricArgs(teamID, startMs, endMs), filter.MetricPublishDuration)
	args = append(args, clickhouse.Named("opDuration", filter.MetricClientOperationDuration))
	args = filter.WithOpAliases(args, filter.PublishOperationAliases)
	var rows []TopicHistogramRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryPublishLatencyByTopic", &rows, query, args...)
}

func (r *Repository) QueryPublishErrorsByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicErrorCounterRow, error) {
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
	args := filter.WithMetricName(filter.MetricArgs(teamID, startMs, endMs), filter.MetricPublishMessages)
	var rows []TopicErrorCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryPublishErrorsByTopic", &rows, query, args...)
}
