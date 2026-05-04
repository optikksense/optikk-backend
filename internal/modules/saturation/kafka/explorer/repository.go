package explorer

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/kafka/filter"
)

// Repository owns the catalog-shape "latest value per dim" queries. The two
// readers intentionally take a caller-supplied metric name set so the service
// can scope each fan-out (per-topic vs per-group catalog views).
type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
}

func (r *Repository) QueryConsumerMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]ConsumerMetricSample, error) {
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
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), metricNames)
	var rows []ConsumerMetricSample
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryConsumerMetricSamples", &rows, query, args...)
}

func (r *Repository) QueryTopicMetricSamples(ctx context.Context, teamID int64, startMs, endMs int64, metricNames []string) ([]TopicMetricSample, error) {
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
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), metricNames)
	var rows []TopicMetricSample
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryTopicMetricSamples", &rows, query, args...)
}
