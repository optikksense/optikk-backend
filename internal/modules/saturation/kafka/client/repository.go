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
		       quantilesPrometheusHistogramMerge(0.5, 0.95, 0.99)(latency_state)[2] AS p95
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
