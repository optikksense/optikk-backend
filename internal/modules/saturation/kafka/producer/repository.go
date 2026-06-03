package producer

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
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
		    ifNotFinite(val_sum / val_count, 0) AS value
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
