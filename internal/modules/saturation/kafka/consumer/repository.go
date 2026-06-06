package consumer

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

// Counter rate panels.

const counterSeriesByTopicQuery = `
		SELECT
		    timestamp,
		    messaging_destination                AS topic,
		    ifNotFinite(val_sum / val_count, 0)  AS value
		FROM observability.metrics_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name IN @metricNames
		     AND timestamp   BETWEEN @start AND @end
		WHERE messaging_destination != ''
		  AND lower(messaging_system) = 'kafka'
		ORDER BY timestamp`

func (r *Repository) QueryConsumeRateByTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]TopicCounterRow, error) {
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), filter.ConsumerMetrics)
	var rows []TopicCounterRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryConsumeRateByTopic", &rows, counterSeriesByTopicQuery, args...)
}

// Lag panels.

func (r *Repository) QueryConsumerLagByGroupTopic(ctx context.Context, teamID int64, startMs, endMs int64) ([]GroupTopicGaugeRow, error) {
	const query = `
		SELECT
		    timestamp,
		    messaging_consumer_group             AS consumer_group,
		    messaging_destination                AS topic,
		    ifNotFinite(val_sum / val_count, 0)  AS value
		FROM observability.metrics_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name IN @metricNames
		     AND timestamp   BETWEEN @start AND @end
		WHERE messaging_consumer_group != ''
		ORDER BY timestamp`
	args := filter.WithMetricNames(filter.MetricArgs(teamID, startMs, endMs), filter.ConsumerLagMetrics)
	var rows []GroupTopicGaugeRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "kafka.QueryConsumerLagByGroupTopic", &rows, query, args...)
}
