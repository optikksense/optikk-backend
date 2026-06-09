package query

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
	"github.com/Optikk-Org/optikk-backend/internal/shared/chargs"
)

// MetricBackend evaluates metric monitors against ClickHouse rollup tables.
type MetricBackend struct {
	db clickhouse.Conn
}

func NewMetricBackend(db clickhouse.Conn) *MetricBackend { return &MetricBackend{db: db} }

func (b *MetricBackend) Scalar(ctx context.Context, m models.MonitorRow, q models.MonitorQuery, scope models.Scope, _ models.Conditions, now time.Time) (ScalarResult, error) {
	if q.Metric == nil {
		return ScalarResult{}, nil
	}
	windowSec := int64(q.Metric.WindowSec)
	if windowSec <= 0 {
		windowSec = 300
	}
	endMs := now.UnixMilli()
	startMs := endMs - windowSec*1000

	table, expr := metricSource(q.Metric.Aggregation)
	query := `
		SELECT ` + expr + ` AS value
		FROM observability.` + table + `
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name = @metricName
		WHERE timestamp BETWEEN @start AND @end`

	args := metricArgs(m.TeamID, q.Metric.Metric, startMs, endMs)
	var rows []scalarRow
	if err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), b.db, "alerting.metric.Scalar", &rows, query, args...); err != nil {
		return ScalarResult{}, err
	}
	if len(rows) == 0 {
		return ScalarResult{}, nil
	}
	r := rows[0]
	return ScalarResult{Value: r.Value, HasData: !r.IsZeroNoData()}, nil
}

func (b *MetricBackend) Series(ctx context.Context, m models.MonitorRow, q models.MonitorQuery, scope models.Scope, _ models.Conditions, windowMs int64, now time.Time) ([]Point, error) {
	if q.Metric == nil {
		return nil, nil
	}
	endMs := now.UnixMilli()
	startMs := endMs - windowMs

	table, expr := metricSource(q.Metric.Aggregation)
	query := `
		SELECT ` + timebucket.DisplayGrainSQL(windowMs) + ` AS bucket, ` +
		expr + ` AS value
		FROM observability.` + table + `
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name = @metricName
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY bucket
		ORDER BY bucket`

	args := metricArgs(m.TeamID, q.Metric.Metric, startMs, endMs)
	var rows []bucketRow
	if err := dbutil.SelectCH(dbutil.DashboardCtx(ctx), b.db, "alerting.metric.Series", &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]Point, 0, len(rows))
	for _, r := range rows {
		out = append(out, Point{BucketMs: r.Bucket.UnixMilli(), Value: r.Value})
	}
	return out, nil
}

// metricSource picks the rollup table and SELECT expression for aggregation.
func metricSource(agg string) (table, expr string) {
	switch agg {
	case "sum":
		return "metrics_1m", "sum(val_sum)"
	case "min":
		return "metrics_1m", "min(val_min)"
	case "max":
		return "metrics_1m", "max(val_max)"
	case "p50":
		return "metrics_hist_1m", "(quantilesPrometheusHistogramMerge(0.50, 0.95, 0.99)(latency_state))[1]"
	case "p95":
		return "metrics_hist_1m", "(quantilesPrometheusHistogramMerge(0.50, 0.95, 0.99)(latency_state))[2]"
	case "p99":
		return "metrics_hist_1m", "(quantilesPrometheusHistogramMerge(0.50, 0.95, 0.99)(latency_state))[3]"
	default: // avg
		return "metrics_1m", "sum(val_sum) / sum(val_count)"
	}
}

func metricArgs(teamID int64, metricName string, startMs, endMs int64) []any {
	bs, be := chargs.BucketBounds(startMs, endMs)
	return []any{
		teamIDArg(teamID),
		clickhouse.Named("bucketStart", bs),
		clickhouse.Named("bucketEnd", be),
		clickhouse.Named("metricName", metricName),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// scalarRow is the destination for a single-row Scalar query.
type scalarRow struct {
	Value float64 `ch:"value"`
}

// IsZeroNoData checks if the returned scalar represents no data.
func (s scalarRow) IsZeroNoData() bool { return false }

// bucketRow is the destination for a Series query.
type bucketRow struct {
	Bucket time.Time `ch:"bucket"`
	Value  float64   `ch:"value"`
}
