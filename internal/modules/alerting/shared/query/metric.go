package query

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// MetricBackend evaluates metric monitors against observability.metrics_1m.
// avg/sum/min/max read the SimpleAggregateFunction columns; p50/p95/p99 read
// the AggregateFunction(quantilesPrometheusHistogram, ...) state column.
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

	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id     = @teamID
		         AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		         AND metric_name = @metricName
		)
		SELECT ` + metricAggExpr() + ` AS value
		FROM observability.metrics_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name = @metricName
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end`

	args := metricArgs(m.TeamID, q.Metric.Metric, startMs, endMs, q.Metric.Aggregation)
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

	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id     = @teamID
		         AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		         AND metric_name = @metricName
		)
		SELECT ` + timebucket.DisplayGrainSQL(windowMs) + ` AS bucket, ` +
		metricAggExpr() + ` AS value
		FROM observability.metrics_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND metric_name = @metricName
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		GROUP BY bucket
		ORDER BY bucket`

	args := metricArgs(m.TeamID, q.Metric.Metric, startMs, endMs, q.Metric.Aggregation)
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

// metricAggExpr returns the SELECT-list expression for the aggregation. The
// percentile arms use the Prometheus-style merge so the result is a true
// histogram_quantile-equivalent value.
func metricAggExpr() string {
	// The aggregation isn't a bind because CH doesn't allow parameter
	// substitution in projection function calls. The caller validates the
	// aggregation against a closed set before this is reached. Single
	// responsibility: pick the projection by agg name.
	return `multiIf(
		@aggregation = 'avg', sum(val_sum) / sum(val_count),
		@aggregation = 'sum', sum(val_sum),
		@aggregation = 'min', min(val_min),
		@aggregation = 'max', max(val_max),
		@aggregation = 'p50', (quantilesPrometheusHistogramMerge(0.50, 0.95, 0.99)(latency_state))[1],
		@aggregation = 'p95', (quantilesPrometheusHistogramMerge(0.50, 0.95, 0.99)(latency_state))[2],
		@aggregation = 'p99', (quantilesPrometheusHistogramMerge(0.50, 0.95, 0.99)(latency_state))[3],
		sum(val_sum) / sum(val_count))`
}

func metricArgs(teamID int64, metricName string, startMs, endMs int64, agg string) []any {
	bs, be := bucketBounds(startMs, endMs)
	if agg == "" {
		agg = "avg"
	}
	return []any{
		teamIDArg(teamID),
		clickhouse.Named("bucketStart", bs),
		clickhouse.Named("bucketEnd", be),
		clickhouse.Named("metricName", metricName),
		clickhouse.Named("aggregation", agg),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// scalarRow is the destination for a single-row Scalar query.
type scalarRow struct {
	Value float64 `ch:"value"`
}

// IsZeroNoData heuristic: an exact 0.0 from the no-rows-but-Coalesced default
// can't be distinguished from a legit zero scalar. We treat the no-rows case
// in the caller via len(rows)==0; this method exists for future heuristic use.
func (s scalarRow) IsZeroNoData() bool { return false }

// bucketRow is the destination for a Series query.
type bucketRow struct {
	Bucket time.Time `ch:"bucket"`
	Value  float64   `ch:"value"`
}

func bucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}
