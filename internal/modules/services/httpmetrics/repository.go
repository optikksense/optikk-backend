package httpmetrics

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

type HistogramAggRow struct {
	SumHistSum   float64   `ch:"sum_hist_sum"`
	SumHistCount uint64    `ch:"sum_hist_count"`
	Buckets      []float64 `ch:"buckets"`
	Counts       []uint64  `ch:"counts"`
}

type StatusCountRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	StatusCode uint16    `ch:"status_code"`
	Count      uint64    `ch:"count"`
}

type MetricSeriesRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Value     float64   `ch:"value"`
}

type RouteAggRow struct {
	Route    string  `ch:"route"`
	Count    uint64  `ch:"req_count"`
	P95Ms    float32 `ch:"p95_ms"`
	ErrCount uint64  `ch:"err_count"`
}

type RouteErrorSeriesRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Route     string    `ch:"route"`
	Count     uint64    `ch:"req_count"`
	ErrCount  uint64    `ch:"err_count"`
}

type HostAggRow struct {
	Host     string  `ch:"host"`
	Count    uint64  `ch:"req_count"`
	P95Ms    float32 `ch:"p95_ms"`
	ErrCount uint64  `ch:"err_count"`
}

type Repository interface {
	QueryHistogramAgg(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) (HistogramAggRow, error)
	QueryStatusHistogramSeries(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]StatusCountRow, error)
	QueryMetricSeries(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]MetricSeriesRow, error)

	QueryRouteAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteAggRow, error)
	QueryRouteErrorSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteErrorSeriesRow, error)
	QueryExternalHostAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]HostAggRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
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
		    sum(hist_sum)           AS sum_hist_sum,
		    sum(hist_count)         AS sum_hist_count,
		    any(hist_buckets)       AS buckets,
		    sumForEach(hist_counts) AS counts
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end`
	var row HistogramAggRow
	err := dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "httpmetrics.QueryHistogramAgg",
		&row, query, metricArgs(teamID, startMs, endMs, metricName)...)
	return row, err
}

func (r *ClickHouseRepository) QueryStatusHistogramSeries(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]StatusCountRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT timestamp, http_status_code AS status_code, hist_count AS count
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND http_status_code != 0
		ORDER BY timestamp, status_code`
	var rows []StatusCountRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "httpmetrics.QueryStatusHistogramSeries",
		&rows, query, metricArgs(teamID, startMs, endMs, metricName)...)
	return rows, err
}

func (r *ClickHouseRepository) QueryMetricSeries(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]MetricSeriesRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT timestamp, value
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket_hour BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		ORDER BY timestamp`
	var rows []MetricSeriesRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "httpmetrics.QueryMetricSeries",
		&rows, query, metricArgs(teamID, startMs, endMs, metricName)...)
	return rows, err
}

func (r *ClickHouseRepository) QueryRouteAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteAggRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT name AS route,
		       count()                                                          AS req_count,
		       quantileTiming(0.95)(duration_nano / 1000000.0)                 AS p95_ms,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400) AS err_count
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND is_root = 1
		  AND name   != ''
		GROUP BY route`
	var rows []RouteAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "httpmetrics.QueryRouteAgg",
		&rows, query, spanArgs(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) QueryRouteErrorSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteErrorSeriesRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toDateTime(ts_bucket)                                              AS timestamp,
		       name                                                               AS route,
		       count()                                                            AS req_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)  AS err_count
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND is_root = 1
		  AND name   != ''
		GROUP BY ts_bucket, route
		ORDER BY timestamp, route`
	var rows []RouteErrorSeriesRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "httpmetrics.QueryRouteErrorSeries",
		&rows, query, spanArgs(teamID, startMs, endMs)...)
	return rows, err
}

func (r *ClickHouseRepository) QueryExternalHostAgg(ctx context.Context, teamID int64, startMs, endMs int64) ([]HostAggRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT peer_service AS host,
		       count()                                                          AS req_count,
		       quantileTiming(0.95)(duration_nano / 1000000.0)                 AS p95_ms,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400) AS err_count
		FROM observability.spans
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND kind_string  = 'SPAN_KIND_CLIENT'
		  AND peer_service != ''
		GROUP BY host`
	var rows []HostAggRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "httpmetrics.QueryExternalHostAgg",
		&rows, query, spanArgs(teamID, startMs, endMs)...)
	return rows, err
}

func metricArgs(teamID int64, startMs, endMs int64, metricName string) []any {
	bucketStart, bucketEnd := metricBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — TeamID fits UInt32
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricName", metricName),
	}
}

func spanArgs(teamID int64, startMs, endMs int64) []any {
	bucketStart, bucketEnd := spanBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — TeamID fits UInt32
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func metricBucketBounds(startMs, endMs int64) (time.Time, time.Time) {
	return timebucket.MetricsHourBucket(startMs / 1000),
		timebucket.MetricsHourBucket(endMs / 1000).Add(time.Hour)
}

func spanBucketBounds(startMs, endMs int64) (uint64, uint64) {
	return timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs/1000) + uint64(timebucket.SpansBucketSeconds)
}
