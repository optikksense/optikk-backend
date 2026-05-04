package httpmetrics

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

type HistogramAggRow struct {
	SumHistSum   float64 `ch:"sum_hist_sum"`
	SumHistCount uint64  `ch:"sum_hist_count"`
	P50          float64 `ch:"p50"`
	P95          float64 `ch:"p95"`
	P99          float64 `ch:"p99"`
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
	QueryServerRequestDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error)
	QueryServerRequestBodySizeHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error)
	QueryServerResponseBodySizeHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error)
	QueryClientRequestDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error)
	QueryDNSLookupDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error)
	QueryTLSConnectDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error)
	QueryServerRequestStatusSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusCountRow, error)
	QueryServerActiveRequestsSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]MetricSeriesRow, error)

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
		)`

func (r *ClickHouseRepository) queryHistogramAgg(ctx context.Context, op, metricName string, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	var row HistogramAggRow
	err := dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, op,
		&row, histogramAggQuery, metricArgs(teamID, startMs, endMs, metricName)...)
	return row, err
}

func (r *ClickHouseRepository) QueryServerRequestDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	return r.queryHistogramAgg(ctx, "httpmetrics.QueryServerRequestDurationHistogram", MetricHTTPServerRequestDuration, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryServerRequestBodySizeHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	return r.queryHistogramAgg(ctx, "httpmetrics.QueryServerRequestBodySizeHistogram", MetricHTTPServerRequestBodySize, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryServerResponseBodySizeHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	return r.queryHistogramAgg(ctx, "httpmetrics.QueryServerResponseBodySizeHistogram", MetricHTTPServerResponseBodySize, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryClientRequestDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	return r.queryHistogramAgg(ctx, "httpmetrics.QueryClientRequestDurationHistogram", MetricHTTPClientRequestDuration, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryDNSLookupDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	return r.queryHistogramAgg(ctx, "httpmetrics.QueryDNSLookupDurationHistogram", MetricDNSLookupDuration, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryTLSConnectDurationHistogram(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramAggRow, error) {
	return r.queryHistogramAgg(ctx, "httpmetrics.QueryTLSConnectDurationHistogram", MetricTLSConnectDuration, teamID, startMs, endMs)
}

func (r *ClickHouseRepository) QueryServerRequestStatusSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusCountRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT timestamp, http_status_code AS status_code, hist_count AS count
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND http_status_code != 0
		ORDER BY timestamp, status_code`
	var rows []StatusCountRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "httpmetrics.QueryServerRequestStatusSeries",
		&rows, query, metricArgs(teamID, startMs, endMs, MetricHTTPServerRequestDuration)...)
	return rows, err
}

func (r *ClickHouseRepository) QueryServerActiveRequestsSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]MetricSeriesRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name = @metricName
		)
		SELECT timestamp, val_sum / val_count AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		ORDER BY timestamp`
	var rows []MetricSeriesRow
	err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "httpmetrics.QueryServerActiveRequestsSeries",
		&rows, query, metricArgs(teamID, startMs, endMs, MetricHTTPServerActiveRequests)...)
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
		       sum(request_count)                                               AS req_count,
		       quantileTimingMerge(0.95)(latency_state)                         AS p95_ms,
		       sum(error_count)                                                 AS err_count
		FROM observability.spans_1m
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
		SELECT toDateTime(ts_bucket)        AS timestamp,
		       name                         AS route,
		       sum(request_count)           AS req_count,
		       sum(error_count)             AS err_count
		FROM observability.spans_1m
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
		       sum(request_count)                                               AS req_count,
		       quantileTimingMerge(0.95)(latency_state)                         AS p95_ms,
		       sum(error_count)                                                 AS err_count
		FROM observability.spans_1m
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

func metricBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}

func spanBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}
