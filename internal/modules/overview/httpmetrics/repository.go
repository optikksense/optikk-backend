package httpmetrics

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

type Repository interface {
	GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]statusCodeBucketRow, error)
	GetRequestDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetActiveRequests(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketRow, error)
	GetRequestBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetResponseBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetClientDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetDNSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetTLSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetTopRoutesByVolume(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeVolumeRow, error)
	GetTopRoutesByLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeVolumeRow, error)
	GetRouteErrorPivot(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeStatusPivotRow, error)
	GetRouteErrorTimeseriesPivot(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeTimeseriesPivotRow, error)
	GetStatusCodeDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]statusCodeCountRow, error)
	GetErrorTimeseriesPivot(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorTimeseriesPivotRow, error)
	GetTopExternalHosts(ctx context.Context, teamID int64, startMs, endMs int64) ([]hostVolumeRow, error)
	GetExternalHostLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]hostVolumeRow, error)
	GetExternalHostErrorPivot(ctx context.Context, teamID int64, startMs, endMs int64) ([]hostStatusPivotRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

// histogramSummaryRow carries sum/count totals so the service can compute avg
// in Go; percentiles are filled from the matching sketch kind in service.go.
type histogramSummaryRow struct {
	HistSum   float64 `ch:"hist_sum"`
	HistCount uint64  `ch:"hist_count"`
}

// timeBucketRow carries sum/count per time bucket so the service can build
// the avg per-bucket in Go.
type timeBucketRow struct {
	Timestamp string  `ch:"time_bucket"`
	ValSum    float64 `ch:"val_sum"`
	ValCount  uint64  `ch:"val_count"`
}

// statusCodeBucketRow is the raw per-(time_bucket, status_code) count for the
// request-rate timeseries. StatusCode stays a string so no cast happens in SQL.
type statusCodeBucketRow struct {
	Timestamp  string `ch:"time_bucket"`
	StatusCode string `ch:"status_code"`
	ReqCount   uint64 `ch:"req_count"`
}

// routeVolumeRow is the raw per-route volume list used by both "top by volume"
// and "top by latency" (latency p95 is merged from the sketch in Go).
type routeVolumeRow struct {
	Route    string `ch:"route"`
	ReqCount uint64 `ch:"req_count"`
}

// hostVolumeRow is the raw per-host volume list for external hosts.
type hostVolumeRow struct {
	Host     string `ch:"host"`
	ReqCount uint64 `ch:"req_count"`
}

// routeStatusPivotRow is the raw (route, has_error, response_status_code)
// pivot that the service reduces in Go for route error-rate views.
type routeStatusPivotRow struct {
	Route        string `ch:"route"`
	HasError     bool   `ch:"has_error"`
	StatusCode   string `ch:"response_status_code"`
	SampleCount  uint64 `ch:"sample_count"`
}

// hostStatusPivotRow mirrors routeStatusPivotRow but for external hosts.
type hostStatusPivotRow struct {
	Host         string `ch:"host"`
	HasError     bool   `ch:"has_error"`
	StatusCode   string `ch:"response_status_code"`
	SampleCount  uint64 `ch:"sample_count"`
}

// routeTimeseriesPivotRow is the same pivot as routeStatusPivotRow with an
// extra time bucket column for the timeseries variant.
type routeTimeseriesPivotRow struct {
	Timestamp   string `ch:"time_bucket"`
	Route       string `ch:"route"`
	HasError    bool   `ch:"has_error"`
	StatusCode  string `ch:"response_status_code"`
	SampleCount uint64 `ch:"sample_count"`
}

// errorTimeseriesPivotRow is the per-(time_bucket, has_error, status) pivot
// used to compute the global error timeseries in Go.
type errorTimeseriesPivotRow struct {
	Timestamp   string `ch:"time_bucket"`
	HasError    bool   `ch:"has_error"`
	StatusCode  string `ch:"response_status_code"`
	SampleCount uint64 `ch:"sample_count"`
}

// statusCodeCountRow is the raw (response_status_code, count) list used to
// bucket 2xx/3xx/4xx/5xx/other in Go for the status-distribution view.
type statusCodeCountRow struct {
	StatusCode string `ch:"response_status_code"`
	Count      uint64 `ch:"count"`
}

func (r *ClickHouseRepository) queryHistogramSummary(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) (histogramSummaryRow, error) {
	query := fmt.Sprintf(`
		SELECT
		    sum(hist_sum)   AS hist_sum,
		    sum(hist_count) AS hist_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
	`,
		TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, metricName,
	)
	var row histogramSummaryRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...).ScanStruct(&row); err != nil {
		return histogramSummaryRow{}, err
	}
	return row, nil
}

func (r *ClickHouseRepository) GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]statusCodeBucketRow, error) {
	bucket := timebucket.Expression(startMs, endMs)
	statusAttr := attrString(AttrHTTPStatusCode)

	query := fmt.Sprintf(`
		SELECT
		    %s        AS time_bucket,
		    %s        AS status_code,
		    count()   AS req_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		GROUP BY time_bucket, status_code
		ORDER BY time_bucket, status_code
	`,
		bucket, statusAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricHTTPServerRequestDuration,
	)
	var rows []statusCodeBucketRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRequestDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricHTTPServerRequestDuration)
}

func (r *ClickHouseRepository) GetActiveRequests(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketRow, error) {
	bucket := timebucket.Expression(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s         AS time_bucket,
		    sum(value) AS val_sum,
		    count()    AS val_count
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricHTTPServerActiveRequests,
	)
	var rows []timeBucketRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRequestBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricHTTPServerRequestBodySize)
}

func (r *ClickHouseRepository) GetResponseBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricHTTPServerResponseBodySize)
}

func (r *ClickHouseRepository) GetClientDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricHTTPClientRequestDuration)
}

func (r *ClickHouseRepository) GetDNSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricDNSLookupDuration)
}

func (r *ClickHouseRepository) GetTLSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricTLSConnectDuration)
}

func (r *ClickHouseRepository) GetTopRoutesByVolume(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeVolumeRow, error) {
	query := fmt.Sprintf(`
		SELECT mat_http_route AS route,
		       count()        AS req_count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND mat_http_route != ''
		GROUP BY route
		ORDER BY req_count DESC
		LIMIT 20
	`, TableSpans)
	var rows []routeVolumeRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetTopRoutesByLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeVolumeRow, error) {
	// p95_ms comes from sketch.SpanLatencyEndpoint merged over the endpoint
	// segment of the dim; SQL just fetches the route list with its volume so
	// rows order is still predictable when the sketch isn't warm.
	query := fmt.Sprintf(`
		SELECT mat_http_route AS route,
		       count()        AS req_count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND mat_http_route != ''
		GROUP BY route
		ORDER BY req_count DESC
		LIMIT 20
	`, TableSpans)
	var rows []routeVolumeRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

// GetRouteErrorPivot returns a (route, has_error, response_status_code) pivot.
// The service reduces this to per-route {req_count, error_count, error_pct}
// in Go so the SQL stays free of -If combinators and status-code casts.
func (r *ClickHouseRepository) GetRouteErrorPivot(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeStatusPivotRow, error) {
	query := fmt.Sprintf(`
		SELECT mat_http_route       AS route,
		       has_error            AS has_error,
		       response_status_code AS response_status_code,
		       count()              AS sample_count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND mat_http_route != ''
		GROUP BY route, has_error, response_status_code
	`, TableSpans)
	var rows []routeStatusPivotRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

// GetRouteErrorTimeseriesPivot is the timeseries variant of GetRouteErrorPivot.
// Aggregation per (time_bucket, route) happens in Go.
func (r *ClickHouseRepository) GetRouteErrorTimeseriesPivot(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeTimeseriesPivotRow, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s                   AS time_bucket,
		       mat_http_route       AS route,
		       has_error            AS has_error,
		       response_status_code AS response_status_code,
		       count()              AS sample_count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND mat_http_route != ''
		GROUP BY time_bucket, route, has_error, response_status_code
	`, bucket, TableSpans)
	var rows []routeTimeseriesPivotRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

// GetStatusCodeDistribution returns raw (response_status_code, count) rows;
// the service groups them into 2xx/3xx/4xx/5xx/other in Go.
func (r *ClickHouseRepository) GetStatusCodeDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]statusCodeCountRow, error) {
	query := fmt.Sprintf(`
		SELECT response_status_code AS response_status_code,
		       count()              AS count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND response_status_code != ''
		GROUP BY response_status_code
	`, TableSpans)
	var rows []statusCodeCountRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

// GetErrorTimeseriesPivot returns a (time_bucket, has_error,
// response_status_code) pivot for the global error timeseries.
func (r *ClickHouseRepository) GetErrorTimeseriesPivot(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorTimeseriesPivotRow, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s                   AS time_bucket,
		       has_error            AS has_error,
		       response_status_code AS response_status_code,
		       count()              AS sample_count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND response_status_code != ''
		GROUP BY time_bucket, has_error, response_status_code
	`, bucket, TableSpans)
	var rows []errorTimeseriesPivotRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetTopExternalHosts(ctx context.Context, teamID int64, startMs, endMs int64) ([]hostVolumeRow, error) {
	query := fmt.Sprintf(`
		SELECT http_host AS host,
		       count()   AS req_count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND kind = 3
		  AND http_host != ''
		GROUP BY host
		ORDER BY req_count DESC
		LIMIT 20
	`, TableSpans)
	var rows []hostVolumeRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetExternalHostLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]hostVolumeRow, error) {
	// p95_ms lands from sketch.HttpClientDuration merged by host-target segment;
	// SQL returns just the host list with volume, sorted by that volume so the
	// result stays deterministic when the sketch has no samples for a host.
	query := fmt.Sprintf(`
		SELECT http_host AS host,
		       count()   AS req_count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND kind = 3
		  AND http_host != ''
		GROUP BY host
		ORDER BY req_count DESC
		LIMIT 20
	`, TableSpans)
	var rows []hostVolumeRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

// GetExternalHostErrorPivot is the per-host analogue of GetRouteErrorPivot.
func (r *ClickHouseRepository) GetExternalHostErrorPivot(ctx context.Context, teamID int64, startMs, endMs int64) ([]hostStatusPivotRow, error) {
	query := fmt.Sprintf(`
		SELECT http_host            AS host,
		       has_error            AS has_error,
		       response_status_code AS response_status_code,
		       count()              AS sample_count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND kind = 3
		  AND http_host != ''
		GROUP BY host, has_error, response_status_code
	`, TableSpans)
	var rows []hostStatusPivotRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}
