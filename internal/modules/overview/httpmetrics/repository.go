package httpmetrics

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

type Repository interface {
	GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error)
	GetRequestDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetActiveRequests(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketRow, error)
	GetRequestBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetResponseBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetClientDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetDNSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetTLSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryRow, error)
	GetTopRoutesByVolume(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetTopRoutesByLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetRouteErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetRouteErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteTimeseriesPoint, error)
	GetStatusDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusGroupBucket, error)
	GetErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorTimeseriesPoint, error)
	GetTopExternalHosts(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
	GetExternalHostLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
	GetExternalHostErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
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
	HistCount int64   `ch:"hist_count"`
	P50       float64 `ch:"p50"`
	P95       float64 `ch:"p95"`
	P99       float64 `ch:"p99"`
}

// timeBucketRow carries sum/count per time bucket so the service can build
// the avg per-bucket in Go.
type timeBucketRow struct {
	Timestamp string  `ch:"time_bucket"`
	ValSum    float64 `ch:"val_sum"`
	ValCount  int64   `ch:"val_count"`
}

func (r *ClickHouseRepository) queryHistogramSummary(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) (histogramSummaryRow, error) {
	query := fmt.Sprintf(`
		SELECT
		    sum(hist_sum)            AS hist_sum,
		    toInt64(sum(hist_count)) AS hist_count,
		    0 AS p50, 0 AS p95, 0 AS p99
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

func (r *ClickHouseRepository) GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error) {
	bucket := timebucket.Expression(startMs, endMs)
	statusAttr := attrString(AttrHTTPStatusCode)

	query := fmt.Sprintf(`
		SELECT
		    %s                 AS time_bucket,
		    %s                 AS status_code,
		    toInt64(count())   AS req_count
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
	var rows []StatusCodeBucket
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
		    %s               AS time_bucket,
		    sum(value)       AS val_sum,
		    toInt64(count()) AS val_count
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

func (r *ClickHouseRepository) GetTopRoutesByVolume(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	query := fmt.Sprintf(`
		SELECT mat_http_route AS route, toInt64(count()) AS req_count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND mat_http_route != ''
		GROUP BY route
		ORDER BY req_count DESC
		LIMIT 20
	`, TableSpans)
	var rows []RouteMetric
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetTopRoutesByLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	// p95_ms comes from sketch.SpanLatencyEndpoint merged over the endpoint
	// segment of the dim; SQL just fetches the route list with its volume so
	// rows order is still predictable when the sketch isn't warm.
	query := fmt.Sprintf(`
		SELECT mat_http_route AS route,
		       toInt64(count()) AS req_count,
		       0                AS p95_ms
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND mat_http_route != ''
		GROUP BY route
		ORDER BY req_count DESC
		LIMIT 20
	`, TableSpans)
	var rows []RouteMetric
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRouteErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	query := fmt.Sprintf(`
		SELECT mat_http_route AS route,
		       toInt64(count()) AS req_count,
		       countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400) * 100.0 / count() AS error_pct
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND mat_http_route != ''
		GROUP BY route
		ORDER BY error_pct DESC
		LIMIT 20
	`, TableSpans)
	var rows []RouteMetric
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRouteErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteTimeseriesPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       mat_http_route AS http_route,
		       toInt64(count()) AS req_count,
		       toInt64(countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400)) AS error_count,
		       countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400) * 100.0 / count() AS error_rate
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND mat_http_route != ''
		GROUP BY time_bucket, http_route
		ORDER BY time_bucket ASC, error_count DESC
	`, bucket, TableSpans)
	var rows []RouteTimeseriesPoint
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetStatusDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusGroupBucket, error) {
	query := fmt.Sprintf(`
		SELECT multiIf(
			toUInt16OrZero(response_status_code) BETWEEN 200 AND 299, '2xx',
			toUInt16OrZero(response_status_code) BETWEEN 300 AND 399, '3xx',
			toUInt16OrZero(response_status_code) BETWEEN 400 AND 499, '4xx',
			toUInt16OrZero(response_status_code) >= 500, '5xx',
			'other'
		) AS status_group,
		toInt64(count()) AS count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND response_status_code != ''
		GROUP BY status_group
		ORDER BY status_group ASC
	`, TableSpans)
	var rows []StatusGroupBucket
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorTimeseriesPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       toInt64(count()) AS req_count,
		       toInt64(countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400)) AS error_count,
		       countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400) * 100.0 / count() AS error_rate
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND response_status_code != ''
		GROUP BY time_bucket
		ORDER BY time_bucket ASC
	`, bucket, TableSpans)
	var rows []ErrorTimeseriesPoint
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetTopExternalHosts(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	query := fmt.Sprintf(`
		SELECT http_host AS host, toInt64(count()) AS req_count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND kind = 3
		  AND http_host != ''
		GROUP BY host
		ORDER BY req_count DESC
		LIMIT 20
	`, TableSpans)
	var rows []ExternalHostMetric
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetExternalHostLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	// p95_ms lands from sketch.HttpClientDuration merged by host-target segment;
	// SQL returns just the host list with volume, sorted by that volume so the
	// result stays deterministic when the sketch has no samples for a host.
	query := fmt.Sprintf(`
		SELECT http_host AS host,
		       toInt64(count()) AS req_count,
		       0                AS p95_ms
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND kind = 3
		  AND http_host != ''
		GROUP BY host
		ORDER BY req_count DESC
		LIMIT 20
	`, TableSpans)
	var rows []ExternalHostMetric
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetExternalHostErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	query := fmt.Sprintf(`
		SELECT http_host AS host,
		       toInt64(count()) AS req_count,
		       countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400) * 100.0 / count() AS error_pct
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND kind = 3
		  AND http_host != ''
		GROUP BY host
		ORDER BY error_pct DESC
		LIMIT 20
	`, TableSpans)
	var rows []ExternalHostMetric
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SpanBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}
