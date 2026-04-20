package httpmetrics

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"

	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

type Repository interface {
	GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]statusCodeBucketDTO, error)
	GetRequestDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetActiveRequests(ctx context.Context, teamID int64, startMs, endMs int64) ([]timeBucketDTO, error)
	GetRequestBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetResponseBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetClientDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetDNSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetTLSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	// Span-based route analysis
	GetTopRoutesByVolume(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeMetricDTO, error)
	GetTopRoutesByLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeMetricDTO, error)
	GetRouteErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeMetricDTO, error)
	GetRouteErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]routeTimeseriesPointDTO, error)
	// Span-based status / error analysis
	GetStatusDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]statusGroupBucketDTO, error)
	GetErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorTimeseriesPointDTO, error)
	// External / outbound host analysis (CLIENT spans)
	GetTopExternalHosts(ctx context.Context, teamID int64, startMs, endMs int64) ([]externalHostMetricDTO, error)
	GetExternalHostLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]externalHostMetricDTO, error)
	GetExternalHostErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]externalHostMetricDTO, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) queryHistogramSummary(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) (HistogramSummary, error) {
	query := fmt.Sprintf(`
		SELECT
		    quantileTDigestWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileTDigestWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileTDigestWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99,
		    avg(hist_sum / nullIf(hist_count, 0))                                     AS avg
		FROM %s
		WHERE %s = @teamID
		  AND %s BETWEEN @start AND @end
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
	`,
		TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, metricName,
	)
	var row HistogramSummary
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...).ScanStruct(&row); err != nil {
		return HistogramSummary{}, err
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

func (r *ClickHouseRepository) GetRequestDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricHTTPServerRequestDuration)
}

func (r *ClickHouseRepository) GetActiveRequests(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	bucket := timebucket.Expression(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s         AS time_bucket,
		    avg(value) AS val
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
	var rows []TimeBucket
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, dbutil.SimpleBaseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRequestBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricHTTPServerRequestBodySize)
}

func (r *ClickHouseRepository) GetResponseBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricHTTPServerResponseBodySize)
}

func (r *ClickHouseRepository) GetClientDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricHTTPClientRequestDuration)
}

func (r *ClickHouseRepository) GetDNSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(ctx, teamID, startMs, endMs, MetricDNSLookupDuration)
}

func (r *ClickHouseRepository) GetTLSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
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
	query := fmt.Sprintf(`
		SELECT mat_http_route AS route,
		       toInt64(count()) AS req_count,
		       quantileTDigest(0.95)(duration_nano / 1000000.0) AS p95_ms
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND mat_http_route != ''
		GROUP BY route
		ORDER BY p95_ms DESC
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
	query := fmt.Sprintf(`
		SELECT http_host AS host,
		       toInt64(count()) AS req_count,
		       quantileTDigest(0.95)(duration_nano / 1000000.0) AS p95_ms
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND kind = 3
		  AND http_host != ''
		GROUP BY host
		ORDER BY p95_ms DESC
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
