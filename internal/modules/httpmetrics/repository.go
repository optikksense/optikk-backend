package httpmetrics

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	database "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository interface {
	GetRequestRate(teamID int64, startMs, endMs int64) ([]statusCodeBucketDTO, error)
	GetRequestDuration(teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetActiveRequests(teamID int64, startMs, endMs int64) ([]timeBucketDTO, error)
	GetRequestBodySize(teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetResponseBodySize(teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetClientDuration(teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetDNSDuration(teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	GetTLSDuration(teamID int64, startMs, endMs int64) (histogramSummaryDTO, error)
	// Span-based route analysis
	GetTopRoutesByVolume(teamID int64, startMs, endMs int64) ([]routeMetricDTO, error)
	GetTopRoutesByLatency(teamID int64, startMs, endMs int64) ([]routeMetricDTO, error)
	GetRouteErrorRate(teamID int64, startMs, endMs int64) ([]routeMetricDTO, error)
	GetRouteErrorTimeseries(teamID int64, startMs, endMs int64) ([]routeTimeseriesPointDTO, error)
	// Span-based status / error analysis
	GetStatusDistribution(teamID int64, startMs, endMs int64) ([]statusGroupBucketDTO, error)
	GetErrorTimeseries(teamID int64, startMs, endMs int64) ([]errorTimeseriesPointDTO, error)
	// External / outbound host analysis (CLIENT spans)
	GetTopExternalHosts(teamID int64, startMs, endMs int64) ([]externalHostMetricDTO, error)
	GetExternalHostLatency(teamID int64, startMs, endMs int64) ([]externalHostMetricDTO, error)
	GetExternalHostErrorRate(teamID int64, startMs, endMs int64) ([]externalHostMetricDTO, error)
}

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) Repository {
	return &ClickHouseRepository{db: db}
}

func baseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// spanParams returns named params for span table queries (uses both ts_bucket_start and timestamp ranges).
func spanParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
	}
}

func (r *ClickHouseRepository) queryHistogramSummary(teamID int64, startMs, endMs int64, metricName string) (HistogramSummary, error) {
	query := fmt.Sprintf(`
		SELECT
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99,
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
	if err := r.db.QueryRow(context.Background(), &row, query, baseParams(teamID, startMs, endMs)...); err != nil {
		return HistogramSummary{}, err
	}
	return row, nil
}

func (r *ClickHouseRepository) GetRequestRate(teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error) {
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
	if err := r.db.Select(context.Background(), &rows, query, baseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRequestDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamID, startMs, endMs, MetricHTTPServerRequestDuration)
}

func (r *ClickHouseRepository) GetActiveRequests(teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
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
	if err := r.db.Select(context.Background(), &rows, query, baseParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRequestBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamID, startMs, endMs, MetricHTTPServerRequestBodySize)
}

func (r *ClickHouseRepository) GetResponseBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamID, startMs, endMs, MetricHTTPServerResponseBodySize)
}

func (r *ClickHouseRepository) GetClientDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamID, startMs, endMs, MetricHTTPClientRequestDuration)
}

func (r *ClickHouseRepository) GetDNSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamID, startMs, endMs, MetricDNSLookupDuration)
}

func (r *ClickHouseRepository) GetTLSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return r.queryHistogramSummary(teamID, startMs, endMs, MetricTLSConnectDuration)
}

func (r *ClickHouseRepository) GetTopRoutesByVolume(teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
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
	if err := r.db.Select(context.Background(), &rows, query, spanParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetTopRoutesByLatency(teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	query := fmt.Sprintf(`
		SELECT mat_http_route AS route,
		       toInt64(count()) AS req_count,
		       quantileExact(0.95)(duration_nano / 1000000.0) AS p95_ms
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND mat_http_route != ''
		GROUP BY route
		ORDER BY p95_ms DESC
		LIMIT 20
	`, TableSpans)
	var rows []RouteMetric
	if err := r.db.Select(context.Background(), &rows, query, spanParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRouteErrorRate(teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
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
	if err := r.db.Select(context.Background(), &rows, query, spanParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRouteErrorTimeseries(teamID int64, startMs, endMs int64) ([]RouteTimeseriesPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       mat_http_route AS http_route,
		       toInt64(countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400)) AS error_count
		FROM %s
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND mat_http_route != ''
		GROUP BY time_bucket, http_route
		ORDER BY time_bucket ASC, error_count DESC
	`, bucket, TableSpans)
	var rows []RouteTimeseriesPoint
	if err := r.db.Select(context.Background(), &rows, query, spanParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetStatusDistribution(teamID int64, startMs, endMs int64) ([]StatusGroupBucket, error) {
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
	if err := r.db.Select(context.Background(), &rows, query, spanParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetErrorTimeseries(teamID int64, startMs, endMs int64) ([]ErrorTimeseriesPoint, error) {
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
	if err := r.db.Select(context.Background(), &rows, query, spanParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetTopExternalHosts(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
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
	if err := r.db.Select(context.Background(), &rows, query, spanParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetExternalHostLatency(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	query := fmt.Sprintf(`
		SELECT http_host AS host,
		       toInt64(count()) AS req_count,
		       quantileExact(0.95)(duration_nano / 1000000.0) AS p95_ms
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
	if err := r.db.Select(context.Background(), &rows, query, spanParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetExternalHostErrorRate(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
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
	if err := r.db.Select(context.Background(), &rows, query, spanParams(teamID, startMs, endMs)...); err != nil {
		return nil, err
	}
	return rows, nil
}
