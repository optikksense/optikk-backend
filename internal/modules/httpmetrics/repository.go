package httpmetrics

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository interface {
	GetRequestRate(teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error)
	GetRequestDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetActiveRequests(teamID int64, startMs, endMs int64) ([]TimeBucket, error)
	GetRequestBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetResponseBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetClientDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetDNSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetTLSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	// Span-based route analysis
	GetTopRoutesByVolume(teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetTopRoutesByLatency(teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetRouteErrorRate(teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetRouteErrorTimeseries(teamID int64, startMs, endMs int64) ([]RouteTimeseriesPoint, error)
	// Span-based status / error analysis
	GetStatusDistribution(teamID int64, startMs, endMs int64) ([]StatusGroupBucket, error)
	GetErrorTimeseries(teamID int64, startMs, endMs int64) ([]ErrorTimeseriesPoint, error)
	// External / outbound host analysis (CLIENT spans)
	GetTopExternalHosts(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
	GetExternalHostLatency(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
	GetExternalHostErrorRate(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) queryHistogramSummary(teamID int64, startMs, endMs int64, metricName string) (HistogramSummary, error) {
	query := fmt.Sprintf(`
		SELECT
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) AS p95,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) AS p99,
		    avg(hist_sum / nullIf(hist_count, 0))                                     AS avg_val
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
	`,
		TableMetrics, ColTeamID, ColTimestamp,
		ColMetricName, metricName,
	)
	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return HistogramSummary{}, err
	}
	return HistogramSummary{
		P50: dbutil.Float64FromAny(row["p50"]),
		P95: dbutil.Float64FromAny(row["p95"]),
		P99: dbutil.Float64FromAny(row["p99"]),
		Avg: dbutil.Float64FromAny(row["avg_val"]),
	}, nil
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
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY time_bucket, status_code
		ORDER BY time_bucket, status_code
	`,
		bucket, statusAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricHTTPServerRequestDuration,
	)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]StatusCodeBucket, len(rows))
	for i, row := range rows {
		results[i] = StatusCodeBucket{
			Timestamp:  dbutil.StringFromAny(row["time_bucket"]),
			StatusCode: dbutil.StringFromAny(row["status_code"]),
			Count:      dbutil.Int64FromAny(row["req_count"]),
		}
	}
	return results, nil
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
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricHTTPServerActiveRequests,
	)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]TimeBucket, len(rows))
	for i, row := range rows {
		v := dbutil.NullableFloat64FromAny(row["val"])
		results[i] = TimeBucket{
			Timestamp: dbutil.StringFromAny(row["time_bucket"]),
			Value:     v,
		}
	}
	return results, nil
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
		SELECT mat_http_route AS route, count() AS req_count
		FROM %s
		WHERE team_id = ? AND ts_bucket_start BETWEEN ? AND ?
		  AND timestamp BETWEEN ? AND ?
		  AND mat_http_route != ''
		GROUP BY route
		ORDER BY req_count DESC
		LIMIT 20
	`, TableSpans)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]RouteMetric, len(rows))
	for i, row := range rows {
		results[i] = RouteMetric{
			Route:    dbutil.StringFromAny(row["route"]),
			ReqCount: dbutil.Int64FromAny(row["req_count"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetTopRoutesByLatency(teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	query := fmt.Sprintf(`
		SELECT mat_http_route AS route,
		       count() AS req_count,
		       quantileExact(0.95)(duration_ns / 1e6) AS p95_ms
		FROM %s
		WHERE team_id = ? AND ts_bucket_start BETWEEN ? AND ?
		  AND timestamp BETWEEN ? AND ?
		  AND mat_http_route != ''
		GROUP BY route
		ORDER BY p95_ms DESC
		LIMIT 20
	`, TableSpans)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]RouteMetric, len(rows))
	for i, row := range rows {
		results[i] = RouteMetric{
			Route:    dbutil.StringFromAny(row["route"]),
			ReqCount: dbutil.Int64FromAny(row["req_count"]),
			P95Ms:    dbutil.Float64FromAny(row["p95_ms"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetRouteErrorRate(teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	query := fmt.Sprintf(`
		SELECT mat_http_route AS route,
		       count() AS req_count,
		       countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400) * 100.0 / count() AS error_pct
		FROM %s
		WHERE team_id = ? AND ts_bucket_start BETWEEN ? AND ?
		  AND timestamp BETWEEN ? AND ?
		  AND mat_http_route != ''
		GROUP BY route
		ORDER BY error_pct DESC
		LIMIT 20
	`, TableSpans)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]RouteMetric, len(rows))
	for i, row := range rows {
		results[i] = RouteMetric{
			Route:    dbutil.StringFromAny(row["route"]),
			ReqCount: dbutil.Int64FromAny(row["req_count"]),
			ErrorPct: dbutil.Float64FromAny(row["error_pct"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetRouteErrorTimeseries(teamID int64, startMs, endMs int64) ([]RouteTimeseriesPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       mat_http_route AS http_route,
		       countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400) AS error_count
		FROM %s
		WHERE team_id = ? AND ts_bucket_start BETWEEN ? AND ?
		  AND timestamp BETWEEN ? AND ?
		  AND mat_http_route != ''
		GROUP BY timestamp, http_route
		ORDER BY timestamp ASC, error_count DESC
	`, bucket, TableSpans)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]RouteTimeseriesPoint, len(rows))
	for i, row := range rows {
		results[i] = RouteTimeseriesPoint{
			Timestamp:  dbutil.StringFromAny(row["timestamp"]),
			HttpRoute:  dbutil.StringFromAny(row["http_route"]),
			ErrorCount: dbutil.Int64FromAny(row["error_count"]),
		}
	}
	return results, nil
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
		count() AS count
		FROM %s
		WHERE team_id = ? AND ts_bucket_start BETWEEN ? AND ?
		  AND timestamp BETWEEN ? AND ?
		  AND response_status_code != ''
		GROUP BY status_group
		ORDER BY status_group ASC
	`, TableSpans)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]StatusGroupBucket, len(rows))
	for i, row := range rows {
		results[i] = StatusGroupBucket{
			StatusGroup: dbutil.StringFromAny(row["status_group"]),
			Count:       dbutil.Int64FromAny(row["count"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetErrorTimeseries(teamID int64, startMs, endMs int64) ([]ErrorTimeseriesPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       count() AS req_count,
		       countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400) AS error_count,
		       countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400) * 100.0 / count() AS error_rate
		FROM %s
		WHERE team_id = ? AND ts_bucket_start BETWEEN ? AND ?
		  AND timestamp BETWEEN ? AND ?
		  AND response_status_code != ''
		GROUP BY timestamp
		ORDER BY timestamp ASC
	`, bucket, TableSpans)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]ErrorTimeseriesPoint, len(rows))
	for i, row := range rows {
		results[i] = ErrorTimeseriesPoint{
			Timestamp:  dbutil.StringFromAny(row["timestamp"]),
			ReqCount:   dbutil.Int64FromAny(row["req_count"]),
			ErrorCount: dbutil.Int64FromAny(row["error_count"]),
			ErrorRate:  dbutil.Float64FromAny(row["error_rate"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetTopExternalHosts(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	query := fmt.Sprintf(`
		SELECT http_host AS host, count() AS req_count
		FROM %s
		WHERE team_id = ? AND ts_bucket_start BETWEEN ? AND ?
		  AND timestamp BETWEEN ? AND ?
		  AND span_kind = 3
		  AND http_host != ''
		GROUP BY host
		ORDER BY req_count DESC
		LIMIT 20
	`, TableSpans)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]ExternalHostMetric, len(rows))
	for i, row := range rows {
		results[i] = ExternalHostMetric{
			Host:     dbutil.StringFromAny(row["host"]),
			ReqCount: dbutil.Int64FromAny(row["req_count"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetExternalHostLatency(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	query := fmt.Sprintf(`
		SELECT http_host AS host,
		       count() AS req_count,
		       quantileExact(0.95)(duration_ns / 1e6) AS p95_ms
		FROM %s
		WHERE team_id = ? AND ts_bucket_start BETWEEN ? AND ?
		  AND timestamp BETWEEN ? AND ?
		  AND span_kind = 3
		  AND http_host != ''
		GROUP BY host
		ORDER BY p95_ms DESC
		LIMIT 20
	`, TableSpans)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]ExternalHostMetric, len(rows))
	for i, row := range rows {
		results[i] = ExternalHostMetric{
			Host:     dbutil.StringFromAny(row["host"]),
			ReqCount: dbutil.Int64FromAny(row["req_count"]),
			P95Ms:    dbutil.Float64FromAny(row["p95_ms"]),
		}
	}
	return results, nil
}

func (r *ClickHouseRepository) GetExternalHostErrorRate(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	query := fmt.Sprintf(`
		SELECT http_host AS host,
		       count() AS req_count,
		       countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400) * 100.0 / count() AS error_pct
		FROM %s
		WHERE team_id = ? AND ts_bucket_start BETWEEN ? AND ?
		  AND timestamp BETWEEN ? AND ?
		  AND span_kind = 3
		  AND http_host != ''
		GROUP BY host
		ORDER BY error_pct DESC
		LIMIT 20
	`, TableSpans)
	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}
	results := make([]ExternalHostMetric, len(rows))
	for i, row := range rows {
		results[i] = ExternalHostMetric{
			Host:     dbutil.StringFromAny(row["host"]),
			ReqCount: dbutil.Int64FromAny(row["req_count"]),
			ErrorPct: dbutil.Float64FromAny(row["error_pct"]),
		}
	}
	return results, nil
}
