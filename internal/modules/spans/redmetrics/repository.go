package redmetrics

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository interface {
	GetSummary(teamID int64, startMs, endMs int64) (REDSummary, error)
	GetServiceScorecard(teamID int64, startMs, endMs int64) ([]ServiceScorecard, error)
	GetApdex(teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]ApdexScore, error)
	GetHTTPStatusDistribution(teamID int64, startMs, endMs int64) ([]HTTPStatusBucket, error)
	GetTopSlowOperations(teamID int64, startMs, endMs int64, limit int) ([]SlowOperation, error)
	GetTopErrorOperations(teamID int64, startMs, endMs int64, limit int) ([]ErrorOperation, error)
	GetRequestRateTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error)
	GetErrorRateTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error)
	GetP95LatencyTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error)
	GetSpanKindBreakdown(teamID int64, startMs, endMs int64) ([]SpanKindPoint, error)
	GetErrorsByRoute(teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error)
	GetLatencyBreakdown(teamID int64, startMs, endMs int64) ([]LatencyBreakdown, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// bucketSecs returns the bucket width in seconds matching the adaptive strategy.
func bucketSecs(startMs, endMs int64) float64 {
	h := (endMs - startMs) / 3_600_000
	switch {
	case h <= 3:
		return 60.0
	case h <= 24:
		return 300.0
	case h <= 168:
		return 3600.0
	default:
		return 86400.0
	}
}

func (r *ClickHouseRepository) GetSummary(teamID int64, startMs, endMs int64) (REDSummary, error) {
	durationSec := float64(endMs-startMs) / 1000.0
	if durationSec <= 0 {
		durationSec = 1
	}

	rows, err := dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       count()                                                                          AS total_count,
		       countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400)        AS error_count,
		       quantileExact(0.95)(duration_nano / 1000000.0)                                  AS p95_ms
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.parent_span_id = '' AND s.timestamp BETWEEN ? AND ?
		GROUP BY service_name
	`, teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return REDSummary{}, err
	}

	var totalCount int64
	var totalErrors int64
	var totalP95 float64
	serviceCount := int64(len(rows))
	for _, row := range rows {
		total := dbutil.Int64FromAny(row["total_count"])
		errors := dbutil.Int64FromAny(row["error_count"])
		totalCount += total
		totalErrors += errors
		totalP95 += dbutil.Float64FromAny(row["p95_ms"])
	}

	avgErrorPct := 0.0
	if totalCount > 0 {
		avgErrorPct = float64(totalErrors) * 100.0 / float64(totalCount)
	}
	avgP95 := 0.0
	if serviceCount > 0 {
		avgP95 = totalP95 / float64(serviceCount)
	}
	return REDSummary{
		ServiceCount: serviceCount,
		TotalRPS:     float64(totalCount) / durationSec,
		AvgErrorPct:  avgErrorPct,
		AvgP95Ms:     avgP95,
	}, nil
}

func (r *ClickHouseRepository) GetServiceScorecard(teamID int64, startMs, endMs int64) ([]ServiceScorecard, error) {
	durationSec := float64(endMs-startMs) / 1000.0
	if durationSec <= 0 {
		durationSec = 1
	}

	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.service_name                                                                   AS service_name,
		       count()                                                                          AS total_count,
		       countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)    AS error_count,
		       quantileExact(0.95)(s.duration_nano / 1000000.0)                                AS p95_ms
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.parent_span_id = '' AND s.timestamp BETWEEN ? AND ?
		GROUP BY s.service_name
		ORDER BY total_count DESC
		LIMIT 1000
	`, teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	result := make([]ServiceScorecard, 0, len(rows))
	for _, row := range rows {
		total := dbutil.Int64FromAny(row["total_count"])
		errors := dbutil.Int64FromAny(row["error_count"])
		errorPct := 0.0
		if total > 0 {
			errorPct = float64(errors) * 100.0 / float64(total)
		}
		result = append(result, ServiceScorecard{
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			RPS:         float64(total) / durationSec,
			ErrorPct:    errorPct,
			P95Ms:       dbutil.Float64FromAny(row["p95_ms"]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetApdex(teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]ApdexScore, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.service_name                                                                              AS service_name,
		       countIf(s.duration_nano / 1000000.0 <= ?)                                                  AS satisfied,
		       countIf(s.duration_nano / 1000000.0 > ? AND s.duration_nano / 1000000.0 <= ?)              AS tolerating,
		       countIf(s.duration_nano / 1000000.0 > ?)                                                   AS frustrated,
		       count()                                                                                     AS total_count
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.parent_span_id = '' AND s.timestamp BETWEEN ? AND ?
		GROUP BY s.service_name
		ORDER BY total_count DESC
	`, satisfiedMs, satisfiedMs, toleratingMs, toleratingMs,
		teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	result := make([]ApdexScore, 0, len(rows))
	for _, row := range rows {
		satisfied := dbutil.Int64FromAny(row["satisfied"])
		tolerating := dbutil.Int64FromAny(row["tolerating"])
		total := dbutil.Int64FromAny(row["total_count"])
		apdex := 0.0
		if total > 0 {
			apdex = (float64(satisfied) + float64(tolerating)*0.5) / float64(total)
		}
		result = append(result, ApdexScore{
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			Apdex:       apdex,
			Satisfied:   satisfied,
			Tolerating:  tolerating,
			Frustrated:  dbutil.Int64FromAny(row["frustrated"]),
			TotalCount:  total,
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetHTTPStatusDistribution(teamID int64, startMs, endMs int64) ([]HTTPStatusBucket, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT toInt64(toUInt16OrZero(response_status_code)) AS status_code,
		       count()                                        AS span_count
		FROM observability.spans
		WHERE team_id = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ?
		  AND response_status_code != ''
		GROUP BY status_code
		ORDER BY status_code ASC
	`, teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	result := make([]HTTPStatusBucket, 0, len(rows))
	for _, row := range rows {
		result = append(result, HTTPStatusBucket{
			StatusCode: dbutil.Int64FromAny(row["status_code"]),
			SpanCount:  dbutil.Int64FromAny(row["span_count"]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetTopSlowOperations(teamID int64, startMs, endMs int64, limit int) ([]SlowOperation, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.name                                              AS operation_name,
		       s.service_name                                       AS service_name,
		       quantileExact(0.50)(s.duration_nano / 1000000.0)    AS p50_ms,
		       quantileExact(0.95)(s.duration_nano / 1000000.0)    AS p95_ms,
		       quantileExact(0.99)(s.duration_nano / 1000000.0)    AS p99_ms,
		       count()                                              AS span_count
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
		GROUP BY s.name, s.service_name
		ORDER BY p99_ms DESC
		LIMIT ?
	`, teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), limit)
	if err != nil {
		return nil, err
	}

	result := make([]SlowOperation, 0, len(rows))
	for _, row := range rows {
		result = append(result, SlowOperation{
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			ServiceName:   dbutil.StringFromAny(row["service_name"]),
			P50Ms:         dbutil.Float64FromAny(row["p50_ms"]),
			P95Ms:         dbutil.Float64FromAny(row["p95_ms"]),
			P99Ms:         dbutil.Float64FromAny(row["p99_ms"]),
			SpanCount:     dbutil.Int64FromAny(row["span_count"]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetTopErrorOperations(teamID int64, startMs, endMs int64, limit int) ([]ErrorOperation, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.name                                                                                    AS operation_name,
		       s.service_name                                                                             AS service_name,
		       s.mat_exception_type                                                                       AS exception_type,
		       count()                                                                                    AS total_count,
		       countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)              AS error_count,
		       if(count() > 0,
		           countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) * 100.0 / count(),
		           0)                                                                                     AS error_rate
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
		GROUP BY s.name, s.service_name, exception_type
		ORDER BY error_rate DESC
		LIMIT ?
	`, teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), limit)
	if err != nil {
		return nil, err
	}

	result := make([]ErrorOperation, 0, len(rows))
	for _, row := range rows {
		result = append(result, ErrorOperation{
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			ServiceName:   dbutil.StringFromAny(row["service_name"]),
			ExceptionType: dbutil.StringFromAny(row["exception_type"]),
			ErrorRate:     dbutil.Float64FromAny(row["error_rate"]),
			ErrorCount:    dbutil.Int64FromAny(row["error_count"]),
			TotalCount:    dbutil.Int64FromAny(row["total_count"]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetRequestRateTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error) {
	bs := bucketSecs(startMs, endMs)
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       s.service_name,
		       count() / ? AS rps
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
		  AND s.parent_span_id = ''
		GROUP BY timestamp, s.service_name
		ORDER BY timestamp ASC, s.service_name ASC
	`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, bs, teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	result := make([]ServiceRatePoint, 0, len(rows))
	for _, row := range rows {
		result = append(result, ServiceRatePoint{
			Timestamp:   dbutil.StringFromAny(row["timestamp"]),
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			RPS:         dbutil.Float64FromAny(row["rps"]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetErrorRateTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       s.service_name,
		       if(count() > 0,
		          countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) * 100.0 / count(),
		          0) AS error_pct
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
		  AND s.parent_span_id = ''
		GROUP BY timestamp, s.service_name
		ORDER BY timestamp ASC, s.service_name ASC
	`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	result := make([]ServiceErrorRatePoint, 0, len(rows))
	for _, row := range rows {
		result = append(result, ServiceErrorRatePoint{
			Timestamp:   dbutil.StringFromAny(row["timestamp"]),
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			ErrorPct:    dbutil.Float64FromAny(row["error_pct"]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetP95LatencyTimeSeries(teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       s.service_name,
		       quantileExact(0.95)(s.duration_nano / 1000000.0) AS p95_ms
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
		  AND s.parent_span_id = ''
		GROUP BY timestamp, s.service_name
		ORDER BY timestamp ASC, s.service_name ASC
	`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	result := make([]ServiceLatencyPoint, 0, len(rows))
	for _, row := range rows {
		result = append(result, ServiceLatencyPoint{
			Timestamp:   dbutil.StringFromAny(row["timestamp"]),
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			P95Ms:       dbutil.Float64FromAny(row["p95_ms"]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetSpanKindBreakdown(teamID int64, startMs, endMs int64) ([]SpanKindPoint, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       s.kind_string,
		       count() AS span_count
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
		  AND s.kind_string != ''
		GROUP BY timestamp, s.kind_string
		ORDER BY timestamp ASC, s.kind_string ASC
	`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	result := make([]SpanKindPoint, 0, len(rows))
	for _, row := range rows {
		result = append(result, SpanKindPoint{
			Timestamp:  dbutil.StringFromAny(row["timestamp"]),
			KindString: dbutil.StringFromAny(row["kind_string"]),
			SpanCount:  dbutil.Int64FromAny(row["span_count"]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetErrorsByRoute(teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       s.mat_http_route AS http_route,
		       countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) AS error_count
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
		  AND s.mat_http_route != ''
		GROUP BY timestamp, http_route
		ORDER BY timestamp ASC, error_count DESC
	`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	result := make([]ErrorByRoutePoint, 0, len(rows))
	for _, row := range rows {
		result = append(result, ErrorByRoutePoint{
			Timestamp:  dbutil.StringFromAny(row["timestamp"]),
			HttpRoute:  dbutil.StringFromAny(row["http_route"]),
			ErrorCount: dbutil.Int64FromAny(row["error_count"]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetLatencyBreakdown(teamID int64, startMs, endMs int64) ([]LatencyBreakdown, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.service_name,
		       sum(s.duration_nano) / 1000000.0 AS total_ms,
		       count() AS span_count
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN ? AND ?
		GROUP BY s.service_name
		ORDER BY total_ms DESC
	`, teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	var grandTotal float64
	breakdown := make([]LatencyBreakdown, 0, len(rows))
	for _, row := range rows {
		ms := dbutil.Float64FromAny(row["total_ms"])
		grandTotal += ms
		breakdown = append(breakdown, LatencyBreakdown{
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			TotalMs:     ms,
			SpanCount:   dbutil.Int64FromAny(row["span_count"]),
		})
	}
	if grandTotal > 0 {
		for i := range breakdown {
			breakdown[i].PctOfTotal = breakdown[i].TotalMs * 100.0 / grandTotal
		}
	}
	return breakdown, nil
}
