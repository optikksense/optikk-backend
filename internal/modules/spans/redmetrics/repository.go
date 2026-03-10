package redmetrics

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// Repository defines data access for RED metrics endpoints.
type Repository interface {
	GetTopSlowOperations(teamID int64, startMs, endMs int64, limit int) ([]SlowOperation, error)
	GetTopErrorOperations(teamID int64, startMs, endMs int64, limit int) ([]ErrorOperation, error)
	GetHTTPStatusDistribution(teamID int64, startMs, endMs int64) ([]HTTPStatusBucket, []HTTPStatusTimePoint, error)
	GetServiceScorecard(teamID int64, startMs, endMs int64) ([]ServiceScorecard, error)
	GetApdex(teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]ApdexScore, error)
}

// ClickHouseRepository implements Repository against ClickHouse.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new RED metrics repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// GetTopSlowOperations returns operations ranked by p99 latency descending.
func (r *ClickHouseRepository) GetTopSlowOperations(teamID int64, startMs, endMs int64, limit int) ([]SlowOperation, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.name                                          AS operation_name,
		       s.service_name                                   AS service_name,
		       quantile(0.50)(s.duration_nano / 1000000.0)    AS p50_ms,
		       quantile(0.95)(s.duration_nano / 1000000.0)    AS p95_ms,
		       quantile(0.99)(s.duration_nano / 1000000.0)    AS p99_ms,
		       count()                                        AS span_count
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
		GROUP BY s.name, s.service_name
		ORDER BY p99_ms DESC
		LIMIT ?
	`, teamID, uint64(startMs/1000), uint64(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), limit)
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

// GetTopErrorOperations returns operations ranked by error rate descending.
func (r *ClickHouseRepository) GetTopErrorOperations(teamID int64, startMs, endMs int64, limit int) ([]ErrorOperation, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.name                                                       AS operation_name,
		       s.service_name                                                AS service_name,
		       s.mat_exception_type                                          AS exception_type,
		       count()                                                       AS total_count,
		       countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) AS error_count,
		       if(count() > 0,
		           countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) * 100.0 / count(),
		           0) AS error_rate
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
		GROUP BY s.name, s.service_name, exception_type
		ORDER BY error_rate DESC
		LIMIT ?
	`, teamID, uint64(startMs/1000), uint64(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), limit)
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

// GetHTTPStatusDistribution returns span counts per HTTP status code (aggregate + time-series).
func (r *ClickHouseRepository) GetHTTPStatusDistribution(teamID int64, startMs, endMs int64) ([]HTTPStatusBucket, []HTTPStatusTimePoint, error) {
	// Aggregate buckets
	aggRows, err := dbutil.QueryMaps(r.db, `
		SELECT toInt64(response_status_code) AS status_code,
		       count()                        AS span_count
		FROM observability.spans
		WHERE team_id = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ?
		  AND response_status_code != ''
		GROUP BY status_code
		ORDER BY status_code ASC
	`, teamID, uint64(startMs/1000), uint64(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, nil, err
	}

	buckets := make([]HTTPStatusBucket, 0, len(aggRows))
	for _, row := range aggRows {
		buckets = append(buckets, HTTPStatusBucket{
			StatusCode: dbutil.Int64FromAny(row["status_code"]),
			SpanCount:  dbutil.Int64FromAny(row["span_count"]),
		})
	}

	// Time-series
	bucket := timebucket.ExprForColumn(startMs, endMs, "timestamp")
	tsQuery := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       toInt64(response_status_code) AS status_code,
		       count()                        AS span_count
		FROM observability.spans
		WHERE team_id = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ?
		  AND response_status_code != ''
		GROUP BY time_bucket, status_code
		ORDER BY time_bucket ASC, status_code ASC
	`, bucket)

	tsRows, err := dbutil.QueryMaps(r.db, tsQuery, teamID, uint64(startMs/1000), uint64(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return buckets, nil, err
	}

	tsPoints := make([]HTTPStatusTimePoint, 0, len(tsRows))
	for _, row := range tsRows {
		tsPoints = append(tsPoints, HTTPStatusTimePoint{
			Timestamp:  dbutil.TimeFromAny(row["time_bucket"]),
			StatusCode: dbutil.Int64FromAny(row["status_code"]),
			SpanCount:  dbutil.Int64FromAny(row["span_count"]),
		})
	}
	return buckets, tsPoints, nil
}

// GetServiceScorecard returns per-service {rps, error_pct, p95_ms} over the time window.
func (r *ClickHouseRepository) GetServiceScorecard(teamID int64, startMs, endMs int64) ([]ServiceScorecard, error) {
	durationSec := float64(endMs-startMs) / 1000.0
	if durationSec <= 0 {
		durationSec = 1
	}

	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.service_name AS service_name,
		       count()                                                         AS total_count,
		       countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) AS error_count,
		       quantile(0.95)(s.duration_nano / 1000000.0)                    AS p95_ms
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.parent_span_id = '' AND s.timestamp BETWEEN ? AND ?
		GROUP BY s.service_name
		ORDER BY total_count DESC
		LIMIT 1000
	`, teamID, uint64(startMs/1000), uint64(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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

// GetApdex returns per-service Apdex scores using the provided thresholds.
// satisfiedMs: max latency for "satisfied" user (T).
// toleratingMs: max latency for "tolerating" user (typically 4T).
func (r *ClickHouseRepository) GetApdex(teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]ApdexScore, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.service_name AS service_name,
		       countIf(s.duration_nano / 1000000.0 <= ?)   AS satisfied,
		       countIf(s.duration_nano / 1000000.0 > ? AND s.duration_nano / 1000000.0 <= ?) AS tolerating,
		       countIf(s.duration_nano / 1000000.0 > ?)    AS frustrated,
		       count()                                      AS total_count
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.parent_span_id = '' AND s.timestamp BETWEEN ? AND ?
		GROUP BY s.service_name
		ORDER BY total_count DESC
	`, satisfiedMs, satisfiedMs, toleratingMs, toleratingMs,
		teamID, uint64(startMs/1000), uint64(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
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
