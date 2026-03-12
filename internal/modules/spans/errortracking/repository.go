package errortracking

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository interface {
	GetExceptionRateByType(teamID int64, startMs, endMs int64, serviceName string) ([]ExceptionRatePoint, error)
	GetErrorHotspot(teamID int64, startMs, endMs int64) ([]ErrorHotspotCell, error)
	GetHTTP5xxByRoute(teamID int64, startMs, endMs int64, serviceName string) ([]HTTP5xxByRoute, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetExceptionRateByType(teamID int64, startMs, endMs int64, serviceName string) ([]ExceptionRatePoint, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       s.exception_type AS exception_type,
		       count()          AS event_count
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.exception_type != '' AND s.timestamp BETWEEN ? AND ?`, bucket)
	args := []any{teamID, timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY time_bucket, exception_type ORDER BY time_bucket ASC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	result := make([]ExceptionRatePoint, 0, len(rows))
	for _, row := range rows {
		result = append(result, ExceptionRatePoint{
			Timestamp:     dbutil.TimeFromAny(row["time_bucket"]),
			ExceptionType: dbutil.StringFromAny(row["exception_type"]),
			Count:         dbutil.Int64FromAny(row["event_count"]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetErrorHotspot(teamID int64, startMs, endMs int64) ([]ErrorHotspotCell, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.service_name AS service_name,
		       s.name AS operation_name,
		       count()                                                            AS total_count,
		       countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)    AS error_count,
		       if(count() > 0,
		           countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) * 100.0 / count(),
		           0) AS error_rate
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
		GROUP BY s.service_name, s.name
		ORDER BY error_rate DESC
		LIMIT 500
	`, teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	result := make([]ErrorHotspotCell, 0, len(rows))
	for _, row := range rows {
		result = append(result, ErrorHotspotCell{
			ServiceName:   dbutil.StringFromAny(row["service_name"]),
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			ErrorRate:     dbutil.Float64FromAny(row["error_rate"]),
			ErrorCount:    dbutil.Int64FromAny(row["error_count"]),
			TotalCount:    dbutil.Int64FromAny(row["total_count"]),
		})
	}
	return result, nil
}

func (r *ClickHouseRepository) GetHTTP5xxByRoute(teamID int64, startMs, endMs int64, serviceName string) ([]HTTP5xxByRoute, error) {
	query := `
		SELECT s.mat_http_route AS http_route,
		       s.service_name AS service_name,
		       count()                                        AS count_5xx
		FROM observability.spans s
		WHERE s.team_id = ? AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?
		  AND toUInt16OrZero(s.response_status_code) >= 500`
	args := []any{teamID, timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY http_route, s.service_name ORDER BY count_5xx DESC LIMIT 100`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	result := make([]HTTP5xxByRoute, 0, len(rows))
	for _, row := range rows {
		result = append(result, HTTP5xxByRoute{
			HTTPRoute:   dbutil.StringFromAny(row["http_route"]),
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			Count:       dbutil.Int64FromAny(row["count_5xx"]),
		})
	}
	return result, nil
}
