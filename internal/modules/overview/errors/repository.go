package errors

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// errorBucketExpr returns a ClickHouse expression for adaptive time bucketing.
func errorBucketExpr(startMs, endMs int64) string {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return "toStartOfMinute(start_time)"
	case hours <= 24:
		return "toStartOfInterval(start_time, INTERVAL 5 MINUTE)"
	case hours <= 168:
		return "toStartOfInterval(start_time, INTERVAL 60 MINUTE)"
	default:
		return "toStartOfInterval(start_time, INTERVAL 1440 MINUTE)"
	}
}

// Repository encapsulates data access logic for overview error dashboards.
type Repository interface {
	GetServiceErrorRate(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
	GetErrorVolume(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
	GetLatencyDuringErrorWindows(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
	GetErrorGroups(teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error)
}

// ClickHouseRepository encapsulates overview error data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new overview error repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// GetErrorGroups still reads raw spans — needs groupArray(trace_id) for sample trace IDs.
func (r *ClickHouseRepository) GetErrorGroups(teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error) {
	query := `
		SELECT service_name, operation_name, status_message, http_status_code,
		       COUNT(*) as error_count,
		       MAX(start_time) as last_occurrence,
		       MIN(start_time) as first_occurrence,
		       (groupArray(trace_id) as trace_ids)[1] as sample_trace_id
		FROM spans
		WHERE team_id = ? AND (status = 'ERROR' OR http_status_code >= 400) AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY service_name, operation_name, status_message, http_status_code
	           ORDER BY error_count DESC LIMIT ?`
	args = append(args, limit)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	groups := make([]ErrorGroup, len(rows))
	for i, row := range rows {
		groups[i] = ErrorGroup{
			ServiceName:     dbutil.StringFromAny(row["service_name"]),
			OperationName:   dbutil.StringFromAny(row["operation_name"]),
			StatusMessage:   dbutil.StringFromAny(row["status_message"]),
			HTTPStatusCode:  int(dbutil.Int64FromAny(row["http_status_code"])),
			ErrorCount:      dbutil.Int64FromAny(row["error_count"]),
			LastOccurrence:  dbutil.TimeFromAny(row["last_occurrence"]),
			FirstOccurrence: dbutil.TimeFromAny(row["first_occurrence"]),
			SampleTraceID:   dbutil.StringFromAny(row["sample_trace_id"]),
		}
	}
	return groups, nil
}

// GetServiceErrorRate reads from spans_service_1m with adaptive time bucketing.
func (r *ClickHouseRepository) GetServiceErrorRate(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name,
		       timestamp,
		       request_count,
		       error_count,
		       if(request_count > 0, error_count*100.0/request_count, 0) AS error_rate,
		       avg_latency
		FROM (
			SELECT service_name,
			       %s AS timestamp,
			       count()                   AS request_count,
			       countIf(status = 'ERROR' OR http_status_code >= 400) AS error_count,
			       avg(duration_ms)          AS avg_latency
			FROM spans
			WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY service_name, %s
		)
		ORDER BY timestamp ASC`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = TimeSeriesPoint{
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			Timestamp:    dbutil.TimeFromAny(row["timestamp"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			ErrorRate:    dbutil.Float64FromAny(row["error_rate"]),
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
		}
	}
	return points, nil
}

// GetErrorVolume reads from spans_service_1m with adaptive time bucketing, filtering minutes with errors.
func (r *ClickHouseRepository) GetErrorVolume(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT *
		FROM (
			SELECT service_name,
			       %s AS timestamp,
			       countIf(status = 'ERROR' OR http_status_code >= 400) AS error_count
			FROM spans
			WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY service_name, %s
		)
		WHERE error_count > 0
		ORDER BY timestamp ASC`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = TimeSeriesPoint{
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			Timestamp:   dbutil.TimeFromAny(row["timestamp"]),
			ErrorCount:  dbutil.Int64FromAny(row["error_count"]),
		}
	}
	return points, nil
}

// GetLatencyDuringErrorWindows reads from spans_service_1m with adaptive time bucketing, filtering minutes with errors.
func (r *ClickHouseRepository) GetLatencyDuringErrorWindows(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT *
		FROM (
			SELECT service_name,
			       %s AS timestamp,
			       count()                   AS request_count,
			       countIf(status = 'ERROR' OR http_status_code >= 400) AS error_count,
			       avg(duration_ms)          AS avg_latency
			FROM spans
			WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY service_name, %s
		)
		WHERE error_count > 0
		ORDER BY timestamp ASC`, bucket)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = TimeSeriesPoint{
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			Timestamp:    dbutil.TimeFromAny(row["timestamp"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
		}
	}
	return points, nil
}
