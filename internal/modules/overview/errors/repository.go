package errors

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// errorBucketExpr returns a ClickHouse expression for adaptive time bucketing
// using OpenTelemetry conventions.
func errorBucketExpr(startMs, endMs int64) string {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return IntervalOneMinute
	case hours <= 24:
		return IntervalFiveMinutes
	case hours <= 168:
		return IntervalSixtyMinutes
	default:
		return IntervalOneDay
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

// GetErrorGroups reads raw spans and groups errors by service, operation, and status.
func (r *ClickHouseRepository) GetErrorGroups(teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error) {
	query := `
		SELECT ` + ColServiceName + `, ` + ColOperationName + `, ` + ColStatusMessage + `, ` + ColHTTPStatusCode + `,
		       COUNT(*) as error_count,
		       MAX(` + ColStartTime + `) as last_occurrence,
		       MIN(` + ColStartTime + `) as first_occurrence,
		       (groupArray(` + ColTraceID + `) as trace_ids)[1] as sample_trace_id
		FROM spans
		WHERE ` + ColTeamID + ` = ? AND (` + ErrorCondition() + `) AND ` + ColStartTime + ` BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND ` + ColServiceName + ` = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY ` + ColServiceName + `, ` + ColOperationName + `, ` + ColStatusMessage + `, ` + ColHTTPStatusCode + `
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

// GetServiceErrorRate reads from spans with adaptive time bucketing.
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
			SELECT `+ColServiceName+`,
			       %s AS timestamp,
			       count()                   AS request_count,
			       countIf(`+ErrorCondition()+`) AS error_count,
			       avg(`+ColDurationMs+`)          AS avg_latency
			FROM spans
			WHERE `+ColTeamID+` = ? AND `+RootSpanCondition()+` AND `+ColStartTime+` BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND ` + ColServiceName + ` = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY `+ColServiceName+`, %s
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

// GetErrorVolume reads from spans with adaptive time bucketing, filtering minutes with errors.
func (r *ClickHouseRepository) GetErrorVolume(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name, timestamp, error_count
		FROM (
			SELECT `+ColServiceName+`,
			       %s AS timestamp,
			       countIf(`+ErrorCondition()+`) AS error_count
			FROM spans
			WHERE `+ColTeamID+` = ? AND `+RootSpanCondition()+` AND `+ColStartTime+` BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND ` + ColServiceName + ` = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY `+ColServiceName+`, %s
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

// GetLatencyDuringErrorWindows reads from spans with adaptive time bucketing, filtering minutes with errors.
func (r *ClickHouseRepository) GetLatencyDuringErrorWindows(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name, timestamp, request_count, error_count, avg_latency
		FROM (
			SELECT `+ColServiceName+`,
			       %s AS timestamp,
			       count()                   AS request_count,
			       countIf(`+ErrorCondition()+`) AS error_count,
			       avg(`+ColDurationMs+`)          AS avg_latency
			FROM spans
			WHERE `+ColTeamID+` = ? AND `+RootSpanCondition()+` AND `+ColStartTime+` BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND ` + ColServiceName + ` = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY `+ColServiceName+`, %s
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
