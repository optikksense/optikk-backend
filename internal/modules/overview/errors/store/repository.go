package store

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/overview/errors/model"
)

// Repository encapsulates data access logic for overview error dashboards.
type Repository interface {
	GetServiceErrorRate(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error)
	GetErrorVolume(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error)
	GetLatencyDuringErrorWindows(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error)
	GetErrorGroups(teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]model.ErrorGroup, error)
}

// ClickHouseRepository encapsulates overview error data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new overview error repository.
func NewRepository(db dbutil.Querier) Repository {
	return &ClickHouseRepository{db: db}
}

// GetErrorGroups still reads raw spans — needs groupArray(trace_id) for sample trace IDs.
func (r *ClickHouseRepository) GetErrorGroups(teamUUID string, startMs, endMs int64, serviceName string, limit int) ([]model.ErrorGroup, error) {
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

	groups := make([]model.ErrorGroup, len(rows))
	for i, row := range rows {
		groups[i] = model.ErrorGroup{
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

// GetServiceErrorRate reads from spans_service_1m — no raw span scan.
func (r *ClickHouseRepository) GetServiceErrorRate(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error) {
	query := `
		SELECT service_name,
		       minute                    AS timestamp,
		       countMerge(request_count) AS request_count,
		       countMerge(error_count)   AS error_count,
		       if(countMerge(request_count) > 0,
		          countMerge(error_count)*100.0/countMerge(request_count), 0) AS error_rate,
		       avgMerge(avg_state)       AS avg_latency
		FROM observability.spans_service_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY service_name, minute ORDER BY timestamp ASC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]model.TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = model.TimeSeriesPoint{
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

// GetErrorVolume reads from spans_service_1m, filtering minutes with errors.
func (r *ClickHouseRepository) GetErrorVolume(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error) {
	query := `
		SELECT service_name,
		       minute                    AS timestamp,
		       countMerge(error_count)   AS error_count
		FROM observability.spans_service_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY service_name, minute
	           HAVING countMerge(error_count) > 0
	           ORDER BY timestamp ASC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]model.TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = model.TimeSeriesPoint{
			ServiceName: dbutil.StringFromAny(row["service_name"]),
			Timestamp:   dbutil.TimeFromAny(row["timestamp"]),
			ErrorCount:  dbutil.Int64FromAny(row["error_count"]),
		}
	}
	return points, nil
}

// GetLatencyDuringErrorWindows reads from spans_service_1m, filtering minutes with errors.
func (r *ClickHouseRepository) GetLatencyDuringErrorWindows(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSeriesPoint, error) {
	query := `
		SELECT service_name,
		       minute                    AS timestamp,
		       countMerge(request_count) AS request_count,
		       countMerge(error_count)   AS error_count,
		       avgMerge(avg_state)       AS avg_latency
		FROM observability.spans_service_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY service_name, minute
	           HAVING countMerge(error_count) > 0
	           ORDER BY timestamp ASC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]model.TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = model.TimeSeriesPoint{
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			Timestamp:    dbutil.TimeFromAny(row["timestamp"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
		}
	}
	return points, nil
}
