package errors

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

func errorBucketExpr(startMs, endMs int64) string {
	return timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
}

type Repository interface {
	GetServiceErrorRate(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
	GetErrorVolume(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
	GetLatencyDuringErrorWindows(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
	GetErrorGroups(teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error)
	GetErrorGroupDetail(teamID int64, startMs, endMs int64, groupID string) (*ErrorGroupDetail, error)
	GetErrorGroupTraces(teamID int64, startMs, endMs int64, groupID string, limit int) ([]ErrorGroupTrace, error)
	GetErrorGroupTimeseries(teamID int64, startMs, endMs int64, groupID string) ([]TimeSeriesPoint, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetErrorGroups(teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error) {
	query := `
		SELECT s.service_name AS service_name,
		       s.name         AS operation_name,
		       s.status_message,
		       s.response_status_code AS http_status_code,
		       COUNT(*) as error_count,
		       MAX(s.timestamp) as last_occurrence,
		       MIN(s.timestamp) as first_occurrence,
		       (groupArray(s.trace_id) as trace_ids)[1] as sample_trace_id
		FROM observability.spans s
		WHERE s.team_id = ? AND (` + ErrorCondition() + `) AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`
	args := []any{teamID, timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY s.service_name, s.name, s.status_message, s.response_status_code
	           ORDER BY error_count DESC LIMIT ?`
	args = append(args, limit)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	groups := make([]ErrorGroup, len(rows))
	for i, row := range rows {
		svc := dbutil.StringFromAny(row["service_name"])
		op := dbutil.StringFromAny(row["operation_name"])
		msg := dbutil.StringFromAny(row["status_message"])
		code := int(dbutil.Int64FromAny(row["http_status_code"]))
		groups[i] = ErrorGroup{
			GroupID:         ErrorGroupID(svc, op, msg, code),
			ServiceName:     svc,
			OperationName:   op,
			StatusMessage:   msg,
			HTTPStatusCode:  code,
			ErrorCount:      dbutil.Int64FromAny(row["error_count"]),
			LastOccurrence:  dbutil.TimeFromAny(row["last_occurrence"]),
			FirstOccurrence: dbutil.TimeFromAny(row["first_occurrence"]),
			SampleTraceID:   dbutil.StringFromAny(row["sample_trace_id"]),
		}
	}
	return groups, nil
}

// resolveGroupID finds the error group matching the given groupID hash.
func (r *ClickHouseRepository) resolveGroupID(teamID int64, startMs, endMs int64, groupID string) (service, operation, statusMessage string, httpCode int, err error) {
	groups, err := r.GetErrorGroups(teamID, startMs, endMs, "", 500)
	if err != nil {
		return "", "", "", 0, err
	}
	for _, g := range groups {
		if g.GroupID == groupID {
			return g.ServiceName, g.OperationName, g.StatusMessage, g.HTTPStatusCode, nil
		}
	}
	return "", "", "", 0, fmt.Errorf("error group %s not found", groupID)
}

// errorGroupCondition returns a WHERE fragment matching a specific error group.
func errorGroupCondition() string {
	return `s.service_name = ? AND s.name = ? AND s.status_message = ? AND s.response_status_code = ?`
}

func (r *ClickHouseRepository) GetErrorGroupDetail(teamID int64, startMs, endMs int64, groupID string) (*ErrorGroupDetail, error) {
	svc, op, msg, code, err := r.resolveGroupID(teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT s.service_name, s.name AS operation_name, s.status_message,
		       s.response_status_code AS http_status_code,
		       COUNT(*) AS error_count,
		       MAX(s.timestamp) AS last_occurrence,
		       MIN(s.timestamp) AS first_occurrence,
		       (groupArray(s.trace_id))[1] AS sample_trace_id,
		       any(s.exception_type) AS exception_type,
		       any(s.exception_stacktrace) AS stack_trace
		FROM observability.spans s
		WHERE s.team_id = ? AND (` + ErrorCondition() + `)
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN ? AND ?
		  AND ` + errorGroupCondition() + `
		GROUP BY s.service_name, s.name, s.status_message, s.response_status_code`

	args := []any{
		teamID,
		timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000),
		dbutil.SqlTime(startMs), dbutil.SqlTime(endMs),
		svc, op, msg, fmt.Sprintf("%d", code),
	}

	row, err := dbutil.QueryMap(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	return &ErrorGroupDetail{
		GroupID:         groupID,
		ServiceName:     dbutil.StringFromAny(row["service_name"]),
		OperationName:   dbutil.StringFromAny(row["operation_name"]),
		StatusMessage:   dbutil.StringFromAny(row["status_message"]),
		HTTPStatusCode:  int(dbutil.Int64FromAny(row["http_status_code"])),
		ErrorCount:      dbutil.Int64FromAny(row["error_count"]),
		LastOccurrence:  dbutil.TimeFromAny(row["last_occurrence"]),
		FirstOccurrence: dbutil.TimeFromAny(row["first_occurrence"]),
		SampleTraceID:   dbutil.StringFromAny(row["sample_trace_id"]),
		ExceptionType:   dbutil.StringFromAny(row["exception_type"]),
		StackTrace:      dbutil.StringFromAny(row["stack_trace"]),
	}, nil
}

func (r *ClickHouseRepository) GetErrorGroupTraces(teamID int64, startMs, endMs int64, groupID string, limit int) ([]ErrorGroupTrace, error) {
	svc, op, msg, code, err := r.resolveGroupID(teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT s.trace_id, s.span_id, s.timestamp,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       s.status_code_string AS status_code
		FROM observability.spans s
		WHERE s.team_id = ? AND (` + ErrorCondition() + `)
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN ? AND ?
		  AND ` + errorGroupCondition() + `
		ORDER BY s.timestamp DESC
		LIMIT ?`

	args := []any{
		teamID,
		timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000),
		dbutil.SqlTime(startMs), dbutil.SqlTime(endMs),
		svc, op, msg, fmt.Sprintf("%d", code),
		limit,
	}

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	traces := make([]ErrorGroupTrace, len(rows))
	for i, row := range rows {
		traces[i] = ErrorGroupTrace{
			TraceID:    dbutil.StringFromAny(row["trace_id"]),
			SpanID:     dbutil.StringFromAny(row["span_id"]),
			Timestamp:  dbutil.TimeFromAny(row["timestamp"]),
			DurationMs: dbutil.Float64FromAny(row["duration_ms"]),
			StatusCode: dbutil.StringFromAny(row["status_code"]),
		}
	}
	return traces, nil
}

func (r *ClickHouseRepository) GetErrorGroupTimeseries(teamID int64, startMs, endMs int64, groupID string) ([]TimeSeriesPoint, error) {
	svc, op, msg, code, err := r.resolveGroupID(teamID, startMs, endMs, groupID)
	if err != nil {
		return nil, err
	}

	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       COUNT(*) AS error_count
		FROM observability.spans s
		WHERE s.team_id = ? AND (`+ErrorCondition()+`)
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN ? AND ?
		  AND `+errorGroupCondition()+`
		GROUP BY timestamp
		ORDER BY timestamp ASC`, bucket)

	args := []any{
		teamID,
		timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000),
		dbutil.SqlTime(startMs), dbutil.SqlTime(endMs),
		svc, op, msg, fmt.Sprintf("%d", code),
	}

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	points := make([]TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = TimeSeriesPoint{
			Timestamp:  dbutil.TimeFromAny(row["timestamp"]),
			ErrorCount: dbutil.Int64FromAny(row["error_count"]),
		}
	}
	return points, nil
}

func (r *ClickHouseRepository) GetServiceErrorRate(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name,
		       timestamp,
		       request_count,
		       error_count,
		       if(request_count > 0, error_count*100.0/request_count, 0) AS error_rate,
		       avg_latency
		FROM (
			SELECT s.service_name AS service_name,
			       %s AS timestamp,
			       count()                          AS request_count,
			       countIf(`+ErrorCondition()+`)    AS error_count,
			       avg(s.duration_nano / 1000000.0) AS avg_latency
			FROM observability.spans s
			WHERE s.team_id = ? AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`, bucket)
	args := []any{teamID, timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY s.service_name, %s
		)
		ORDER BY timestamp ASC
		LIMIT 10000`, bucket)

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

func (r *ClickHouseRepository) GetErrorVolume(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name, timestamp, error_count
		FROM (
			SELECT s.service_name AS service_name,
			       %s AS timestamp,
			       countIf(`+ErrorCondition()+`) AS error_count
			FROM observability.spans s
			WHERE s.team_id = ? AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`, bucket)
	args := []any{teamID, timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY s.service_name, %s
		)
		WHERE error_count > 0
		ORDER BY timestamp ASC
		LIMIT 10000`, bucket)

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

func (r *ClickHouseRepository) GetLatencyDuringErrorWindows(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	bucket := errorBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT service_name, timestamp, request_count, error_count, avg_latency
		FROM (
			SELECT s.service_name AS service_name,
			       %s AS timestamp,
			       count()                          AS request_count,
			       countIf(`+ErrorCondition()+`)    AS error_count,
			       avg(s.duration_nano / 1000000.0) AS avg_latency
			FROM observability.spans s
			WHERE s.team_id = ? AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`, bucket)
	args := []any{teamID, timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += fmt.Sprintf(` GROUP BY s.service_name, %s
		)
		WHERE error_count > 0
		ORDER BY timestamp ASC
		LIMIT 10000`, bucket)

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
