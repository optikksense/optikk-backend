package tracedetail

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

func (r *ClickHouseRepository) GetSpanAttributes(teamID int64, traceID, spanID string) (*SpanAttributes, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.trace_id, s.name AS operation_name, s.service_name,
		       s.attributes_string, s.resource_attributes,
		       s.exception_type, s.exception_message, s.exception_stacktrace,
		       s.db_system, s.db_name, s.db_statement
		FROM observability.spans s
		WHERE s.team_id = ? AND s.trace_id = ? AND s.span_id = ?
		LIMIT 1
	`, uint32(teamID), traceID, spanID)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	row := rows[0]

	attrString, _ := row["attributes_string"].(map[string]string)
	resourceAttrs, _ := row["resource_attributes"].(map[string]string)
	dbStmt := dbutil.StringFromAny(row["db_statement"])

	merged := make(map[string]string, len(attrString)+len(resourceAttrs))
	for k, v := range resourceAttrs {
		merged[k] = v
	}
	for k, v := range attrString {
		merged[k] = v
	}

	return &SpanAttributes{
		SpanID:                dbutil.StringFromAny(row["span_id"]),
		TraceID:               dbutil.StringFromAny(row["trace_id"]),
		OperationName:         dbutil.StringFromAny(row["operation_name"]),
		ServiceName:           dbutil.StringFromAny(row["service_name"]),
		AttributesString:      attrString,
		ResourceAttrs:         resourceAttrs,
		Attributes:            merged,
		ExceptionType:         dbutil.StringFromAny(row["exception_type"]),
		ExceptionMessage:      dbutil.StringFromAny(row["exception_message"]),
		ExceptionStacktrace:   dbutil.StringFromAny(row["exception_stacktrace"]),
		DBSystem:              dbutil.StringFromAny(row["db_system"]),
		DBName:                dbutil.StringFromAny(row["db_name"]),
		DBStatement:           dbStmt,
		DBStatementNormalized: normalizeDBStatement(dbStmt),
	}, nil
}

func (r *ClickHouseRepository) GetRelatedTraces(teamID int64, serviceName, operationName string, startMs, endMs int64, excludeTraceID string, limit int) ([]RelatedTrace, error) {
	startBucket := uint32(startMs / 1000)
	endBucket := uint32(endMs / 1000)
	startNs := uint64(startMs) * 1_000_000
	endNs := uint64(endMs) * 1_000_000

	rows, err := dbutil.QueryMaps(r.db, `
		SELECT s.span_id, s.trace_id, s.name AS operation_name, s.service_name,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       s.status_code_string AS status, s.timestamp AS start_time
		FROM observability.spans s
		WHERE s.team_id = ?
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN ? AND ?
		  AND s.parent_span_id = ''
		  AND s.service_name = ?
		  AND s.name = ?
		  AND s.trace_id != ?
		ORDER BY s.timestamp DESC
		LIMIT ?
	`, uint32(teamID), startBucket, endBucket, startNs, endNs,
		serviceName, operationName, excludeTraceID, limit)
	if err != nil {
		return nil, err
	}

	result := make([]RelatedTrace, 0, len(rows))
	for _, row := range rows {
		result = append(result, RelatedTrace{
			TraceID:       dbutil.StringFromAny(row["trace_id"]),
			SpanID:        dbutil.StringFromAny(row["span_id"]),
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			ServiceName:   dbutil.StringFromAny(row["service_name"]),
			DurationMs:    dbutil.Float64FromAny(row["duration_ms"]),
			Status:        dbutil.StringFromAny(row["status"]),
			StartTime:     dbutil.TimeFromAny(row["start_time"]),
		})
	}
	return result, nil
}
