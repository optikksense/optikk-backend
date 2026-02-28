package logs

import (
	"context"
	"fmt"
	"strings"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// GetLogSurrounding returns a log entry and its surrounding context logs.
func (r *ClickHouseRepository) GetLogSurrounding(ctx context.Context, teamUUID string, logID int64, before, after int) (Log, []Log, []Log, error) {
	anchorRow, err := dbutil.QueryMap(r.db,
		fmt.Sprintf(`SELECT %s FROM logs WHERE team_id = ? AND id = ? LIMIT 1`, logCols),
		teamUUID, logID)
	if err != nil || len(anchorRow) == 0 {
		return Log{}, nil, nil, nil
	}
	anchor := mapRowToLog(anchorRow)
	svc := anchor.ServiceName
	// Use timestamp bounds to let ClickHouse prune partitions instead of scanning all data.
	tsLow := anchor.Timestamp.Add(-1 * time.Hour)
	tsHigh := anchor.Timestamp.Add(1 * time.Hour)

	beforeRows, _ := dbutil.QueryMaps(r.db,
		fmt.Sprintf(`SELECT %s FROM logs WHERE team_id = ? AND service_name = ? AND timestamp BETWEEN ? AND ? AND id < ? ORDER BY id DESC LIMIT ?`, logCols),
		teamUUID, svc, tsLow, tsHigh, logID, before)
	// Reverse before rows for chronological order.
	for i, j := 0, len(beforeRows)-1; i < j; i, j = i+1, j-1 {
		beforeRows[i], beforeRows[j] = beforeRows[j], beforeRows[i]
	}

	afterRows, _ := dbutil.QueryMaps(r.db,
		fmt.Sprintf(`SELECT %s FROM logs WHERE team_id = ? AND service_name = ? AND timestamp BETWEEN ? AND ? AND id > ? ORDER BY id ASC LIMIT ?`, logCols),
		teamUUID, svc, tsLow, tsHigh, logID, after)

	beforeLogs := mapRowsToLogs(beforeRows)
	afterLogs := mapRowsToLogs(afterRows)

	return anchor, beforeLogs, afterLogs, nil
}

// GetLogDetail returns a single log entry and its service context.
func (r *ClickHouseRepository) GetLogDetail(ctx context.Context, teamUUID, traceID, spanID string, center, from, to time.Time) (Log, []Log, error) {
	logRow, err := dbutil.QueryMap(r.db, fmt.Sprintf(`
		SELECT %s FROM logs
		WHERE team_id = ? AND trace_id = ? AND span_id = ?
		  AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp DESC LIMIT 1
	`, logCols), teamUUID, traceID, spanID,
		center.Add(-1*time.Second), center.Add(1*time.Second))
	if err != nil || len(logRow) == 0 {
		return Log{}, nil, err
	}

	log := mapRowToLog(logRow)
	serviceName := log.ServiceName

	contextLogs := []Log{}
	if serviceName != "" {
		rows, _ := dbutil.QueryMaps(r.db, fmt.Sprintf(`
			SELECT %s FROM logs
			WHERE team_id = ? AND service_name = ? AND timestamp BETWEEN ? AND ?
			ORDER BY timestamp ASC LIMIT 100
		`, logCols), teamUUID, serviceName, from, to)
		contextLogs = mapRowsToLogs(rows)
	}

	return log, contextLogs, nil
}

// GetTraceLogs returns logs for a trace, with speculative fallback.
func (r *ClickHouseRepository) GetTraceLogs(ctx context.Context, teamUUID, traceID string) (TraceLogsResponse, error) {
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s FROM logs
		WHERE team_id = ? AND trace_id = ?
		ORDER BY timestamp ASC LIMIT 500
	`, logCols), teamUUID, traceID)
	if err != nil {
		return TraceLogsResponse{}, err
	}
	logs := mapRowsToLogs(rows)
	if len(logs) > 0 {
		return TraceLogsResponse{Logs: logs, IsSpeculative: false}, nil
	}

	// Fallback: find related logs by service + HTTP context from span metadata.
	// These are speculative matches — we do NOT inject fake trace_ids.
	traceMeta, metaErr := dbutil.QueryMap(r.db, `
		SELECT min(start_time) as trace_start,
		       max(end_time) as trace_end,
		       any(service_name) as service_name,
		       any(http_method) as http_method,
		       any(http_url) as http_url,
		       any(operation_name) as operation_name
		FROM spans
		WHERE team_id = ? AND trace_id = ?
	`, teamUUID, traceID)
	if metaErr != nil || len(traceMeta) == 0 {
		return TraceLogsResponse{Logs: []Log{}}, nil
	}

	traceStart := dbutil.TimeFromAny(traceMeta["trace_start"])
	traceEnd := dbutil.TimeFromAny(traceMeta["trace_end"])
	serviceName := dbutil.StringFromAny(traceMeta["service_name"])
	httpMethod := strings.ToUpper(strings.TrimSpace(dbutil.StringFromAny(traceMeta["http_method"])))
	route := normalizeRoute(dbutil.StringFromAny(traceMeta["http_url"]))
	if route == "" {
		route = normalizeRoute(dbutil.StringFromAny(traceMeta["operation_name"]))
	}
	if traceStart.IsZero() || traceEnd.IsZero() || serviceName == "" {
		return TraceLogsResponse{Logs: []Log{}}, nil
	}

	routeLike := "%"
	if route != "" {
		routeLike = "%" + route + "%"
	}
	fallbackRows, fallbackErr := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s FROM logs
		WHERE team_id = ? AND service_name = ?
		  AND timestamp BETWEEN ? AND ?
		  AND (? = '' OR upper(JSONExtractString(attributes, 'http.method')) = ?)
		  AND (? = '' OR JSONExtractString(attributes, 'http.route') = ? OR message LIKE ?)
		ORDER BY timestamp ASC LIMIT 500
	`, logCols),
		teamUUID,
		serviceName,
		traceStart.Add(-2*time.Second),
		traceEnd.Add(2*time.Second),
		httpMethod, httpMethod,
		route, route, routeLike,
	)
	if fallbackErr != nil {
		return TraceLogsResponse{Logs: []Log{}}, nil
	}

	logs = mapRowsToLogs(fallbackRows)
	return TraceLogsResponse{Logs: logs, IsSpeculative: true}, nil
}
