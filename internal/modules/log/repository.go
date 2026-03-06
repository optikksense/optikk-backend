package logs

import (
	"context"
	"fmt"
	"strings"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// logCols is the SELECT column list for raw log queries.
const logCols = `id, timestamp, observed_timestamp, severity_text, severity_number,
	body, trace_id, span_id, trace_flags,
	service, host, pod, container, environment,
	attributes_string, attributes_number, attributes_bool,
	scope_name, scope_version`

// ClickHouseRepository is the data access layer for logs.
type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// ── Helpers ──────────────────────────────────────────────────────────────────

// buildLogWhere builds a WHERE clause from LogFilters.
func buildLogWhere(f LogFilters) (string, []any) {
	const maxTimeRangeMs = 30 * 24 * 60 * 60 * 1000
	startMs := f.StartMs
	endMs := f.EndMs
	if endMs <= 0 {
		endMs = time.Now().UnixMilli()
	}
	if startMs <= 0 || (endMs-startMs) > maxTimeRangeMs {
		startMs = endMs - maxTimeRangeMs
	}

	// Convert ms to nanoseconds for UInt64 timestamp column.
	startNs := uint64(startMs) * 1_000_000
	endNs := uint64(endMs) * 1_000_000

	// ts_bucket_start bounds for partition pruning (seconds, truncated to day).
	startBucket := uint32(startMs / 1000)
	endBucket := uint32(endMs / 1000)

	where := ` team_id = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ?`
	args := []any{f.TeamUUID, startBucket, endBucket, startNs, endNs}

	if len(f.Severities) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Severities)
		where += ` AND severity_text IN ` + in
		args = append(args, vals...)
	}
	if len(f.Services) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Services)
		where += ` AND service IN ` + in
		args = append(args, vals...)
	}
	if len(f.Hosts) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Hosts)
		where += ` AND host IN ` + in
		args = append(args, vals...)
	}
	if len(f.Pods) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Pods)
		where += ` AND pod IN ` + in
		args = append(args, vals...)
	}
	if len(f.Containers) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Containers)
		where += ` AND container IN ` + in
		args = append(args, vals...)
	}
	if len(f.Environments) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Environments)
		where += ` AND environment IN ` + in
		args = append(args, vals...)
	}
	if len(f.ExcludeSeverities) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.ExcludeSeverities)
		where += ` AND severity_text NOT IN ` + in
		args = append(args, vals...)
	}
	if len(f.ExcludeServices) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.ExcludeServices)
		where += ` AND service NOT IN ` + in
		args = append(args, vals...)
	}
	if len(f.ExcludeHosts) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.ExcludeHosts)
		where += ` AND host NOT IN ` + in
		args = append(args, vals...)
	}
	if f.TraceID != "" {
		where += ` AND trace_id = ?`
		args = append(args, f.TraceID)
	}
	if f.SpanID != "" {
		where += ` AND span_id = ?`
		args = append(args, f.SpanID)
	}
	if f.Search != "" {
		where += ` AND body LIKE ?`
		args = append(args, "%"+f.Search+"%")
	}
	return where, args
}

// mapRowToLog converts a generic map row into a typed Log.
func mapRowToLog(row map[string]any) Log {
	return Log{
		ID:                dbutil.StringFromAny(row["id"]),
		Timestamp:         uint64(dbutil.Int64FromAny(row["timestamp"])),
		ObservedTimestamp: uint64(dbutil.Int64FromAny(row["observed_timestamp"])),
		SeverityText:      dbutil.StringFromAny(row["severity_text"]),
		SeverityNumber:    uint8(dbutil.Int64FromAny(row["severity_number"])),
		Body:              dbutil.StringFromAny(row["body"]),
		TraceID:           dbutil.StringFromAny(row["trace_id"]),
		SpanID:            dbutil.StringFromAny(row["span_id"]),
		TraceFlags:        uint32(dbutil.Int64FromAny(row["trace_flags"])),
		ServiceName:       dbutil.StringFromAny(row["service"]),
		Host:              dbutil.StringFromAny(row["host"]),
		Pod:               dbutil.StringFromAny(row["pod"]),
		Container:         dbutil.StringFromAny(row["container"]),
		Environment:       dbutil.StringFromAny(row["environment"]),
		AttributesString:  mapStringFromAny(row["attributes_string"]),
		AttributesNumber:  mapFloat64FromAny(row["attributes_number"]),
		AttributesBool:    mapBoolFromAny(row["attributes_bool"]),
		ScopeName:         dbutil.StringFromAny(row["scope_name"]),
		ScopeVersion:      dbutil.StringFromAny(row["scope_version"]),
	}
}

func mapStringFromAny(v any) map[string]string {
	if m, ok := v.(map[string]string); ok {
		return m
	}
	return nil
}

func mapFloat64FromAny(v any) map[string]float64 {
	if m, ok := v.(map[string]float64); ok {
		return m
	}
	return nil
}

func mapBoolFromAny(v any) map[string]bool {
	if m, ok := v.(map[string]bool); ok {
		return m
	}
	return nil
}

func mapRowsToLogs(rows []map[string]any) []Log {
	logs := make([]Log, 0, len(rows))
	for _, row := range rows {
		logs = append(logs, mapRowToLog(row))
	}
	return logs
}

func mapRowsToFacets(rows []map[string]any) []Facet {
	facets := make([]Facet, 0, len(rows))
	for _, row := range rows {
		facets = append(facets, Facet{
			Value: dbutil.StringFromAny(row["value"]),
			Count: dbutil.Int64FromAny(row["count"]),
		})
	}
	return facets
}

// logBucketExpr returns a ClickHouse SQL expression for time bucketing on
// the UInt64 nanosecond timestamp column.
func logBucketExpr(startMs, endMs int64) string {
	tsExpr := "toDateTime(intDiv(timestamp, 1000000000))"
	return timebucket.ExprForColumn(startMs, endMs, tsExpr)
}

func normalizeRoute(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	parts := strings.Fields(s)
	if len(parts) >= 2 && strings.HasPrefix(parts[1], "/") {
		return parts[1]
	}
	if strings.HasPrefix(s, "/") {
		return s
	}
	return s
}

// ── Search ───────────────────────────────────────────────────────────────────

// GetLogs returns paginated log entries.
func (r *ClickHouseRepository) GetLogs(ctx context.Context, f LogFilters, limit int, direction string, cursor LogCursor) ([]Log, int64, error) {
	where, args := buildLogWhere(f)
	orderDir := "DESC"
	if direction == "asc" {
		orderDir = "ASC"
	}

	orderBy := fmt.Sprintf(`timestamp %s, id %s`, orderDir, orderDir)

	offset := 0
	if cursor.Offset > 0 {
		offset = cursor.Offset
	}

	if offset == 0 && cursor.ID != "" {
		if direction == "desc" {
			if cursor.HasTimestamp() {
				where += ` AND (timestamp < ? OR (timestamp = ? AND id < ?))`
				args = append(args, cursor.Timestamp, cursor.Timestamp, cursor.ID)
			} else {
				where += ` AND id < ?`
				args = append(args, cursor.ID)
			}
		} else {
			if cursor.HasTimestamp() {
				where += ` AND (timestamp > ? OR (timestamp = ? AND id > ?))`
				args = append(args, cursor.Timestamp, cursor.Timestamp, cursor.ID)
			} else {
				where += ` AND id > ?`
				args = append(args, cursor.ID)
			}
		}
	}

	query := fmt.Sprintf(`SELECT %s FROM logs WHERE%s ORDER BY %s LIMIT ?`, logCols, where, orderBy)
	queryArgs := append(args, limit)
	if offset > 0 {
		query += ` OFFSET ?`
		queryArgs = append(queryArgs, offset)
	}

	rows, err := dbutil.QueryMaps(r.db, query, queryArgs...)
	if err != nil {
		return nil, 0, err
	}

	logs := mapRowsToLogs(rows)
	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM logs WHERE`+where, args...)

	return logs, total, nil
}

// ── Aggregation ──────────────────────────────────────────────────────────────

// GetLogHistogram returns time-bucketed log counts by severity.
func (r *ClickHouseRepository) GetLogHistogram(ctx context.Context, f LogFilters, step string) ([]LogHistogramBucket, error) {
	bucketExpr := logBucketExpr(f.StartMs, f.EndMs)
	if step != "" {
		// If an explicit step is provided, use the timebucket.ByName strategy.
		s := timebucket.ByName(step)
		tsExpr := "toDateTime(intDiv(timestamp, 1000000000))"
		bucketExpr = fmt.Sprintf("formatDateTime(%s(%s), '%%Y-%%m-%%d %%H:%%i:00')",
			bucketFuncFromStrategy(s), tsExpr)
	}
	where, args := buildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, severity_text as severity, COUNT(*) as count
		FROM logs WHERE%s
		GROUP BY %s, severity_text
		ORDER BY time_bucket ASC`, bucketExpr, where, bucketExpr)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	buckets := make([]LogHistogramBucket, 0, len(rows))
	for _, row := range rows {
		buckets = append(buckets, LogHistogramBucket{
			TimeBucket: dbutil.StringFromAny(row["time_bucket"]),
			Severity:   dbutil.StringFromAny(row["severity"]),
			Count:      dbutil.Int64FromAny(row["count"]),
		})
	}
	return buckets, nil
}

// GetLogVolume returns time-bucketed log counts with severity breakdown.
func (r *ClickHouseRepository) GetLogVolume(ctx context.Context, f LogFilters, step string) ([]LogVolumeBucket, error) {
	bucketExpr := logBucketExpr(f.StartMs, f.EndMs)
	where, args := buildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket,
		       COUNT(*) as total,
		       sum(if(severity_text='ERROR', 1, 0)) as errors,
		       sum(if(severity_text='WARN', 1, 0)) as warnings,
		       sum(if(severity_text='INFO', 1, 0)) as infos,
		       sum(if(severity_text='DEBUG', 1, 0)) as debugs,
		       sum(if(severity_text='FATAL', 1, 0)) as fatals
		FROM logs WHERE%s
		GROUP BY %s
		ORDER BY time_bucket ASC`, bucketExpr, where, bucketExpr)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	buckets := make([]LogVolumeBucket, 0, len(rows))
	for _, row := range rows {
		buckets = append(buckets, LogVolumeBucket{
			TimeBucket: dbutil.StringFromAny(row["time_bucket"]),
			Total:      dbutil.Int64FromAny(row["total"]),
			Errors:     dbutil.Int64FromAny(row["errors"]),
			Warnings:   dbutil.Int64FromAny(row["warnings"]),
			Infos:      dbutil.Int64FromAny(row["infos"]),
			Debugs:     dbutil.Int64FromAny(row["debugs"]),
			Fatals:     dbutil.Int64FromAny(row["fatals"]),
		})
	}
	return buckets, nil
}

// bucketFuncFromStrategy extracts the ClickHouse function name from a strategy's expression.
// Falls back to toStartOfMinute if parsing fails.
func bucketFuncFromStrategy(s timebucket.Strategy) string {
	expr := s.GetBucketExpression()
	// Expression format: formatDateTime(toStartOfXxx(timestamp), ...)
	start := strings.Index(expr, "(") + 1
	end := strings.Index(expr[start:], "(")
	if start > 0 && end > 0 {
		return expr[start : start+end]
	}
	return "toStartOfMinute"
}

// ── Context ──────────────────────────────────────────────────────────────────

// GetLogSurrounding returns a log entry and its surrounding context logs.
func (r *ClickHouseRepository) GetLogSurrounding(ctx context.Context, teamUUID, logID string, before, after int) (Log, []Log, []Log, error) {
	anchorRow, err := dbutil.QueryMap(r.db,
		fmt.Sprintf(`SELECT %s FROM logs WHERE team_id = ? AND id = ? LIMIT 1`, logCols),
		teamUUID, logID)
	if err != nil || len(anchorRow) == 0 {
		return Log{}, nil, nil, nil
	}
	anchor := mapRowToLog(anchorRow)
	svc := anchor.ServiceName
	anchorTs := anchor.Timestamp

	// Compute nanosecond bounds (±1 hour).
	const oneHourNs = uint64(3_600_000_000_000)
	tsLow := anchorTs - oneHourNs
	tsHigh := anchorTs + oneHourNs

	// Compute ts_bucket_start bounds for partition pruning.
	bucketLow := uint32(tsLow / 1_000_000_000 / 86400 * 86400)
	bucketHigh := uint32(tsHigh / 1_000_000_000 / 86400 * 86400)

	beforeRows, _ := dbutil.QueryMaps(r.db,
		fmt.Sprintf(`SELECT %s FROM logs WHERE team_id = ? AND service = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ? AND (timestamp < ? OR (timestamp = ? AND id < ?)) ORDER BY timestamp DESC, id DESC LIMIT ?`, logCols),
		teamUUID, svc, bucketLow, bucketHigh, tsLow, tsHigh, anchorTs, anchorTs, logID, before)
	for i, j := 0, len(beforeRows)-1; i < j; i, j = i+1, j-1 {
		beforeRows[i], beforeRows[j] = beforeRows[j], beforeRows[i]
	}

	afterRows, _ := dbutil.QueryMaps(r.db,
		fmt.Sprintf(`SELECT %s FROM logs WHERE team_id = ? AND service = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ? AND (timestamp > ? OR (timestamp = ? AND id > ?)) ORDER BY timestamp ASC, id ASC LIMIT ?`, logCols),
		teamUUID, svc, bucketLow, bucketHigh, tsLow, tsHigh, anchorTs, anchorTs, logID, after)

	return anchor, mapRowsToLogs(beforeRows), mapRowsToLogs(afterRows), nil
}

// GetLogDetail returns a single log entry and its service context.
func (r *ClickHouseRepository) GetLogDetail(ctx context.Context, teamUUID, traceID, spanID string, centerNs uint64, fromNs, toNs uint64) (Log, []Log, error) {
	// ±1 second around center for anchor lookup.
	const oneSecNs = uint64(1_000_000_000)
	cLow := centerNs - oneSecNs
	cHigh := centerNs + oneSecNs

	bucketLow := uint32(fromNs / 1_000_000_000 / 86400 * 86400)
	bucketHigh := uint32(toNs / 1_000_000_000 / 86400 * 86400)

	logRow, err := dbutil.QueryMap(r.db, fmt.Sprintf(`
		SELECT %s FROM logs
		WHERE team_id = ? AND trace_id = ? AND span_id = ?
		  AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp DESC LIMIT 1
	`, logCols), teamUUID, traceID, spanID, cLow, cHigh)
	if err != nil || len(logRow) == 0 {
		return Log{}, nil, err
	}

	log := mapRowToLog(logRow)
	serviceName := log.ServiceName

	contextLogs := []Log{}
	if serviceName != "" {
		rows, _ := dbutil.QueryMaps(r.db, fmt.Sprintf(`
			SELECT %s FROM logs
			WHERE team_id = ? AND service = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ?
			ORDER BY timestamp ASC LIMIT 100
		`, logCols), teamUUID, serviceName, bucketLow, bucketHigh, fromNs, toNs)
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

	// Convert trace time bounds to nanoseconds with ±2s padding.
	startNs := uint64(traceStart.Add(-2*time.Second).UnixNano())
	endNs := uint64(traceEnd.Add(2*time.Second).UnixNano())
	bucketLow := uint32(startNs / 1_000_000_000 / 86400 * 86400)
	bucketHigh := uint32(endNs / 1_000_000_000 / 86400 * 86400)

	routeLike := "%"
	if route != "" {
		routeLike = "%" + route + "%"
	}
	fallbackRows, fallbackErr := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s FROM logs
		WHERE team_id = ? AND service = ?
		  AND ts_bucket_start BETWEEN ? AND ?
		  AND timestamp BETWEEN ? AND ?
		  AND (? = '' OR upper(attributes_string['http.method']) = ?)
		  AND (? = '' OR attributes_string['http.route'] = ? OR body LIKE ?)
		ORDER BY timestamp ASC LIMIT 500
	`, logCols),
		teamUUID,
		serviceName,
		bucketLow,
		bucketHigh,
		startNs,
		endNs,
		httpMethod, httpMethod,
		route, route, routeLike,
	)
	if fallbackErr != nil {
		return TraceLogsResponse{Logs: []Log{}}, nil
	}

	logs = mapRowsToLogs(fallbackRows)
	return TraceLogsResponse{Logs: logs, IsSpeculative: true}, nil
}

// ── Facets ───────────────────────────────────────────────────────────────────

// GetLogFacets returns total count and facet breakdowns for severity, service, host.
func (r *ClickHouseRepository) GetLogFacets(ctx context.Context, f LogFilters) (LogFacetsResponse, error) {
	where, args := buildLogWhere(f)
	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM logs WHERE`+where, args...)

	sevRows, sevErr := dbutil.QueryMaps(r.db, `SELECT severity_text as value, COUNT(*) as count FROM logs WHERE`+where+` GROUP BY severity_text ORDER BY count DESC`, args...)
	serviceRows, svcErr := dbutil.QueryMaps(r.db, `SELECT service as value, COUNT(*) as count FROM logs WHERE`+where+` GROUP BY service ORDER BY count DESC LIMIT 50`, args...)
	hostRows, hostErr := dbutil.QueryMaps(r.db, `SELECT host as value, COUNT(*) as count FROM logs WHERE`+where+` AND host != '' GROUP BY host ORDER BY count DESC LIMIT 50`, args...)

	if sevErr != nil || svcErr != nil || hostErr != nil {
		fmt.Printf("logs: facet query errors: severity=%v service=%v host=%v\n", sevErr, svcErr, hostErr)
	}

	return LogFacetsResponse{
		Total: total,
		Facets: map[string][]Facet{
			"severities": mapRowsToFacets(sevRows),
			"services":   mapRowsToFacets(serviceRows),
			"hosts":      mapRowsToFacets(hostRows),
		},
	}, nil
}

// GetLogStats returns total count and extended facet breakdowns.
func (r *ClickHouseRepository) GetLogStats(ctx context.Context, f LogFilters) (LogStats, error) {
	where, args := buildLogWhere(f)
	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM logs WHERE`+where, args...)

	sevRows, sevErr := dbutil.QueryMaps(r.db, `SELECT severity_text as value, COUNT(*) as count FROM logs WHERE`+where+` GROUP BY severity_text ORDER BY count DESC`, args...)
	serviceRows, svcErr := dbutil.QueryMaps(r.db, `SELECT service as value, COUNT(*) as count FROM logs WHERE`+where+` GROUP BY service ORDER BY count DESC LIMIT 50`, args...)
	hostRows, hostErr := dbutil.QueryMaps(r.db, `SELECT host as value, COUNT(*) as count FROM logs WHERE`+where+` AND host != '' GROUP BY host ORDER BY count DESC LIMIT 50`, args...)
	podRows, podErr := dbutil.QueryMaps(r.db, `SELECT pod as value, COUNT(*) as count FROM logs WHERE`+where+` AND pod != '' GROUP BY pod ORDER BY count DESC LIMIT 50`, args...)
	scopeRows, scopeErr := dbutil.QueryMaps(r.db, `SELECT scope_name as value, COUNT(*) as count FROM logs WHERE`+where+` AND scope_name != '' GROUP BY scope_name ORDER BY count DESC LIMIT 50`, args...)

	if sevErr != nil || svcErr != nil || hostErr != nil || podErr != nil || scopeErr != nil {
		fmt.Printf("logs: stats facet query errors: severity=%v service=%v host=%v pod=%v scope=%v\n",
			sevErr, svcErr, hostErr, podErr, scopeErr)
	}

	return LogStats{
		Total: total,
		Fields: map[string][]Facet{
			"severity_text": mapRowsToFacets(sevRows),
			"service":       mapRowsToFacets(serviceRows),
			"host":          mapRowsToFacets(hostRows),
			"pod":           mapRowsToFacets(podRows),
			"scope_name":    mapRowsToFacets(scopeRows),
		},
	}, nil
}

// GetLogFields returns facet values for a single column.
func (r *ClickHouseRepository) GetLogFields(ctx context.Context, f LogFilters, col string) ([]Facet, error) {
	// Allowlist columns to prevent SQL injection.
	allowed := map[string]bool{
		"severity_text": true, "service": true, "host": true,
		"pod": true, "container": true, "scope_name": true, "environment": true,
	}
	if !allowed[col] {
		return nil, fmt.Errorf("invalid column: %s", col)
	}

	where, args := buildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as value, COUNT(*) as count
		FROM logs WHERE%s AND %s != ''
		GROUP BY %s ORDER BY count DESC LIMIT 200`, col, where, col, col)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	return mapRowsToFacets(rows), nil
}
