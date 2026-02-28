package store

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/modules/log/model"
)

// Repository defines the data access layer for logs.
type Repository interface {
	GetLogs(ctx context.Context, f model.LogFilters, limit int, direction string, cursor model.LogCursor) (model.LogQueryResult, error)
	GetLogHistogram(ctx context.Context, f model.LogFilters, step string) ([]model.LogHistogramBucket, error)
	GetLogVolume(ctx context.Context, f model.LogFilters, step string) ([]model.LogVolumeBucket, error)
	GetLogStats(ctx context.Context, f model.LogFilters) (model.LogStatsResult, error)
	GetLogFields(ctx context.Context, f model.LogFilters, col string) ([]model.Facet, error)
	GetLogSurrounding(ctx context.Context, teamUUID string, logID int64, before, after int) (anchor model.Log, beforeRows, afterRows []model.Log, err error)
	GetLogDetail(ctx context.Context, teamUUID, traceID, spanID string, center, from, to time.Time) (log model.Log, contextLogs []model.Log, err error)
	GetTraceLogs(ctx context.Context, teamUUID, traceID string) (model.TraceLogsResponse, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

const logCols = `id, timestamp, level, service_name, logger, message,
	trace_id, span_id, host, pod, container, thread, exception, attributes`

func (r *ClickHouseRepository) GetLogs(ctx context.Context, f model.LogFilters, limit int, direction string, cursor model.LogCursor) (model.LogQueryResult, error) {
	where, args := r.buildLogWhere(f)
	orderDir := "DESC"
	if direction == "asc" {
		orderDir = "ASC"
	}

	orderBy := fmt.Sprintf(
		`timestamp %s, id %s, service_name %s, trace_id %s, span_id %s, message %s`,
		orderDir, orderDir, orderDir, orderDir, orderDir, orderDir,
	)

	offset := 0
	if cursor.Offset > 0 {
		offset = cursor.Offset
	}

	if offset == 0 && cursor.ID > 0 {
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
		return model.LogQueryResult{}, err
	}

	logs := r.mapRowsToLogs(rows)

	// Facets + total reuse the same where clause (no duplicate buildLogWhere call).
	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM logs WHERE`+where, args...)

	levelRows, levelErr := dbutil.QueryMaps(r.db, `SELECT level as value, COUNT(*) as count FROM logs WHERE`+where+` GROUP BY level ORDER BY count DESC`, args...)
	serviceRows, svcErr := dbutil.QueryMaps(r.db, `SELECT service_name as value, COUNT(*) as count FROM logs WHERE`+where+` GROUP BY service_name ORDER BY count DESC LIMIT 50`, args...)
	hostRows, hostErr := dbutil.QueryMaps(r.db, `SELECT host as value, COUNT(*) as count FROM logs WHERE`+where+` AND host != '' GROUP BY host ORDER BY count DESC LIMIT 50`, args...)

	// Log facet errors but don't fail the whole request — partial data is better than none.
	if levelErr != nil || svcErr != nil || hostErr != nil {
		fmt.Printf("logs: facet query errors: level=%v service=%v host=%v\n", levelErr, svcErr, hostErr)
	}

	return model.LogQueryResult{
		Logs:          logs,
		Total:         total,
		LevelFacets:   r.mapRowsToFacets(levelRows),
		ServiceFacets: r.mapRowsToFacets(serviceRows),
		HostFacets:    r.mapRowsToFacets(hostRows),
	}, nil
}

func (r *ClickHouseRepository) GetLogHistogram(ctx context.Context, f model.LogFilters, step string) ([]model.LogHistogramBucket, error) {
	bucketExpr := logBucketExpr(step)
	where, args := r.buildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, level, COUNT(*) as count
		FROM logs WHERE%s
		GROUP BY %s, level
		ORDER BY time_bucket ASC`, bucketExpr, where, bucketExpr)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	buckets := make([]model.LogHistogramBucket, 0, len(rows))
	for _, row := range rows {
		buckets = append(buckets, model.LogHistogramBucket{
			TimeBucket: dbutil.StringFromAny(row["time_bucket"]),
			Level:      dbutil.StringFromAny(row["level"]),
			Count:      dbutil.Int64FromAny(row["count"]),
		})
	}
	return buckets, nil
}

func (r *ClickHouseRepository) GetLogVolume(ctx context.Context, f model.LogFilters, step string) ([]model.LogVolumeBucket, error) {
	bucketExpr := logBucketExpr(step)
	where, args := r.buildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket,
		       COUNT(*) as total,
		       sum(if(level='ERROR', 1, 0)) as errors,
		       sum(if(level='WARN', 1, 0)) as warnings,
		       sum(if(level='INFO', 1, 0)) as infos,
		       sum(if(level='DEBUG', 1, 0)) as debugs,
		       sum(if(level='FATAL', 1, 0)) as fatals
		FROM logs WHERE%s
		GROUP BY %s
		ORDER BY time_bucket ASC`, bucketExpr, where, bucketExpr)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	buckets := make([]model.LogVolumeBucket, 0, len(rows))
	for _, row := range rows {
		buckets = append(buckets, model.LogVolumeBucket{
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

func (r *ClickHouseRepository) GetLogStats(ctx context.Context, f model.LogFilters) (model.LogStatsResult, error) {
	where, args := r.buildLogWhere(f)
	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM logs WHERE`+where, args...)

	levelRows, levelErr := dbutil.QueryMaps(r.db, `SELECT level as value, COUNT(*) as count FROM logs WHERE`+where+` GROUP BY level ORDER BY count DESC`, args...)
	serviceRows, svcErr := dbutil.QueryMaps(r.db, `SELECT service_name as value, COUNT(*) as count FROM logs WHERE`+where+` GROUP BY service_name ORDER BY count DESC LIMIT 50`, args...)
	hostRows, hostErr := dbutil.QueryMaps(r.db, `SELECT host as value, COUNT(*) as count FROM logs WHERE`+where+` AND host != '' GROUP BY host ORDER BY count DESC LIMIT 50`, args...)
	podRows, podErr := dbutil.QueryMaps(r.db, `SELECT pod as value, COUNT(*) as count FROM logs WHERE`+where+` AND pod != '' GROUP BY pod ORDER BY count DESC LIMIT 50`, args...)
	loggerRows, loggerErr := dbutil.QueryMaps(r.db, `SELECT logger as value, COUNT(*) as count FROM logs WHERE`+where+` AND logger != '' GROUP BY logger ORDER BY count DESC LIMIT 50`, args...)

	if levelErr != nil || svcErr != nil || hostErr != nil || podErr != nil || loggerErr != nil {
		fmt.Printf("logs: stats facet query errors: level=%v service=%v host=%v pod=%v logger=%v\n",
			levelErr, svcErr, hostErr, podErr, loggerErr)
	}

	return model.LogStatsResult{
		Total:         total,
		LevelFacets:   r.mapRowsToFacets(levelRows),
		ServiceFacets: r.mapRowsToFacets(serviceRows),
		HostFacets:    r.mapRowsToFacets(hostRows),
		PodFacets:     r.mapRowsToFacets(podRows),
		LoggerFacets:  r.mapRowsToFacets(loggerRows),
	}, nil
}

func (r *ClickHouseRepository) GetLogFields(ctx context.Context, f model.LogFilters, col string) ([]model.Facet, error) {
	where, args := r.buildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as value, COUNT(*) as count
		FROM logs WHERE%s AND %s != ''
		GROUP BY %s ORDER BY count DESC LIMIT 200`, col, where, col, col)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	return r.mapRowsToFacets(rows), nil
}

func (r *ClickHouseRepository) GetLogSurrounding(ctx context.Context, teamUUID string, logID int64, before, after int) (model.Log, []model.Log, []model.Log, error) {
	anchorRow, err := dbutil.QueryMap(r.db,
		fmt.Sprintf(`SELECT %s FROM logs WHERE team_id = ? AND id = ? LIMIT 1`, logCols),
		teamUUID, logID)
	if err != nil || len(anchorRow) == 0 {
		return model.Log{}, nil, nil, nil
	}
	anchor := r.mapRowToLog(anchorRow)
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

	beforeLogs := r.mapRowsToLogs(beforeRows)
	afterLogs := r.mapRowsToLogs(afterRows)

	return anchor, beforeLogs, afterLogs, nil
}

func (r *ClickHouseRepository) GetLogDetail(ctx context.Context, teamUUID, traceID, spanID string, center, from, to time.Time) (model.Log, []model.Log, error) {
	logRow, err := dbutil.QueryMap(r.db, fmt.Sprintf(`
		SELECT %s FROM logs
		WHERE team_id = ? AND trace_id = ? AND span_id = ?
		  AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp DESC LIMIT 1
	`, logCols), teamUUID, traceID, spanID,
		center.Add(-1*time.Second), center.Add(1*time.Second))
	if err != nil || len(logRow) == 0 {
		return model.Log{}, nil, err
	}

	log := r.mapRowToLog(logRow)
	serviceName := log.ServiceName

	contextLogs := []model.Log{}
	if serviceName != "" {
		rows, _ := dbutil.QueryMaps(r.db, fmt.Sprintf(`
			SELECT %s FROM logs
			WHERE team_id = ? AND service_name = ? AND timestamp BETWEEN ? AND ?
			ORDER BY timestamp ASC LIMIT 100
		`, logCols), teamUUID, serviceName, from, to)
		contextLogs = r.mapRowsToLogs(rows)
	}

	return log, contextLogs, nil
}

func (r *ClickHouseRepository) GetTraceLogs(ctx context.Context, teamUUID, traceID string) (model.TraceLogsResponse, error) {
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s FROM logs
		WHERE team_id = ? AND trace_id = ?
		ORDER BY timestamp ASC LIMIT 500
	`, logCols), teamUUID, traceID)
	if err != nil {
		return model.TraceLogsResponse{}, err
	}
	logs := r.mapRowsToLogs(rows)
	if len(logs) > 0 {
		return model.TraceLogsResponse{Logs: logs, IsSpeculative: false}, nil
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
		return model.TraceLogsResponse{Logs: []model.Log{}}, nil
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
		return model.TraceLogsResponse{Logs: []model.Log{}}, nil
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
		return model.TraceLogsResponse{Logs: []model.Log{}}, nil
	}

	logs = r.mapRowsToLogs(fallbackRows)
	return model.TraceLogsResponse{Logs: logs, IsSpeculative: true}, nil
}

// logBucketExpr returns the ClickHouse SQL expression for time bucketing.
// This is owned by the repository layer to prevent raw SQL from leaking across boundaries.
func logBucketExpr(step string) string {
	switch step {
	case "1m":
		return `DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00')`
	case "2m":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL MINUTE(timestamp) MOD 2 MINUTE), '%Y-%m-%d %H:%i:00')`
	case "5m":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL MINUTE(timestamp) MOD 5 MINUTE), '%Y-%m-%d %H:%i:00')`
	case "10m":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL MINUTE(timestamp) MOD 10 MINUTE), '%Y-%m-%d %H:%i:00')`
	case "15m":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL MINUTE(timestamp) MOD 15 MINUTE), '%Y-%m-%d %H:%i:00')`
	case "30m":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL MINUTE(timestamp) MOD 30 MINUTE), '%Y-%m-%d %H:%i:00')`
	case "1h":
		return `DATE_FORMAT(timestamp, '%Y-%m-%d %H:00:00')`
	case "2h":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL HOUR(timestamp) MOD 2 HOUR), '%Y-%m-%d %H:00:00')`
	case "6h":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL HOUR(timestamp) MOD 6 HOUR), '%Y-%m-%d %H:00:00')`
	case "12h":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL HOUR(timestamp) MOD 12 HOUR), '%Y-%m-%d %H:00:00')`
	default:
		return `DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00')`
	}
}

func (r *ClickHouseRepository) buildLogWhere(f model.LogFilters) (string, []any) {
	where := ` team_id = ? AND timestamp BETWEEN ? AND ?`
	args := []any{f.TeamUUID, dbutil.SqlTime(f.StartMs), dbutil.SqlTime(f.EndMs)}

	if len(f.Levels) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Levels)
		where += ` AND level IN ` + in
		args = append(args, vals...)
	}
	if len(f.Services) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Services)
		where += ` AND service_name IN ` + in
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
	if len(f.Loggers) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Loggers)
		where += ` AND logger IN ` + in
		args = append(args, vals...)
	}
	if len(f.ExcludeLevels) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.ExcludeLevels)
		where += ` AND level NOT IN ` + in
		args = append(args, vals...)
	}
	if len(f.ExcludeServices) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.ExcludeServices)
		where += ` AND service_name NOT IN ` + in
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
		where += ` AND (message LIKE ? OR exception LIKE ?)`
		like := "%" + f.Search + "%"
		args = append(args, like, like)
	}
	return where, args
}

func (r *ClickHouseRepository) mapRowToLog(row map[string]any) model.Log {
	ts := dbutil.TimeFromAny(row["timestamp"])
	if ts.IsZero() {
		ts = time.Unix(0, 0).UTC()
	}

	id := normalizeLogID(row["id"])
	if id == "" || id == "0" {
		id = syntheticLogID(row, ts)
	}

	return model.Log{
		ID:          id,
		Timestamp:   ts,
		Level:       dbutil.StringFromAny(row["level"]),
		ServiceName: dbutil.StringFromAny(row["service_name"]),
		Logger:      dbutil.StringFromAny(row["logger"]),
		Message:     dbutil.StringFromAny(row["message"]),
		TraceID:     dbutil.StringFromAny(row["trace_id"]),
		SpanID:      dbutil.StringFromAny(row["span_id"]),
		Host:        dbutil.StringFromAny(row["host"]),
		Pod:         dbutil.StringFromAny(row["pod"]),
		Container:   dbutil.StringFromAny(row["container"]),
		Thread:      dbutil.StringFromAny(row["thread"]),
		Exception:   dbutil.StringFromAny(row["exception"]),
		Attributes:  dbutil.StringFromAny(row["attributes"]),
	}
}

func (r *ClickHouseRepository) mapRowsToLogs(rows []map[string]any) []model.Log {
	logs := make([]model.Log, 0, len(rows))
	for _, row := range rows {
		logs = append(logs, r.mapRowToLog(row))
	}
	return logs
}

func (r *ClickHouseRepository) mapRowsToFacets(rows []map[string]any) []model.Facet {
	facets := make([]model.Facet, 0, len(rows))
	for _, row := range rows {
		facets = append(facets, model.Facet{
			Value: dbutil.StringFromAny(row["value"]),
			Count: dbutil.Int64FromAny(row["count"]),
		})
	}
	return facets
}

func parseLogAttributes(raw string) map[string]string {
	attrs := map[string]string{}
	if strings.TrimSpace(raw) == "" {
		return attrs
	}

	var generic map[string]any
	if err := json.Unmarshal([]byte(raw), &generic); err != nil {
		return attrs
	}
	for k, v := range generic {
		switch typed := v.(type) {
		case string:
			attrs[k] = typed
		default:
			attrs[k] = strings.TrimSpace(fmt.Sprint(v))
		}
	}
	return attrs
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

func normalizeLogID(raw any) string {
	s := strings.TrimSpace(dbutil.StringFromAny(raw))
	if s == "" {
		return ""
	}

	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		if i <= 0 {
			return ""
		}
		return strconv.FormatInt(i, 10)
	}

	if u, err := strconv.ParseUint(s, 10, 64); err == nil {
		if u == 0 {
			return ""
		}
		return strconv.FormatUint(u, 10)
	}

	// Handle float/scientific notation from generic scanners.
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		if f <= 0 {
			return ""
		}
		return strconv.FormatUint(uint64(f), 10)
	}

	return s
}

func syntheticLogID(row map[string]any, ts time.Time) string {
	h := fnv.New64a()
	write := func(s string) {
		_, _ = h.Write([]byte(s))
		_, _ = h.Write([]byte{0})
	}

	write(ts.UTC().Format(time.RFC3339Nano))
	write(dbutil.StringFromAny(row["level"]))
	write(dbutil.StringFromAny(row["service_name"]))
	write(dbutil.StringFromAny(row["logger"]))
	write(dbutil.StringFromAny(row["message"]))
	write(dbutil.StringFromAny(row["trace_id"]))
	write(dbutil.StringFromAny(row["span_id"]))
	write(dbutil.StringFromAny(row["host"]))
	write(dbutil.StringFromAny(row["pod"]))
	write(dbutil.StringFromAny(row["container"]))
	write(dbutil.StringFromAny(row["thread"]))
	write(dbutil.StringFromAny(row["exception"]))
	write(dbutil.StringFromAny(row["attributes"]))

	// Keep fallback ids in signed 64-bit range for compatibility with endpoints
	// that still accept numeric log ids as int64.
	id := h.Sum64() & uint64((1<<63)-1)
	if id == 0 {
		id = 1
	}
	return strconv.FormatUint(id, 10)
}
