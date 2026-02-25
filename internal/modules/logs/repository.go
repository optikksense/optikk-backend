package logs

import (
	"fmt"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type Repository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *Repository {
	return &Repository{db: db}
}

const logCols = `id, timestamp, level, service_name, logger, message,
	trace_id, span_id, host, pod, container, thread, exception, attributes`

type LogFilters struct {
	TeamUUID   string
	StartMs    int64
	EndMs      int64
	Levels     []string
	Services   []string
	Hosts      []string
	Pods       []string
	Containers []string
	Loggers    []string
	TraceID    string
	SpanID     string
	Search     string
}

func buildLogWhere(f LogFilters) (string, []any) {
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

func (r *Repository) GetLogs(f LogFilters, limit int, direction string, cursor int64) ([]map[string]any, int64, []map[string]any, []map[string]any, []map[string]any, error) {
	where, args := buildLogWhere(f)
	if cursor > 0 {
		if direction == "desc" {
			where += ` AND id < ?`
		} else {
			where += ` AND id > ?`
		}
		args = append(args, cursor)
	}

	query := fmt.Sprintf(`SELECT %s FROM logs WHERE%s ORDER BY id %s LIMIT ?`, logCols, where, direction)
	args = append(args, limit)

	logs, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, 0, nil, nil, nil, err
	}

	baseWhere, baseArgs := buildLogWhere(f)
	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM logs WHERE`+baseWhere, baseArgs...)
	levelFacets, _ := dbutil.QueryMaps(r.db, `SELECT level, COUNT(*) as count FROM logs WHERE`+baseWhere+` GROUP BY level ORDER BY count DESC`, baseArgs...)
	serviceFacets, _ := dbutil.QueryMaps(r.db, `SELECT service_name, COUNT(*) as count FROM logs WHERE`+baseWhere+` GROUP BY service_name ORDER BY count DESC LIMIT 50`, baseArgs...)
	hostFacets, _ := dbutil.QueryMaps(r.db, `SELECT host, COUNT(*) as count FROM logs WHERE`+baseWhere+` AND host != '' GROUP BY host ORDER BY count DESC LIMIT 50`, baseArgs...)

	return logs, total, levelFacets, serviceFacets, hostFacets, nil
}

func (r *Repository) GetLogHistogram(f LogFilters, bucketExpr string) ([]map[string]any, error) {
	where, args := buildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as time_bucket, level, COUNT(*) as count
		FROM logs WHERE%s
		GROUP BY %s, level
		ORDER BY time_bucket ASC`, bucketExpr, where, bucketExpr)
	return dbutil.QueryMaps(r.db, query, args...)
}

func (r *Repository) GetLogVolume(f LogFilters, bucketExpr string) ([]map[string]any, error) {
	where, args := buildLogWhere(f)
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
	return dbutil.QueryMaps(r.db, query, args...)
}

func (r *Repository) GetLogStats(f LogFilters) (int64, []map[string]any, []map[string]any, []map[string]any, []map[string]any, []map[string]any, error) {
	where, args := buildLogWhere(f)
	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM logs WHERE`+where, args...)
	levelRows, _ := dbutil.QueryMaps(r.db, `SELECT level as value, COUNT(*) as count FROM logs WHERE`+where+` GROUP BY level ORDER BY count DESC`, args...)
	serviceRows, _ := dbutil.QueryMaps(r.db, `SELECT service_name as value, COUNT(*) as count FROM logs WHERE`+where+` GROUP BY service_name ORDER BY count DESC LIMIT 50`, args...)
	hostRows, _ := dbutil.QueryMaps(r.db, `SELECT host as value, COUNT(*) as count FROM logs WHERE`+where+` AND host != '' GROUP BY host ORDER BY count DESC LIMIT 50`, args...)
	podRows, _ := dbutil.QueryMaps(r.db, `SELECT pod as value, COUNT(*) as count FROM logs WHERE`+where+` AND pod != '' GROUP BY pod ORDER BY count DESC LIMIT 50`, args...)
	loggerRows, _ := dbutil.QueryMaps(r.db, `SELECT logger as value, COUNT(*) as count FROM logs WHERE`+where+` AND logger != '' GROUP BY logger ORDER BY count DESC LIMIT 50`, args...)

	return total, levelRows, serviceRows, hostRows, podRows, loggerRows, nil
}

func (r *Repository) GetLogFields(f LogFilters, col string) ([]map[string]any, error) {
	where, args := buildLogWhere(f)
	query := fmt.Sprintf(`
		SELECT %s as value, COUNT(*) as count
		FROM logs WHERE%s AND %s != ''
		GROUP BY %s ORDER BY count DESC LIMIT 200`, col, where, col, col)
	return dbutil.QueryMaps(r.db, query, args...)
}

func (r *Repository) GetLogSurrounding(teamUUID string, logID int64, before, after int) (map[string]any, []map[string]any, []map[string]any, error) {
	anchor, err := dbutil.QueryMap(r.db,
		fmt.Sprintf(`SELECT %s FROM logs WHERE team_id = ? AND id = ? LIMIT 1`, logCols),
		teamUUID, logID)
	if err != nil || len(anchor) == 0 {
		return nil, nil, nil, nil
	}
	svc := dbutil.StringFromAny(anchor["service_name"])

	beforeRows, _ := dbutil.QueryMaps(r.db,
		fmt.Sprintf(`SELECT %s FROM logs WHERE team_id = ? AND service_name = ? AND id < ? ORDER BY id DESC LIMIT ?`, logCols),
		teamUUID, svc, logID, before)
	for i, j := 0, len(beforeRows)-1; i < j; i, j = i+1, j-1 {
		beforeRows[i], beforeRows[j] = beforeRows[j], beforeRows[i]
	}
	afterRows, _ := dbutil.QueryMaps(r.db,
		fmt.Sprintf(`SELECT %s FROM logs WHERE team_id = ? AND service_name = ? AND id > ? ORDER BY id ASC LIMIT ?`, logCols),
		teamUUID, svc, logID, after)

	return anchor, beforeRows, afterRows, nil
}

func (r *Repository) GetLogDetail(teamUUID, traceID, spanID string, center, from, to time.Time) (map[string]any, []map[string]any, error) {
	logRow, err := dbutil.QueryMap(r.db, fmt.Sprintf(`
		SELECT %s FROM logs
		WHERE team_id = ? AND trace_id = ? AND span_id = ?
		  AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp DESC LIMIT 1
	`, logCols), teamUUID, traceID, spanID,
		center.Add(-1*time.Second), center.Add(1*time.Second))
	if err != nil {
		return nil, nil, err
	}
	serviceName := dbutil.StringFromAny(logRow["service_name"])
	contextLogs := []map[string]any{}
	if serviceName != "" {
		contextLogs, _ = dbutil.QueryMaps(r.db, fmt.Sprintf(`
			SELECT %s FROM logs
			WHERE team_id = ? AND service_name = ? AND timestamp BETWEEN ? AND ?
			ORDER BY timestamp ASC LIMIT 100
		`, logCols), teamUUID, serviceName, from, to)
	}

	return logRow, contextLogs, nil
}

func (r *Repository) GetTraceLogs(teamUUID, traceID string) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT %s FROM logs
		WHERE team_id = ? AND trace_id = ?
		ORDER BY timestamp ASC LIMIT 500
	`, logCols), teamUUID, traceID)
}
