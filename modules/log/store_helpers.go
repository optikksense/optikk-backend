package logs

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// logCols is the SELECT column list for raw log queries.
const logCols = `id, timestamp, level, service_name, logger, message,
	trace_id, span_id, host, pod, container, thread, exception, attributes`

// buildLogWhere builds a WHERE clause from LogFilters.
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

// mapRowToLog converts a generic map row into a typed Log.
func mapRowToLog(row map[string]any) Log {
	ts := dbutil.TimeFromAny(row["timestamp"])
	if ts.IsZero() {
		ts = time.Unix(0, 0).UTC()
	}

	id := normalizeLogID(row["id"])
	if id == "" || id == "0" {
		id = syntheticLogID(row, ts)
	}

	return Log{
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

// mapRowsToLogs converts multiple generic map rows into typed Logs.
func mapRowsToLogs(rows []map[string]any) []Log {
	logs := make([]Log, 0, len(rows))
	for _, row := range rows {
		logs = append(logs, mapRowToLog(row))
	}
	return logs
}

// mapRowsToFacets converts generic map rows into typed Facets.
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

// logBucketExpr returns the ClickHouse SQL expression for time bucketing.
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

// autoStep picks a bucket size so the time window produces ~20-30 bars.
func autoStep(startMs, endMs int64) string {
	ms := endMs - startMs
	targetBars := int64(30)
	stepMs := ms / targetBars

	switch {
	case stepMs <= 1*60*1000:
		return "1m"
	case stepMs <= 2*60*1000:
		return "2m"
	case stepMs <= 5*60*1000:
		return "5m"
	case stepMs <= 10*60*1000:
		return "10m"
	case stepMs <= 15*60*1000:
		return "15m"
	case stepMs <= 30*60*1000:
		return "30m"
	case stepMs <= 60*60*1000:
		return "1h"
	case stepMs <= 2*60*60*1000:
		return "2h"
	case stepMs <= 6*60*60*1000:
		return "6h"
	default:
		return "12h"
	}
}

// stepDuration returns the duration for a given step string.
func stepDuration(step string) time.Duration {
	switch step {
	case "1m":
		return time.Minute
	case "2m":
		return 2 * time.Minute
	case "5m":
		return 5 * time.Minute
	case "10m":
		return 10 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "30m":
		return 30 * time.Minute
	case "1h":
		return time.Hour
	case "2h":
		return 2 * time.Hour
	case "6h":
		return 6 * time.Hour
	case "12h":
		return 12 * time.Hour
	default:
		return time.Minute
	}
}

// bucketKey truncates t to the step boundary and returns the bucket label string.
func bucketKey(t time.Time, step string) string {
	d := stepDuration(step)
	truncated := t.Truncate(d)
	switch step {
	case "1h", "2h", "6h", "12h":
		return truncated.UTC().Format("2006-01-02 15:00:00")
	default:
		return truncated.UTC().Format("2006-01-02 15:04:00")
	}
}

// fillVolumeBuckets ensures every interval in [startMs, endMs] has a bucket,
// inserting zero-count entries for missing ones.
func fillVolumeBuckets(buckets []LogVolumeBucket, startMs, endMs int64, step string) []LogVolumeBucket {
	d := stepDuration(step)
	start := time.UnixMilli(startMs).UTC().Truncate(d)
	end := time.UnixMilli(endMs).UTC()

	// Build lookup from existing data.
	byKey := make(map[string]LogVolumeBucket, len(buckets))
	for _, b := range buckets {
		byKey[b.TimeBucket] = b
	}

	var result []LogVolumeBucket
	for t := start; !t.After(end); t = t.Add(d) {
		key := bucketKey(t, step)
		if b, ok := byKey[key]; ok {
			result = append(result, b)
		} else {
			result = append(result, LogVolumeBucket{TimeBucket: key})
		}
	}
	return result
}
