package shared

import (
	"context"
	"fmt"
	"strings"
	"time"

	database "github.com/observability/observability-backend-go/internal/database"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/platform/timebucket"
)

const LogColumns = `id, timestamp, observed_timestamp, severity_text, severity_number,
	body, trace_id, span_id, trace_flags,
	service, host, pod, container, environment,
	attributes_string, attributes_number, attributes_bool,
	scope_name, scope_version`

func QueryCount(ctx context.Context, db *database.NativeQuerier, query string, args ...any) int64 {
	var row CountRow
	if err := db.QueryRow(ctx, &row, query, args...); err != nil {
		return 0
	}
	return row.Count
}

func BuildLogWhere(f LogFilters) (string, []any) {
	const maxTimeRangeMs = 30 * 24 * 60 * 60 * 1000

	startMs := f.StartMs
	endMs := f.EndMs
	if endMs <= 0 {
		endMs = time.Now().UnixMilli()
	}
	if startMs <= 0 || (endMs-startMs) > maxTimeRangeMs {
		startMs = endMs - maxTimeRangeMs
	}

	startNs := uint64(startMs) * 1_000_000
	endNs := uint64(endMs) * 1_000_000
	startBucket := timebucket.LogsBucketStart(startMs / 1000)
	endBucket := timebucket.LogsBucketStart(endMs / 1000)

	where := ` team_id = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ?`
	args := []any{uint32(f.TeamID), startBucket, endBucket, startNs, endNs}

	appendInClause := func(column string, values []string, negated bool) {
		if len(values) == 0 {
			return
		}
		in, vals := dbutil.InClauseFromStrings(values)
		if negated {
			where += ` AND ` + column + ` NOT IN ` + in
		} else {
			where += ` AND ` + column + ` IN ` + in
		}
		args = append(args, vals...)
	}

	appendInClause("severity_text", f.Severities, false)
	appendInClause("service", f.Services, false)
	appendInClause("host", f.Hosts, false)
	appendInClause("pod", f.Pods, false)
	appendInClause("container", f.Containers, false)
	appendInClause("environment", f.Environments, false)
	appendInClause("severity_text", f.ExcludeSeverities, true)
	appendInClause("service", f.ExcludeServices, true)
	appendInClause("host", f.ExcludeHosts, true)

	if f.TraceID != "" {
		where += ` AND trace_id = ?`
		args = append(args, f.TraceID)
	}
	if f.SpanID != "" {
		where += ` AND span_id = ?`
		args = append(args, f.SpanID)
	}
	if f.Search != "" {
		if f.SearchMode == "exact" {
			where += ` AND positionCaseInsensitive(body, ?) > 0`
		} else {
			where += ` AND ngramSearch(lower(body), lower(?)) > 0`
		}
		args = append(args, f.Search)
	}

	for _, af := range f.AttributeFilters {
		switch af.Op {
		case "neq":
			where += ` AND attributes_string[?] != ?`
		case "contains":
			where += ` AND positionCaseInsensitive(attributes_string[?], ?) > 0`
		case "regex":
			where += ` AND match(attributes_string[?], ?)`
		default:
			where += ` AND attributes_string[?] = ?`
		}
		args = append(args, af.Key, af.Value)
	}

	return where, args
}

func LogBucketExpr(startMs, endMs int64) string {
	tsExpr := "toDateTime(intDiv(timestamp, 1000000000))"
	return timebucket.ExprForColumn(startMs, endMs, tsExpr)
}

func LogBucketExprForStep(startMs, endMs int64, step string) string {
	if strings.TrimSpace(step) == "" {
		return LogBucketExpr(startMs, endMs)
	}

	tsExpr := "toDateTime(intDiv(timestamp, 1000000000))"
	return fmt.Sprintf("formatDateTime(%s(%s), '%%Y-%%m-%%d %%H:%%i:00')",
		bucketFuncFromStrategy(timebucket.ByName(step)), tsExpr)
}

func NormalizeRoute(raw string) string {
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

func bucketFuncFromStrategy(s timebucket.Strategy) string {
	expr := s.GetBucketExpression()
	start := strings.Index(expr, "(") + 1
	end := strings.Index(expr[start:], "(")
	if start > 0 && end > 0 {
		return expr[start : start+end]
	}
	return "toStartOfMinute"
}
