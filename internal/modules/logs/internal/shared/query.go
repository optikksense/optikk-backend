package shared

import (
	"fmt"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const LogColumns = `timestamp, observed_timestamp, severity_text, severity_number,
	body, trace_id, span_id, trace_flags,
	service, host, pod, container, environment,
	attributes_string, attributes_number, attributes_bool,
	scope_name, scope_version`

func BuildLogWhere(f LogFilters) (where string, args []any) {
	const maxTimeRangeMs = 30 * 24 * 60 * 60 * 1000

	startMs := f.StartMs
	endMs := f.EndMs
	if endMs <= 0 {
		endMs = time.Now().UnixMilli()
	}
	if startMs <= 0 || (endMs-startMs) > maxTimeRangeMs {
		startMs = endMs - maxTimeRangeMs
	}

	startNs := uint64(startMs) * 1_000_000 //nolint:gosec // G115 - domain-constrained value
	endNs := uint64(endMs) * 1_000_000     //nolint:gosec // G115 - domain-constrained value

	// Filter by team_id + timestamp only. Rows must still match ingest shape
	// (ts_bucket_start = floor bucket of event time), but an extra
	// ts_bucket_start BETWEEN predicate could exclude valid rows when the
	// column was mis-set (manual inserts) or drifted from timestamp.
	where = ` team_id = ? AND timestamp BETWEEN ? AND ?`
	args = []any{
		uint32(f.TeamID), //nolint:gosec // G115
		time.Unix(0, int64(startNs)),
		time.Unix(0, int64(endNs)),
	}

	appendInClause := func(column string, values []string, negated bool) {
		if len(values) == 0 {
			return
		}
		if negated {
			where += ` AND ` + column + ` NOT IN (?)`
		} else {
			where += ` AND ` + column + ` IN (?)`
		}
		args = append(args, values)
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
	tsExpr := "toDateTime(timestamp)"
	return utils.ExprForColumn(startMs, endMs, tsExpr)
}

func LogBucketExprForStep(startMs, endMs int64, step string) string {
	if strings.TrimSpace(step) == "" {
		return LogBucketExpr(startMs, endMs)
	}

	tsExpr := "toDateTime(timestamp)"
	return fmt.Sprintf("formatDateTime(%s(%s), '%%Y-%%m-%%d %%H:%%i:00')",
		bucketFuncFromStrategy(utils.ByName(step)), tsExpr)
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

func bucketFuncFromStrategy(s utils.Strategy) string {
	expr := s.GetBucketExpression()
	start := strings.Index(expr, "(") + 1
	end := strings.Index(expr[start:], "(")
	if start > 0 && end > 0 {
		return expr[start : start+end]
	}
	return "toStartOfMinute"
}
