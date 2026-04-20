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

// BuildLogWhere returns a combined WHERE clause + positional args.
// Kept for callers that don't want the prewhere split; new code should prefer
// BuildLogWhereSplit so CH can prune parts before evaluating non-indexed
// filters.
func BuildLogWhere(f LogFilters) (where string, args []any) {
	prewhere, rest, allArgs := BuildLogWhereSplit(f)
	// Merge prewhere + rest with AND for legacy callers; CH treats the result
	// identically to a plain WHERE.
	combined := prewhere
	if rest != "" {
		combined += " AND" + rest
	}
	return combined, allArgs
}

// BuildLogWhereSplit separates indexed-prefix predicates from the rest so
// repositories can emit `PREWHERE <prefix> WHERE <rest>`. CH prunes parts on
// PREWHERE before evaluating anything else — especially important for
// text-search and custom-attribute filters that would otherwise scan every
// row.
//
// The prefix includes everything that matches the MergeTree sort key prefix
// (team_id, ts_bucket_start, timestamp) plus low-cardinality / indexed
// columns (service, severity_text, host). Everything else — body text,
// attributes_string regex, trace_id/span_id exact match — stays in the
// regular WHERE.
func BuildLogWhereSplit(f LogFilters) (prewhere, where string, args []any) {
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
	startBucket := utils.LogsBucketStart(startMs / 1000)
	endBucket := utils.LogsBucketStart(endMs / 1000)

	prewhere = ` team_id = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ?`
	args = []any{
		uint32(f.TeamID), //nolint:gosec // G115
		startBucket,
		endBucket,
		time.Unix(0, int64(startNs)),
		time.Unix(0, int64(endNs)),
	}

	appendIn := func(target *string, column string, values []string, negated bool) {
		if len(values) == 0 {
			return
		}
		if negated {
			*target += ` AND ` + column + ` NOT IN (?)`
		} else {
			*target += ` AND ` + column + ` IN (?)`
		}
		args = append(args, values)
	}

	// Indexed-prefix filters go to PREWHERE.
	appendIn(&prewhere, "severity_text", f.Severities, false)
	appendIn(&prewhere, "service", f.Services, false)
	appendIn(&prewhere, "host", f.Hosts, false)
	appendIn(&prewhere, "severity_text", f.ExcludeSeverities, true)
	appendIn(&prewhere, "service", f.ExcludeServices, true)
	appendIn(&prewhere, "host", f.ExcludeHosts, true)

	// Pod / container / environment are materialized but sit outside the sort
	// key prefix — keep them in WHERE (still indexed).
	appendIn(&where, "pod", f.Pods, false)
	appendIn(&where, "container", f.Containers, false)
	appendIn(&where, "environment", f.Environments, false)

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

	return prewhere, where, args
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
