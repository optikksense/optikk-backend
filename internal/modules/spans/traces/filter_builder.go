package traces

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

func baseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

const maxTimeRangeMs = 30 * 24 * 60 * 60 * 1000 // 30 days

// normalizeTimeRange clamps start/end to a valid window.
func normalizeTimeRange(startMs, endMs int64) (int64, int64) {
	if endMs <= 0 {
		endMs = time.Now().UnixMilli()
	}
	if startMs <= 0 || (endMs-startMs) > maxTimeRangeMs {
		startMs = endMs - maxTimeRangeMs
	}
	return startMs, endMs
}

// buildWhereClause constructs the WHERE fragment and positional args for span queries.
// When f.SearchMode is "all", the root-span restriction (parent_span_id = ”) is skipped,
// enabling span-level search across all spans.
func buildWhereClause(f TraceFilters) (string, []any) {
	startMs, endMs := normalizeTimeRange(f.StartMs, f.EndMs)

	frag := ` WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN @start AND @end`
	args := []any{
		clickhouse.Named("teamID", uint32(f.TeamID)),
		timebucket.SpansBucketStart(startMs / 1000),
		timebucket.SpansBucketStart(endMs / 1000),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}

	// Root-span filter: only apply when searching root traces (default behaviour).
	if f.SearchMode != "all" {
		frag += ` AND s.parent_span_id = ''`
	}

	if f.SpanKind != "" {
		frag += ` AND s.kind_string = ?`
		args = append(args, f.SpanKind)
	}
	if f.SpanName != "" {
		frag += ` AND s.name = ?`
		args = append(args, f.SpanName)
	}

	if len(f.Services) > 0 {
		in, vals := dbutil.InClauseFromStrings(f.Services)
		frag += ` AND s.service_name IN ` + in
		args = append(args, vals...)
	}
	if f.Status != "" {
		if f.Status == "ERROR" {
			frag += ` AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)`
		} else {
			frag += ` AND s.status_code_string = ?`
			args = append(args, f.Status)
		}
	}
	if f.MinDuration != "" {
		frag += ` AND s.duration_nano >= ?`
		args = append(args, dbutil.MustAtoi64(f.MinDuration, 0)*1_000_000)
	}
	if f.MaxDuration != "" {
		frag += ` AND s.duration_nano <= ?`
		args = append(args, dbutil.MustAtoi64(f.MaxDuration, 0)*1_000_000)
	}
	if f.TraceID != "" {
		frag += ` AND s.trace_id = ?`
		args = append(args, f.TraceID)
	}
	if f.Operation != "" {
		frag += ` AND s.name LIKE ?`
		args = append(args, "%"+f.Operation+"%")
	}
	if f.HTTPMethod != "" {
		frag += ` AND upper(s.http_method) = upper(?)`
		args = append(args, f.HTTPMethod)
	}
	if f.HTTPStatus != "" {
		frag += ` AND s.response_status_code = ?`
		args = append(args, f.HTTPStatus)
	}
	for _, af := range f.AttributeFilters {
		switch af.Op {
		case "neq":
			frag += ` AND s.attributes_string[?] != ?`
			args = append(args, af.Key, af.Value)
		case "contains":
			frag += ` AND positionCaseInsensitive(s.attributes_string[?], ?) > 0`
			args = append(args, af.Key, af.Value)
		case "regex":
			frag += ` AND match(s.attributes_string[?], ?)`
			args = append(args, af.Key, af.Value)
		default: // "eq"
			frag += ` AND s.attributes_string[?] = ?`
			args = append(args, af.Key, af.Value)
		}
	}
	return frag, args
}

// splitWhereClause returns the fragment string and args separately.
// Useful when the summary query must reuse the same WHERE without cursor conditions.
func splitWhereClause(f TraceFilters) (frag string, args []any) {
	return buildWhereClause(f)
}
