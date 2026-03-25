package traces

import (
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	rootspan "github.com/observability/observability-backend-go/internal/modules/spans/shared/rootspan"
)

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

// buildWhereClause constructs the WHERE fragment and named args for span queries.
// When f.SearchMode is "all", the root-span restriction is skipped so span-level
// search can cover the whole trace tree.
func buildWhereClause(f TraceFilters) (string, []any) {
	startMs, endMs := normalizeTimeRange(f.StartMs, f.EndMs)

	frag := ` WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`
	args := dbutil.SpanBaseParams(f.TeamID, startMs, endMs)

	if f.SearchMode != "all" {
		frag += ` AND ` + rootspan.Condition("s")
	}

	if f.SpanKind != "" {
		frag += ` AND s.kind_string = @spanKind`
		args = append(args, clickhouse.Named("spanKind", f.SpanKind))
	}
	if f.SpanName != "" {
		frag += ` AND s.name = @spanName`
		args = append(args, clickhouse.Named("spanName", f.SpanName))
	}

	if len(f.Services) > 0 {
		inFrag, inArgs := dbutil.NamedInArgs("s.service_name", "service", f.Services)
		frag += ` AND ` + inFrag
		args = append(args, inArgs...)
	}
	if f.Status != "" {
		if f.Status == "ERROR" {
			frag += ` AND (s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)`
		} else {
			frag += ` AND s.status_code_string = @status`
			args = append(args, clickhouse.Named("status", f.Status))
		}
	}
	if f.MinDuration != "" {
		frag += ` AND s.duration_nano >= @minDuration`
		args = append(args, clickhouse.Named("minDuration", dbutil.MustAtoi64(f.MinDuration, 0)*1_000_000))
	}
	if f.MaxDuration != "" {
		frag += ` AND s.duration_nano <= @maxDuration`
		args = append(args, clickhouse.Named("maxDuration", dbutil.MustAtoi64(f.MaxDuration, 0)*1_000_000))
	}
	if f.TraceID != "" {
		frag += ` AND s.trace_id = @traceID`
		args = append(args, clickhouse.Named("traceID", f.TraceID))
	}
	if f.SearchText != "" {
		frag += ` AND (
			positionCaseInsensitive(s.trace_id, @searchText) > 0 OR
			positionCaseInsensitive(s.service_name, @searchText) > 0 OR
			positionCaseInsensitive(s.name, @searchText) > 0 OR
			positionCaseInsensitive(s.status_message, @searchText) > 0
		)`
		args = append(args, clickhouse.Named("searchText", f.SearchText))
	}
	if f.Operation != "" {
		frag += ` AND s.name LIKE @operationLike`
		args = append(args, clickhouse.Named("operationLike", "%"+f.Operation+"%"))
	}
	if f.HTTPMethod != "" {
		frag += ` AND upper(s.http_method) = upper(@httpMethod)`
		args = append(args, clickhouse.Named("httpMethod", f.HTTPMethod))
	}
	if f.HTTPStatus != "" {
		frag += ` AND s.response_status_code = @httpStatus`
		args = append(args, clickhouse.Named("httpStatus", f.HTTPStatus))
	}

	for i, af := range f.AttributeFilters {
		keyName := fmt.Sprintf("attrKey%d", i)
		valueName := fmt.Sprintf("attrValue%d", i)
		switch af.Op {
		case "neq":
			frag += ` AND s.attributes_string[@` + keyName + `] != @` + valueName
		case "contains":
			frag += ` AND positionCaseInsensitive(s.attributes_string[@` + keyName + `], @` + valueName + `) > 0`
		case "regex":
			frag += ` AND match(s.attributes_string[@` + keyName + `], @` + valueName + `)`
		default:
			frag += ` AND s.attributes_string[@` + keyName + `] = @` + valueName
		}
		args = append(args, clickhouse.Named(keyName, af.Key), clickhouse.Named(valueName, af.Value))
	}

	return frag, args
}

// splitWhereClause returns the fragment string and args separately.
// Useful when the summary query must reuse the same WHERE without cursor conditions.
func splitWhereClause(f TraceFilters) (frag string, args []any) {
	return buildWhereClause(f)
}
