package querycompiler

import (
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

const maxTimeRangeMs = 30 * 24 * 60 * 60 * 1000

// Compile produces a parameterized WHERE fragment for the chosen target.
// Callers embed it via `WHERE ` + Compiled.Where.
func Compile(f Filters, tgt Target) Compiled {
	startMs, endMs := clampRange(f.StartMs, f.EndMs)
	return compileSpansRaw(f, startMs, endMs)
}



func compileSpansRaw(f Filters, startMs, endMs int64) Compiled {
	var sb strings.Builder
	args := baseSpanArgs(f.TeamID, startMs, endMs, "")
	sb.WriteString(`ts_bucket BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end`)
	// Resource filters (Services, ExcludeServices, Environments) flow through
	// internal/modules/traces/shared/resource.WithFingerprints into a
	// `fingerprint IN (...)` PREWHERE — don't push them down here too.
	appendInList(&sb, &args, "name", f.Operations, false)
	appendInList(&sb, &args, "kind_string", f.SpanKinds, false)
	appendInList(&sb, &args, "http_method", f.HTTPMethods, false)
	appendInList(&sb, &args, "response_status_code", f.HTTPStatuses, false)
	appendInList(&sb, &args, "status_code_string", f.Statuses, false)
	appendInList(&sb, &args, "status_code_string", f.ExcludeStatuses, true)
	appendInList(&sb, &args, "peer_service", f.PeerServices, false)
	appendScalar(&sb, &args, "trace_id", f.TraceID)
	if f.HasError != nil {
		appendBool(&sb, &args, "has_error", *f.HasError)
	}
	if f.MinDurationNs > 0 {
		appendInt(&sb, &args, "duration_nano", ">=", f.MinDurationNs)
	}
	if f.MaxDurationNs > 0 {
		appendInt(&sb, &args, "duration_nano", "<=", f.MaxDurationNs)
	}
	appendSpanSearch(&sb, &args, f.Search)
	appendAttrs(&sb, &args, f.Attributes)
	return Compiled{PreWhere: "team_id = @teamID", Where: sb.String(), Args: args}
}



func baseSpanArgs(teamID, startMs, endMs int64, _ string) []any {
	startNs := startMs * 1_000_000
	endNs := endMs * 1_000_000
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.Unix(0, startNs)),
		clickhouse.Named("end", time.Unix(0, endNs)),
		clickhouse.Named("startMs", startMs),
		clickhouse.Named("endMs", endMs),
	}
}



func clampRange(startMs, endMs int64) (int64, int64) {
	if endMs <= 0 {
		endMs = time.Now().UnixMilli()
	}
	if startMs <= 0 || (endMs-startMs) > maxTimeRangeMs {
		startMs = endMs - maxTimeRangeMs
	}
	return startMs, endMs
}

func appendInList(sb *strings.Builder, args *[]any, col string, vals []string, negated bool) {
	if len(vals) == 0 {
		return
	}
	name := fmt.Sprintf("%s_%d", col, len(*args))
	if negated {
		fmt.Fprintf(sb, " AND %s NOT IN @%s", col, name)
	} else {
		fmt.Fprintf(sb, " AND %s IN @%s", col, name)
	}
	*args = append(*args, clickhouse.Named(name, vals))
}

func appendScalar(sb *strings.Builder, args *[]any, col, val string) {
	if val == "" {
		return
	}
	name := fmt.Sprintf("%s_%d", col, len(*args))
	fmt.Fprintf(sb, " AND %s = @%s", col, name)
	*args = append(*args, clickhouse.Named(name, val))
}

func appendBool(sb *strings.Builder, args *[]any, col string, v bool) {
	name := fmt.Sprintf("%s_%d", col, len(*args))
	fmt.Fprintf(sb, " AND %s = @%s", col, name)
	*args = append(*args, clickhouse.Named(name, v))
}

func appendInt(sb *strings.Builder, args *[]any, col, op string, v int64) {
	name := fmt.Sprintf("%s_%d", col, len(*args))
	fmt.Fprintf(sb, " AND %s %s @%s", col, op, name)
	*args = append(*args, clickhouse.Named(name, v))
}



func appendSpanSearch(sb *strings.Builder, args *[]any, search string) {
	if search == "" {
		return
	}
	name := fmt.Sprintf("search_%d", len(*args))
	fmt.Fprintf(sb, " AND (positionCaseInsensitive(trace_id, @%s) > 0 OR positionCaseInsensitive(service, @%s) > 0 OR positionCaseInsensitive(name, @%s) > 0 OR positionCaseInsensitive(status_message, @%s) > 0)", name, name, name, name)
	*args = append(*args, clickhouse.Named(name, search))
}

func appendAttrs(sb *strings.Builder, args *[]any, attrs []AttrFilter) {
	for i, af := range attrs {
		k := fmt.Sprintf("akey_%d", i)
		v := fmt.Sprintf("aval_%d", i)
		switch af.Op {
		case "neq":
			fmt.Fprintf(sb, " AND JSONExtractString(toJSONString(attributes), @%s) != @%s", k, v)
		case "contains":
			fmt.Fprintf(sb, " AND positionCaseInsensitive(JSONExtractString(toJSONString(attributes), @%s), @%s) > 0", k, v)
		case "regex":
			fmt.Fprintf(sb, " AND match(JSONExtractString(toJSONString(attributes), @%s), @%s)", k, v)
		default:
			fmt.Fprintf(sb, " AND JSONExtractString(toJSONString(attributes), @%s) = @%s", k, v)
		}
		*args = append(*args, clickhouse.Named(k, af.Key), clickhouse.Named(v, af.Value))
	}
}
