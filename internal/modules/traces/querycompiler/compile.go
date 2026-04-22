package querycompiler

import (
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const maxTimeRangeMs = 30 * 24 * 60 * 60 * 1000

// Compile produces a parameterized WHERE fragment for the chosen target.
// Callers embed it via `WHERE ` + Compiled.Where.
func Compile(f Filters, tgt Target) Compiled {
	startMs, endMs := clampRange(f.StartMs, f.EndMs)
	switch tgt {
	case TargetTracesIndex:
		return compileTracesIndex(f, startMs, endMs)
	case TargetSpansRaw:
		return compileSpansRaw(f, startMs, endMs)
	case TargetSpansRollup, TargetFacetRollup:
		return compileRollup(f, startMs, endMs)
	}
	return compileSpansRaw(f, startMs, endMs)
}

func compileTracesIndex(f Filters, startMs, endMs int64) Compiled {
	var sb strings.Builder
	args := baseSpanArgs(f.TeamID, startMs, endMs, "start_ms")
	sb.WriteString(` team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND start_ms BETWEEN @startMs AND @endMs`)
	appendInList(&sb, &args, "root_service", f.Services, false)
	appendInList(&sb, &args, "root_service", f.ExcludeServices, true)
	appendInList(&sb, &args, "root_operation", f.Operations, false)
	appendInList(&sb, &args, "root_http_method", f.HTTPMethods, false)
	appendInList(&sb, &args, "root_http_status", f.HTTPStatuses, false)
	appendInList(&sb, &args, "root_status", f.Statuses, false)
	appendInList(&sb, &args, "root_status", f.ExcludeStatuses, true)
	appendScalar(&sb, &args, "trace_id", f.TraceID)
	if f.HasError != nil {
		appendBool(&sb, &args, "has_error", *f.HasError)
	}
	if f.MinDurationNs > 0 {
		appendInt(&sb, &args, "duration_ns", ">=", f.MinDurationNs)
	}
	if f.MaxDurationNs > 0 {
		appendInt(&sb, &args, "duration_ns", "<=", f.MaxDurationNs)
	}
	appendSearchIndex(&sb, &args, f.Search)
	dropped := tracesIndexDropped(f)
	return Compiled{Where: sb.String(), Args: args, DroppedClauses: dropped}
}

func compileSpansRaw(f Filters, startMs, endMs int64) Compiled {
	var sb strings.Builder
	args := baseSpanArgs(f.TeamID, startMs, endMs, "")
	sb.WriteString(` team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end`)
	appendInList(&sb, &args, "service_name", f.Services, false)
	appendInList(&sb, &args, "service_name", f.ExcludeServices, true)
	appendInList(&sb, &args, "name", f.Operations, false)
	appendInList(&sb, &args, "kind_string", f.SpanKinds, false)
	appendInList(&sb, &args, "http_method", f.HTTPMethods, false)
	appendInList(&sb, &args, "response_status_code", f.HTTPStatuses, false)
	appendInList(&sb, &args, "status_code_string", f.Statuses, false)
	appendInList(&sb, &args, "status_code_string", f.ExcludeStatuses, true)
	appendInList(&sb, &args, "mat_deployment_environment", f.Environments, false)
	appendInList(&sb, &args, "mat_peer_service", f.PeerServices, false)
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
	return Compiled{Where: sb.String(), Args: args}
}

func compileRollup(f Filters, startMs, endMs int64) Compiled {
	var sb strings.Builder
	args := baseRollupArgs(f.TeamID, startMs, endMs)
	sb.WriteString(` team_id = @teamID AND bucket_ts BETWEEN @start AND @end`)
	appendInList(&sb, &args, "service", f.Services, false)
	appendInList(&sb, &args, "service", f.ExcludeServices, true)
	appendInList(&sb, &args, "operation", f.Operations, false)
	appendInList(&sb, &args, "http_method", f.HTTPMethods, false)
	dropped := rollupDropped(f)
	return Compiled{Where: sb.String(), Args: args, DroppedClauses: dropped}
}

func tracesIndexDropped(f Filters) []string {
	var dropped []string
	if len(f.SpanKinds) > 0 {
		dropped = append(dropped, "span_kind filter requires raw spans; dropped on traces_index")
	}
	if len(f.Attributes) > 0 {
		dropped = append(dropped, "attribute filters require raw spans; dropped on traces_index")
	}
	return dropped
}

func rollupDropped(f Filters) []string {
	var dropped []string
	if f.TraceID != "" {
		dropped = append(dropped, "trace_id filter ignored by rollup")
	}
	if f.Search != "" {
		dropped = append(dropped, "search ignored by rollup")
	}
	if len(f.Attributes) > 0 {
		dropped = append(dropped, "attribute filters ignored by rollup")
	}
	if len(f.SpanKinds) > 0 {
		dropped = append(dropped, "span_kind filter ignored by rollup")
	}
	return dropped
}

func baseSpanArgs(teamID, startMs, endMs int64, _ string) []any {
	startNs := startMs * 1_000_000
	endNs := endMs * 1_000_000
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("bucketStart", utils.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", utils.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.Unix(0, startNs)),
		clickhouse.Named("end", time.Unix(0, endNs)),
		clickhouse.Named("startMs", startMs),
		clickhouse.Named("endMs", endMs),
	}
}

func baseRollupArgs(teamID, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
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

func appendSearchIndex(sb *strings.Builder, args *[]any, search string) {
	if search == "" {
		return
	}
	name := fmt.Sprintf("search_%d", len(*args))
	fmt.Fprintf(sb, " AND (positionCaseInsensitive(trace_id, @%s) > 0 OR positionCaseInsensitive(root_service, @%s) > 0 OR positionCaseInsensitive(root_operation, @%s) > 0)", name, name, name)
	*args = append(*args, clickhouse.Named(name, search))
}

func appendSpanSearch(sb *strings.Builder, args *[]any, search string) {
	if search == "" {
		return
	}
	name := fmt.Sprintf("search_%d", len(*args))
	fmt.Fprintf(sb, " AND (positionCaseInsensitive(trace_id, @%s) > 0 OR positionCaseInsensitive(service_name, @%s) > 0 OR positionCaseInsensitive(name, @%s) > 0 OR positionCaseInsensitive(status_message, @%s) > 0)", name, name, name, name)
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
