package querycompiler

import (
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const maxTimeRangeMs = 30 * 24 * 60 * 60 * 1000

// Compile produces a Compiled WHERE + args block for the given target.
// Dropped clauses are recorded in Compiled.DroppedClauses so callers can
// surface them to the client as warnings[].
func Compile(f Filters, tgt Target) Compiled {
	startMs, endMs := clampRange(f.StartMs, f.EndMs)
	if tgt == TargetRaw {
		return compileRaw(f, startMs, endMs)
	}
	return compileRollup(f, startMs, endMs, tgt)
}

func compileRaw(f Filters, startMs, endMs int64) Compiled {
	var sb strings.Builder
	args := baseRawArgs(f.TeamID, startMs, endMs)
	sb.WriteString(` team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end`)
	appendInList(&sb, &args, "severity_text", f.Severities, false)
	appendInList(&sb, &args, "service", f.Services, false)
	appendInList(&sb, &args, "host", f.Hosts, false)
	appendInList(&sb, &args, "pod", f.Pods, false)
	appendInList(&sb, &args, "container", f.Containers, false)
	appendInList(&sb, &args, "environment", f.Environments, false)
	appendInList(&sb, &args, "severity_text", f.ExcludeSeverities, true)
	appendInList(&sb, &args, "service", f.ExcludeServices, true)
	appendInList(&sb, &args, "host", f.ExcludeHosts, true)
	appendScalar(&sb, &args, "trace_id", f.TraceID)
	appendScalar(&sb, &args, "span_id", f.SpanID)
	appendSearch(&sb, &args, f.Search, f.SearchMode)
	appendAttrs(&sb, &args, f.Attributes)
	return Compiled{Where: sb.String(), Args: args}
}

func compileRollup(f Filters, startMs, endMs int64, _ Target) Compiled {
	var sb strings.Builder
	args := baseRollupArgs(f.TeamID, startMs, endMs)
	sb.WriteString(` team_id = @teamID AND bucket_ts BETWEEN @start AND @end`)
	appendInList(&sb, &args, "severity_text", f.Severities, false)
	appendInList(&sb, &args, "service", f.Services, false)
	appendInList(&sb, &args, "environment", f.Environments, false)
	appendInList(&sb, &args, "host", f.Hosts, false)
	appendInList(&sb, &args, "pod", f.Pods, false)
	appendInList(&sb, &args, "severity_text", f.ExcludeSeverities, true)
	appendInList(&sb, &args, "service", f.ExcludeServices, true)
	appendInList(&sb, &args, "host", f.ExcludeHosts, true)
	dropped := rollupDroppedClauses(f)
	return Compiled{Where: sb.String(), Args: args, DroppedClauses: dropped}
}

func rollupDroppedClauses(f Filters) []string {
	var dropped []string
	if len(f.Containers) > 0 {
		dropped = append(dropped, "container filter ignored by rollup")
	}
	if f.TraceID != "" {
		dropped = append(dropped, "trace_id filter ignored by rollup")
	}
	if f.SpanID != "" {
		dropped = append(dropped, "span_id filter ignored by rollup")
	}
	if f.Search != "" {
		dropped = append(dropped, "body-search filter ignored by rollup")
	}
	if len(f.Attributes) > 0 {
		dropped = append(dropped, "attribute filters ignored by rollup")
	}
	return dropped
}

func baseRawArgs(teamID int64, startMs, endMs int64) []any {
	startNs := startMs * 1_000_000
	endNs := endMs * 1_000_000
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 domain-constrained
		clickhouse.Named("bucketStart", utils.LogsBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", utils.LogsBucketStart(endMs/1000)),
		clickhouse.Named("start", time.Unix(0, startNs)),
		clickhouse.Named("end", time.Unix(0, endNs)),
	}
}

func baseRollupArgs(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 domain-constrained
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
		sb.WriteString(" AND ")
		sb.WriteString(col)
		sb.WriteString(" NOT IN @")
		sb.WriteString(name)
	} else {
		sb.WriteString(" AND ")
		sb.WriteString(col)
		sb.WriteString(" IN @")
		sb.WriteString(name)
	}
	*args = append(*args, clickhouse.Named(name, vals))
}

func appendScalar(sb *strings.Builder, args *[]any, col, val string) {
	if val == "" {
		return
	}
	name := fmt.Sprintf("%s_%d", col, len(*args))
	sb.WriteString(" AND ")
	sb.WriteString(col)
	sb.WriteString(" = @")
	sb.WriteString(name)
	*args = append(*args, clickhouse.Named(name, val))
}

func appendSearch(sb *strings.Builder, args *[]any, search, mode string) {
	if search == "" {
		return
	}
	name := fmt.Sprintf("search_%d", len(*args))
	if mode == "exact" {
		sb.WriteString(fmt.Sprintf(" AND positionCaseInsensitive(body, @%s) > 0", name))
	} else {
		sb.WriteString(fmt.Sprintf(" AND hasToken(lower(body), lower(@%s))", name))
	}
	*args = append(*args, clickhouse.Named(name, search))
}

func appendAttrs(sb *strings.Builder, args *[]any, attrs []AttrFilter) {
	for i, af := range attrs {
		k := fmt.Sprintf("akey_%d", i)
		v := fmt.Sprintf("aval_%d", i)
		switch af.Op {
		case "neq":
			sb.WriteString(fmt.Sprintf(" AND attributes_string[@%s] != @%s", k, v))
		case "contains":
			sb.WriteString(fmt.Sprintf(" AND positionCaseInsensitive(attributes_string[@%s], @%s) > 0", k, v))
		case "regex":
			sb.WriteString(fmt.Sprintf(" AND match(attributes_string[@%s], @%s)", k, v))
		default:
			sb.WriteString(fmt.Sprintf(" AND attributes_string[@%s] = @%s", k, v))
		}
		*args = append(*args, clickhouse.Named(k, af.Key), clickhouse.Named(v, af.Value))
	}
}
