package runs

import (
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

const (
	colModel         = "attributes.'gen_ai.request.model'::String"
	colProvider      = "attributes.'server.address'::String"
	colOperationType = "attributes.'gen_ai.operation.name'::String"
	colInputTokens   = "attributes.'gen_ai.usage.input_tokens'::Int64"  //nolint:gosec // G101 - column expressions, not credentials
	colOutputTokens  = "attributes.'gen_ai.usage.output_tokens'::Int64" //nolint:gosec // G101 - column expressions, not credentials
	colFinishReason  = "attributes.'gen_ai.response.finish_reasons'::String"
)

// buildWhereClause constructs the WHERE clause and args for LLM run queries.
func buildWhereClause(f LLMRunFilters) (where string, args []any) {
	clauses := []string{
		"s.team_id = @teamID",
		"s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd",
		"s.timestamp BETWEEN @start AND @end",
		colModel + " != ''",
	}
	args = []any{
		clickhouse.Named("teamID", uint32(f.TeamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(f.StartMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(f.EndMs/1000)),
		clickhouse.Named("start", time.UnixMilli(f.StartMs)),
		clickhouse.Named("end", time.UnixMilli(f.EndMs)),
	}

	appendIn := func(col, prefix string, values []string) {
		if len(values) == 0 {
			return
		}
		frag, inArgs := dbutil.NamedInArgs(col, prefix, values)
		clauses = append(clauses, frag)
		args = append(args, inArgs...)
	}
	appendIn(colModel, "model", f.Models)
	appendIn(colProvider, "provider", f.Providers)
	appendIn(colOperationType, "op", f.Operations)
	appendIn("s.service_name", "svc", f.Services)

	switch f.Status {
	case "error":
		clauses = append(clauses, "s.has_error = true")
	case "ok":
		clauses = append(clauses, "s.has_error = false")
	}

	if f.MinDurationMs > 0 {
		clauses = append(clauses, "s.duration_nano / 1000000 >= @minDuration")
		args = append(args, clickhouse.Named("minDuration", f.MinDurationMs))
	}
	if f.MaxDurationMs > 0 {
		clauses = append(clauses, "s.duration_nano / 1000000 <= @maxDuration")
		args = append(args, clickhouse.Named("maxDuration", f.MaxDurationMs))
	}

	if f.MinTokens > 0 {
		clauses = append(clauses, fmt.Sprintf("(%s + %s) >= @minTokens", colInputTokens, colOutputTokens))
		args = append(args, clickhouse.Named("minTokens", f.MinTokens))
	}
	if f.MaxTokens > 0 {
		clauses = append(clauses, fmt.Sprintf("(%s + %s) <= @maxTokens", colInputTokens, colOutputTokens))
		args = append(args, clickhouse.Named("maxTokens", f.MaxTokens))
	}

	if f.TraceID != "" {
		clauses = append(clauses, "s.trace_id = @traceID")
		args = append(args, clickhouse.Named("traceID", f.TraceID))
	}

	// Keyset pagination cursor
	if f.CursorTimestamp != nil && f.CursorSpanID != "" {
		clauses = append(clauses, "(s.timestamp < @cursorTs OR (s.timestamp = @cursorTs AND s.span_id < @cursorSpanID))")
		args = append(args, clickhouse.Named("cursorTs", *f.CursorTimestamp))
		args = append(args, clickhouse.Named("cursorSpanID", f.CursorSpanID))
	}

	where = "WHERE " + strings.Join(clauses, " AND ")
	return where, args
}
