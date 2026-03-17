package runs

import (
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

const (
	colModel         = "attributes.'gen_ai.request.model'::String"
	colProvider      = "attributes.'server.address'::String"
	colOperationType = "attributes.'gen_ai.operation.name'::String"
	colInputTokens   = "attributes.'gen_ai.usage.input_tokens'::Int64"
	colOutputTokens  = "attributes.'gen_ai.usage.output_tokens'::Int64"
	colFinishReason  = "attributes.'gen_ai.response.finish_reasons'::String"
)

func baseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// buildWhereClause constructs the WHERE clause and args for LLM run queries.
func buildWhereClause(f LLMRunFilters) (string, []any) {
	clauses := []string{
		"s.team_id = @teamID",
		"s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd",
		"s.timestamp BETWEEN @start AND @end",
		fmt.Sprintf("%s != ''", colModel),
	}
	args := []any{
		clickhouse.Named("teamID", uint32(f.TeamID)),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(f.StartMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(f.EndMs/1000)),
		clickhouse.Named("start", time.UnixMilli(f.StartMs)),
		clickhouse.Named("end", time.UnixMilli(f.EndMs)),
	}

	if len(f.Models) > 0 {
		placeholders := make([]string, len(f.Models))
		for i, m := range f.Models {
			name := fmt.Sprintf("model%d", i)
			placeholders[i] = "@" + name
			args = append(args, clickhouse.Named(name, m))
		}
		clauses = append(clauses, fmt.Sprintf("%s IN (%s)", colModel, strings.Join(placeholders, ",")))
	}

	if len(f.Providers) > 0 {
		placeholders := make([]string, len(f.Providers))
		for i, p := range f.Providers {
			name := fmt.Sprintf("provider%d", i)
			placeholders[i] = "@" + name
			args = append(args, clickhouse.Named(name, p))
		}
		clauses = append(clauses, fmt.Sprintf("%s IN (%s)", colProvider, strings.Join(placeholders, ",")))
	}

	if len(f.Operations) > 0 {
		placeholders := make([]string, len(f.Operations))
		for i, o := range f.Operations {
			name := fmt.Sprintf("op%d", i)
			placeholders[i] = "@" + name
			args = append(args, clickhouse.Named(name, o))
		}
		clauses = append(clauses, fmt.Sprintf("%s IN (%s)", colOperationType, strings.Join(placeholders, ",")))
	}

	if len(f.Services) > 0 {
		placeholders := make([]string, len(f.Services))
		for i, sv := range f.Services {
			name := fmt.Sprintf("svc%d", i)
			placeholders[i] = "@" + name
			args = append(args, clickhouse.Named(name, sv))
		}
		clauses = append(clauses, fmt.Sprintf("s.service_name IN (%s)", strings.Join(placeholders, ",")))
	}

	if f.Status == "error" {
		clauses = append(clauses, "s.has_error = true")
	} else if f.Status == "ok" {
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

	return "WHERE " + strings.Join(clauses, " AND "), args
}
