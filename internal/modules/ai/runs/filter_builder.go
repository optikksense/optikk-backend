package runs

import (
	"fmt"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
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

// buildWhereClause constructs the WHERE clause and args for LLM run queries.
func buildWhereClause(f LLMRunFilters) (string, []any) {
	clauses := []string{
		"s.team_id = ?",
		"s.ts_bucket_start BETWEEN ? AND ?",
		"s.timestamp BETWEEN ? AND ?",
		fmt.Sprintf("%s != ''", colModel),
	}
	args := []any{
		uint32(f.TeamID),
		timebucket.SpansBucketStart(f.StartMs / 1000),
		timebucket.SpansBucketStart(f.EndMs / 1000),
		dbutil.SqlTime(f.StartMs),
		dbutil.SqlTime(f.EndMs),
	}

	if len(f.Models) > 0 {
		placeholders := make([]string, len(f.Models))
		for i, m := range f.Models {
			placeholders[i] = "?"
			args = append(args, m)
		}
		clauses = append(clauses, fmt.Sprintf("%s IN (%s)", colModel, strings.Join(placeholders, ",")))
	}

	if len(f.Providers) > 0 {
		placeholders := make([]string, len(f.Providers))
		for i, p := range f.Providers {
			placeholders[i] = "?"
			args = append(args, p)
		}
		clauses = append(clauses, fmt.Sprintf("%s IN (%s)", colProvider, strings.Join(placeholders, ",")))
	}

	if len(f.Operations) > 0 {
		placeholders := make([]string, len(f.Operations))
		for i, o := range f.Operations {
			placeholders[i] = "?"
			args = append(args, o)
		}
		clauses = append(clauses, fmt.Sprintf("%s IN (%s)", colOperationType, strings.Join(placeholders, ",")))
	}

	if len(f.Services) > 0 {
		placeholders := make([]string, len(f.Services))
		for i, sv := range f.Services {
			placeholders[i] = "?"
			args = append(args, sv)
		}
		clauses = append(clauses, fmt.Sprintf("s.service_name IN (%s)", strings.Join(placeholders, ",")))
	}

	if f.Status == "error" {
		clauses = append(clauses, "s.has_error = true")
	} else if f.Status == "ok" {
		clauses = append(clauses, "s.has_error = false")
	}

	if f.MinDurationMs > 0 {
		clauses = append(clauses, "s.duration_nano / 1000000 >= ?")
		args = append(args, f.MinDurationMs)
	}
	if f.MaxDurationMs > 0 {
		clauses = append(clauses, "s.duration_nano / 1000000 <= ?")
		args = append(args, f.MaxDurationMs)
	}

	if f.MinTokens > 0 {
		clauses = append(clauses, fmt.Sprintf("(%s + %s) >= ?", colInputTokens, colOutputTokens))
		args = append(args, f.MinTokens)
	}
	if f.MaxTokens > 0 {
		clauses = append(clauses, fmt.Sprintf("(%s + %s) <= ?", colInputTokens, colOutputTokens))
		args = append(args, f.MaxTokens)
	}

	if f.TraceID != "" {
		clauses = append(clauses, "s.trace_id = ?")
		args = append(args, f.TraceID)
	}

	// Keyset pagination cursor
	if f.CursorTimestamp != nil && f.CursorSpanID != "" {
		clauses = append(clauses, "(s.timestamp < ? OR (s.timestamp = ? AND s.span_id < ?))")
		args = append(args, *f.CursorTimestamp, *f.CursorTimestamp, f.CursorSpanID)
	}

	return "WHERE " + strings.Join(clauses, " AND "), args
}
