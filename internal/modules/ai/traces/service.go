package traces

import (
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type Service interface {
	GetLLMTrace(teamID int64, traceID string) ([]LLMTraceSpan, error)
	GetLLMTraceSummary(teamID int64, traceID string) (*LLMTraceSummary, error)
}

type TraceService struct {
	repo Repository
}

func NewService(repo Repository) *TraceService {
	return &TraceService{repo: repo}
}

func (s *TraceService) GetLLMTrace(teamID int64, traceID string) ([]LLMTraceSpan, error) {
	rows, err := s.repo.GetTraceSpans(teamID, traceID)
	if err != nil {
		return nil, err
	}
	return buildTraceSpans(rows), nil
}

func (s *TraceService) GetLLMTraceSummary(teamID int64, traceID string) (*LLMTraceSummary, error) {
	rows, err := s.repo.GetTraceSpans(teamID, traceID)
	if err != nil {
		return nil, err
	}
	spans := buildTraceSpans(rows)
	return buildSummary(spans), nil
}

func buildTraceSpans(rows []map[string]any) []LLMTraceSpan {
	spans := make([]LLMTraceSpan, 0, len(rows))
	for _, row := range rows {
		model := dbutil.StringFromAny(row["model"])
		name := dbutil.StringFromAny(row["operation_name"])
		spans = append(spans, LLMTraceSpan{
			SpanID:        dbutil.StringFromAny(row["span_id"]),
			ParentSpanID:  dbutil.StringFromAny(row["parent_span_id"]),
			ServiceName:   dbutil.StringFromAny(row["service_name"]),
			OperationName: name,
			StartTime:     dbutil.TimeFromAny(row["timestamp"]),
			DurationMs:    dbutil.Float64FromAny(row["duration_ms"]),
			HasError:      dbutil.BoolFromAny(row["has_error"]),
			SpanKind:      dbutil.StringFromAny(row["kind_string"]),
			Role:          classifyRole(name, model),
			Model:         model,
			InputTokens:   dbutil.Int64FromAny(row["input_tokens"]),
			OutputTokens:  dbutil.Int64FromAny(row["output_tokens"]),
		})
	}
	return spans
}

func buildSummary(spans []LLMTraceSpan) *LLMTraceSummary {
	summary := &LLMTraceSummary{}
	modelSet := map[string]bool{}
	serviceSet := map[string]bool{}

	for _, sp := range spans {
		summary.TotalSpans++
		serviceSet[sp.ServiceName] = true
		if sp.HasError {
			summary.HasErrors = true
		}

		switch sp.Role {
		case "llm_call":
			summary.LLMCalls++
			summary.LLMMs += sp.DurationMs
			summary.TotalTokens += sp.InputTokens + sp.OutputTokens
			if sp.Model != "" {
				modelSet[sp.Model] = true
			}
		case "tool_call":
			summary.ToolCalls++
		}
	}

	// Total trace duration from root span
	if len(spans) > 0 {
		var maxEnd float64
		for _, sp := range spans {
			if sp.ParentSpanID == "" || sp.ParentSpanID == "0000000000000000" {
				summary.TotalMs = sp.DurationMs
			}
			end := float64(sp.StartTime.UnixMilli()) + sp.DurationMs
			if end > maxEnd {
				maxEnd = end
			}
		}
		if summary.TotalMs == 0 && len(spans) > 0 {
			start := float64(spans[0].StartTime.UnixMilli())
			summary.TotalMs = maxEnd - start
		}
	}

	if summary.TotalMs > 0 {
		summary.LLMTimePct = (summary.LLMMs / summary.TotalMs) * 100
	}

	for m := range modelSet {
		summary.ModelsUsed = append(summary.ModelsUsed, m)
	}
	summary.ServiceCount = int64(len(serviceSet))

	return summary
}

func classifyRole(name, model string) string {
	if model != "" {
		return "llm_call"
	}
	lower := strings.ToLower(name)
	if strings.Contains(lower, "tool") {
		return "tool_call"
	}
	if strings.Contains(lower, "retriev") {
		return "retriever"
	}
	if strings.Contains(lower, "chain") {
		return "chain"
	}
	if strings.Contains(lower, "agent") {
		return "agent"
	}
	return "other"
}
