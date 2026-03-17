package traces

import (
	"context"
	"strings"
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
	rows, err := s.repo.GetTraceSpans(context.Background(), teamID, traceID)
	if err != nil {
		return nil, err
	}
	return buildTraceSpans(rows), nil
}

func (s *TraceService) GetLLMTraceSummary(teamID int64, traceID string) (*LLMTraceSummary, error) {
	rows, err := s.repo.GetTraceSpans(context.Background(), teamID, traceID)
	if err != nil {
		return nil, err
	}
	spans := buildTraceSpans(rows)
	return buildSummary(spans), nil
}

func buildTraceSpans(rows []traceSpanDTO) []LLMTraceSpan {
	spans := make([]LLMTraceSpan, 0, len(rows))
	for _, row := range rows {
		model := row.Model
		name := row.OperationName
		spans = append(spans, LLMTraceSpan{
			SpanID:        row.SpanID,
			ParentSpanID:  row.ParentSpanID,
			ServiceName:   row.ServiceName,
			OperationName: name,
			StartTime:     row.Timestamp,
			DurationMs:    row.DurationMs,
			HasError:      row.HasError,
			SpanKind:      row.KindString,
			Role:          classifyRole(name, model),
			Model:         model,
			InputTokens:   row.InputTokens,
			OutputTokens:  row.OutputTokens,
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
