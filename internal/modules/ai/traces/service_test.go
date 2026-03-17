package traces

import (
	"testing"
	"time"
)

func TestBuildTraceSpansAndSummary(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	rows := []traceSpanDTO{
		{
			SpanID:        "root",
			ServiceName:   "gateway",
			OperationName: "chat.completion",
			Timestamp:     now,
			DurationMs:    120,
			KindString:    "SERVER",
		},
		{
			SpanID:        "llm",
			ParentSpanID:  "root",
			ServiceName:   "gateway",
			OperationName: "call-model",
			Timestamp:     now.Add(10 * time.Millisecond),
			DurationMs:    80,
			KindString:    "CLIENT",
			Model:         "gpt-4.1",
			InputTokens:   10,
			OutputTokens:  20,
		},
	}

	spans := buildTraceSpans(rows)
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
	if spans[1].Role != "llm_call" {
		t.Fatalf("expected llm_call role, got %q", spans[1].Role)
	}

	summary := buildSummary(spans)
	if summary.TotalSpans != 2 {
		t.Fatalf("expected total spans 2, got %d", summary.TotalSpans)
	}
	if summary.LLMCalls != 1 {
		t.Fatalf("expected llm calls 1, got %d", summary.LLMCalls)
	}
	if summary.TotalTokens != 30 {
		t.Fatalf("expected total tokens 30, got %d", summary.TotalTokens)
	}
	if summary.ServiceCount != 1 {
		t.Fatalf("expected service count 1, got %d", summary.ServiceCount)
	}
	if summary.TotalMs != 120 {
		t.Fatalf("expected total duration 120, got %v", summary.TotalMs)
	}
}
