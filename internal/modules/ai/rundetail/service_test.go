package rundetail

import (
	"testing"
	"time"
)

func TestExtractMessagesParsesPromptAndCompletionEvents(t *testing.T) {
	rows := []runEventDTO{
		{
			EventJSON: `{"name":"gen_ai.content.prompt","attributes":{"gen_ai.prompt":"[{\"role\":\"user\",\"content\":\"hello\"}]"}}`,
		},
		{
			EventJSON: `{"name":"gen_ai.content.completion","attributes":{"gen_ai.completion":"[{\"role\":\"assistant\",\"content\":\"world\"}]"}}`,
		},
	}

	messages := extractMessages(rows)
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
	if messages[0].Role != "user" || messages[0].Content != "hello" {
		t.Fatalf("unexpected first message: %+v", messages[0])
	}
	if messages[1].Role != "assistant" || messages[1].Type != "completion" {
		t.Fatalf("unexpected second message: %+v", messages[1])
	}
}

func TestBuildContextBuildsAncestorsCurrentAndChildren(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	rows := []traceContextSpanDTO{
		{SpanID: "root", ServiceName: "api", OperationName: "root", Timestamp: now, DurationMs: 100},
		{SpanID: "current", ParentSpanID: "root", ServiceName: "api", OperationName: "call-model", Timestamp: now.Add(10 * time.Millisecond), DurationMs: 60, Model: "gpt-4.1"},
		{SpanID: "child", ParentSpanID: "current", ServiceName: "worker", OperationName: "tool-search", Timestamp: now.Add(20 * time.Millisecond), DurationMs: 20},
	}

	ctx := buildContext("current", rows)
	if ctx.Current.SpanID != "current" {
		t.Fatalf("expected current span, got %+v", ctx.Current)
	}
	if len(ctx.Ancestors) != 1 || ctx.Ancestors[0].SpanID != "root" {
		t.Fatalf("unexpected ancestors: %+v", ctx.Ancestors)
	}
	if len(ctx.Children) != 1 || ctx.Children[0].SpanID != "child" {
		t.Fatalf("unexpected children: %+v", ctx.Children)
	}
	if ctx.Current.Role != "llm_call" {
		t.Fatalf("expected llm_call role, got %q", ctx.Current.Role)
	}
}
