package traces

import "time"

type traceSpanDTO struct {
	SpanID        string    `ch:"span_id"`
	ParentSpanID  string    `ch:"parent_span_id"`
	ServiceName   string    `ch:"service_name"`
	OperationName string    `ch:"operation_name"`
	Timestamp     time.Time `ch:"timestamp"`
	DurationMs    float64   `ch:"duration_ms"`
	HasError      bool      `ch:"has_error"`
	KindString    string    `ch:"kind_string"`
	Model         string    `ch:"model"`
	InputTokens   int64     `ch:"input_tokens"`
	OutputTokens  int64     `ch:"output_tokens"`
}
