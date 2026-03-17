package rundetail

import "time"

// runDetailDTO is an unexported scan target for GetRunDetail queries.
// LLMRunDetail has an Attributes map field that cannot be scanned from CH,
// so we use this DTO then convert to the domain model.
type runDetailDTO struct {
	SpanID        string    `ch:"span_id"`
	TraceID       string    `ch:"trace_id"`
	ParentSpanID  string    `ch:"parent_span_id"`
	ServiceName   string    `ch:"service_name"`
	OperationName string    `ch:"operation_name"`
	Model         string    `ch:"model"`
	Provider      string    `ch:"provider"`
	OperationType string    `ch:"operation_type"`
	StartTime     time.Time `ch:"timestamp"`
	DurationMs    float64   `ch:"duration_ms"`
	InputTokens   int64     `ch:"input_tokens"`
	OutputTokens  int64     `ch:"output_tokens"`
	TotalTokens   int64     `ch:"total_tokens"`
	HasError      bool      `ch:"has_error"`
	StatusMessage string    `ch:"status_message"`
	FinishReason  string    `ch:"finish_reason"`
	SpanKind      string    `ch:"kind_string"`
}

type runEventDTO struct {
	SpanID    string `ch:"span_id"`
	EventJSON string `ch:"event_json"`
}

type traceContextSpanDTO struct {
	SpanID        string    `ch:"span_id"`
	ParentSpanID  string    `ch:"parent_span_id"`
	ServiceName   string    `ch:"service_name"`
	OperationName string    `ch:"operation_name"`
	Timestamp     time.Time `ch:"timestamp"`
	DurationMs    float64   `ch:"duration_ms"`
	HasError      bool      `ch:"has_error"`
	KindString    string    `ch:"kind_string"`
	Model         string    `ch:"model"`
}

func (d *runDetailDTO) toModel() *LLMRunDetail {
	return &LLMRunDetail{
		SpanID:        d.SpanID,
		TraceID:       d.TraceID,
		ParentSpanID:  d.ParentSpanID,
		ServiceName:   d.ServiceName,
		OperationName: d.OperationName,
		Model:         d.Model,
		Provider:      d.Provider,
		OperationType: d.OperationType,
		StartTime:     d.StartTime,
		DurationMs:    d.DurationMs,
		InputTokens:   d.InputTokens,
		OutputTokens:  d.OutputTokens,
		TotalTokens:   d.TotalTokens,
		HasError:      d.HasError,
		StatusMessage: d.StatusMessage,
		FinishReason:  d.FinishReason,
		SpanKind:      d.SpanKind,
	}
}
