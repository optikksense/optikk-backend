package rundetail

import "time"

// LLMRunDetail contains the full detail of a single LLM call span.
type LLMRunDetail struct {
	SpanID        string            `json:"spanId"`
	TraceID       string            `json:"traceId"`
	ParentSpanID  string            `json:"parentSpanId,omitempty"`
	ServiceName   string            `json:"serviceName"`
	OperationName string            `json:"operationName"`
	Model         string            `json:"model"`
	Provider      string            `json:"provider,omitempty"`
	OperationType string            `json:"operationType,omitempty"`
	StartTime     time.Time         `json:"startTime"`
	DurationMs    float64           `json:"durationMs"`
	InputTokens   int64             `json:"inputTokens"`
	OutputTokens  int64             `json:"outputTokens"`
	TotalTokens   int64             `json:"totalTokens"`
	HasError      bool              `json:"hasError"`
	StatusMessage string            `json:"statusMessage,omitempty"`
	FinishReason  string            `json:"finishReason,omitempty"`
	SpanKind      string            `json:"spanKind"`
	Attributes    map[string]string `json:"attributes,omitempty"`
}

// LLMMessage represents a single prompt or completion message extracted from span events.
type LLMMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
	Type    string `json:"type"` // "prompt" or "completion"
}

// ChainSpan represents a span in the parent/child context chain.
type ChainSpan struct {
	SpanID        string    `json:"spanId"`
	ParentSpanID  string    `json:"parentSpanId,omitempty"`
	ServiceName   string    `json:"serviceName"`
	OperationName string    `json:"operationName"`
	StartTime     time.Time `json:"startTime"`
	DurationMs    float64   `json:"durationMs"`
	HasError      bool      `json:"hasError"`
	SpanKind      string    `json:"spanKind"`
	Role          string    `json:"role"` // llm_call, tool_call, retriever, chain, agent, other
	Model         string    `json:"model,omitempty"`
}

// LLMRunContext contains the parent chain and child spans for context.
type LLMRunContext struct {
	Ancestors []ChainSpan `json:"ancestors"`
	Current   ChainSpan   `json:"current"`
	Children  []ChainSpan `json:"children"`
}
