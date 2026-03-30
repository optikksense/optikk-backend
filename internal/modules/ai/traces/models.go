package traces

import "time"

// LLMTraceSpan represents a span in an LLM-annotated trace.
type LLMTraceSpan struct {
	SpanID        string    `json:"spanId"`
	ParentSpanID  string    `json:"parentSpanId,omitempty"`
	ServiceName   string    `json:"serviceName"`
	OperationName string    `json:"operationName"`
	StartTime     time.Time `json:"startTime"`
	DurationMs    float64   `json:"durationMs"`
	HasError      bool      `json:"hasError"`
	SpanKind      string    `json:"spanKind"`
	Role          string    `json:"role"`
	Model         string    `json:"model,omitempty"`
	InputTokens   int64     `json:"inputTokens,omitempty"`
	OutputTokens  int64     `json:"outputTokens,omitempty"`
}

// LLMTraceSummary provides aggregate stats for an LLM trace.
type LLMTraceSummary struct {
	TotalSpans   int64    `json:"totalSpans"`
	LLMCalls     int64    `json:"llmCalls"`
	ToolCalls    int64    `json:"toolCalls"`
	TotalTokens  int64    `json:"totalTokens"`
	ModelsUsed   []string `json:"modelsUsed"`
	TotalMs      float64  `json:"totalMs"`
	LLMMs        float64  `json:"llmMs"`
	LLMTimePct   float64  `json:"llmTimePct"`
	HasErrors    bool     `json:"hasErrors"`
	ServiceCount int64    `json:"serviceCount"`
}
