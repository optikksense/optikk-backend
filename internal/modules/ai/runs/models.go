package runs

import "time"

// LLMRun represents a single LLM call span in the runs explorer.
type LLMRun struct {
	SpanID        string    `json:"spanId"`
	TraceID       string    `json:"traceId"`
	ParentSpanID  string    `json:"parentSpanId,omitempty"`
	ServiceName   string    `json:"serviceName"`
	OperationName string    `json:"operationName"`
	Model         string    `json:"model"`
	Provider      string    `json:"provider,omitempty"`
	OperationType string    `json:"operationType,omitempty"`
	StartTime     time.Time `json:"startTime"`
	DurationMs    float64   `json:"durationMs"`
	InputTokens   int64     `json:"inputTokens"`
	OutputTokens  int64     `json:"outputTokens"`
	TotalTokens   int64     `json:"totalTokens"`
	HasError      bool      `json:"hasError"`
	StatusMessage string    `json:"statusMessage,omitempty"`
	FinishReason  string    `json:"finishReason,omitempty"`
	InputPreview  string    `json:"inputPreview,omitempty"`
	OutputPreview string    `json:"outputPreview,omitempty"`
	SpanKind      string    `json:"spanKind"`
}

// LLMRunFilters defines the filter parameters for listing runs.
type LLMRunFilters struct {
	TeamID        int64
	StartMs       int64
	EndMs         int64
	Models        []string
	Providers     []string
	Operations    []string
	Services      []string
	Status        string // "error" or "ok"
	MinDurationMs int64
	MaxDurationMs int64
	MinTokens     int64
	MaxTokens     int64
	TraceID       string
	Limit         int
	// Keyset pagination cursor
	CursorTimestamp *time.Time
	CursorSpanID   string
}

// LLMRunSummary provides aggregate stats for the current filter set.
type LLMRunSummary struct {
	TotalRuns    int64   `json:"totalRuns"`
	ErrorRuns    int64   `json:"errorRuns"`
	ErrorRate    float64 `json:"errorRate"`
	AvgLatencyMs float64 `json:"avgLatencyMs"`
	P95LatencyMs float64 `json:"p95LatencyMs"`
	TotalTokens  int64   `json:"totalTokens"`
	UniqueModels int64   `json:"uniqueModels"`
}

// LLMRunModel represents a distinct model found in LLM spans.
type LLMRunModel struct {
	Model    string `json:"model"`
	Provider string `json:"provider,omitempty"`
}

// LLMRunOperation represents a distinct operation type.
type LLMRunOperation struct {
	OperationType string `json:"operationType"`
}
