package runs

import "time"

// LLMRun represents a single LLM call span in the runs explorer.
type LLMRun struct {
	SpanID        string    `ch:"span_id"        json:"spanId"`
	TraceID       string    `ch:"trace_id"       json:"traceId"`
	ParentSpanID  string    `ch:"parent_span_id" json:"parentSpanId,omitempty"`
	ServiceName   string    `ch:"service_name"   json:"serviceName"`
	OperationName string    `ch:"operation_name" json:"operationName"`
	Model         string    `ch:"model"          json:"model"`
	Provider      string    `ch:"provider"       json:"provider,omitempty"`
	OperationType string    `ch:"operation_type" json:"operationType,omitempty"`
	StartTime     time.Time `ch:"timestamp"      json:"startTime"`
	DurationMs    float64   `ch:"duration_ms"    json:"durationMs"`
	InputTokens   int64     `ch:"input_tokens"   json:"inputTokens"`
	OutputTokens  int64     `ch:"output_tokens"  json:"outputTokens"`
	TotalTokens   int64     `ch:"total_tokens"   json:"totalTokens"`
	HasError      bool      `ch:"has_error"      json:"hasError"`
	StatusMessage string    `ch:"status_message" json:"statusMessage,omitempty"`
	FinishReason  string    `ch:"finish_reason"  json:"finishReason,omitempty"`
	InputPreview  string    `json:"inputPreview,omitempty"`
	OutputPreview string    `json:"outputPreview,omitempty"`
	SpanKind      string    `ch:"kind_string"    json:"spanKind"`
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
	TotalRuns    int64   `ch:"total_runs"    json:"totalRuns"`
	ErrorRuns    int64   `ch:"error_runs"    json:"errorRuns"`
	ErrorRate    float64 `ch:"error_rate"    json:"errorRate"`
	AvgLatencyMs float64 `ch:"avg_latency_ms" json:"avgLatencyMs"`
	P95LatencyMs float64 `ch:"p95_latency_ms" json:"p95LatencyMs"`
	TotalTokens  int64   `ch:"total_tokens"  json:"totalTokens"`
	UniqueModels int64   `ch:"unique_models" json:"uniqueModels"`
}

// LLMRunModel represents a distinct model found in LLM spans.
type LLMRunModel struct {
	Model    string `ch:"model"    json:"model"`
	Provider string `ch:"provider" json:"provider,omitempty"`
}

// LLMRunOperation represents a distinct operation type.
type LLMRunOperation struct {
	OperationType string `ch:"operation_type" json:"operationType"`
}
