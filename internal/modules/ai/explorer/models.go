package explorer

import "time"

// aiCallRow maps a ClickHouse result row for an AI/LLM span.
type aiCallRow struct {
	SpanID        string    `ch:"span_id"`
	TraceID       string    `ch:"trace_id"`
	ServiceName   string    `ch:"service_name"`
	OperationName string    `ch:"operation_name"`
	StartTime     time.Time `ch:"start_time"`
	DurationMs    float64   `ch:"duration_ms"`
	Status        string    `ch:"status"`
	StatusMessage string    `ch:"status_message"`

	// GenAI semantic convention attributes extracted via JSONExtractString.
	AISystem       string  `ch:"ai_system"`
	AIRequestModel string  `ch:"ai_request_model"`
	AIResponseModel string `ch:"ai_response_model"`
	AIOperation    string  `ch:"ai_operation"`
	InputTokens    float64 `ch:"input_tokens"`
	OutputTokens   float64 `ch:"output_tokens"`
	TotalTokens    float64 `ch:"total_tokens"`
	Temperature    string  `ch:"temperature"`
	MaxTokens      string  `ch:"max_tokens"`
	FinishReason   string  `ch:"finish_reason"`
	ErrorType      string  `ch:"error_type"`
}

// aiSummaryRow holds aggregated stats for the current query window.
type aiSummaryRow struct {
	TotalCalls       uint64  `ch:"total_calls"`
	ErrorCalls       uint64  `ch:"error_calls"`
	AvgLatencyMs     float64 `ch:"avg_latency_ms"`
	P50LatencyMs     float64 `ch:"p50_latency_ms"`
	P95LatencyMs     float64 `ch:"p95_latency_ms"`
	P99LatencyMs     float64 `ch:"p99_latency_ms"`
	TotalInputTokens float64 `ch:"total_input_tokens"`
	TotalOutputTokens float64 `ch:"total_output_tokens"`
}

// aiFacetRow represents a single facet bucket returned by the UNION ALL facet query.
type aiFacetRow struct {
	FacetKey   string `ch:"facet_key"`
	FacetValue string `ch:"facet_value"`
	Count      uint64 `ch:"count"`
}

// aiTrendRow represents one time bucket in the trend timeseries.
type aiTrendRow struct {
	TimeBucket   string  `ch:"time_bucket"`
	TotalCalls   uint64  `ch:"total_calls"`
	ErrorCalls   uint64  `ch:"error_calls"`
	AvgLatencyMs float64 `ch:"avg_latency_ms"`
	TotalTokens  float64 `ch:"total_tokens"`
}
