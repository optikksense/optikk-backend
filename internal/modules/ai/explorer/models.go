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
// AvgLatencyMs is computed in the service from LatencyMsSum/LatencyMsCount.
// Percentile slots are zero-filled by CH and overwritten from sketch.Querier.
// Services carries the set of service_names in scope so the service layer can
// merge only relevant SpanLatencyService sketches.
type aiSummaryRow struct {
	TotalCalls        uint64   `ch:"total_calls"`
	ErrorCalls        uint64   `ch:"error_calls"`
	LatencyMsSum      float64  `ch:"latency_ms_sum"`
	LatencyMsCount    int64    `ch:"latency_ms_count"`
	AvgLatencyMs      float64  // set by service
	P50LatencyMs      float64  `ch:"p50_latency_ms"`
	P95LatencyMs      float64  `ch:"p95_latency_ms"`
	P99LatencyMs      float64  `ch:"p99_latency_ms"`
	Services          []string `ch:"services"`
	TotalInputTokens  float64  `ch:"total_input_tokens"`
	TotalOutputTokens float64  `ch:"total_output_tokens"`
}

// aiFacetRow represents a single facet bucket returned by the UNION ALL facet query.
type aiFacetRow struct {
	FacetKey   string `ch:"facet_key"`
	FacetValue string `ch:"facet_value"`
	Count      uint64 `ch:"count"`
}

// aiTrendRow represents one time bucket in the trend timeseries.
// AvgLatencyMs is computed in the service from LatencyMsSum/LatencyMsCount.
type aiTrendRow struct {
	TimeBucket     string  `ch:"time_bucket"`
	TotalCalls     uint64  `ch:"total_calls"`
	ErrorCalls     uint64  `ch:"error_calls"`
	LatencyMsSum   float64 `ch:"latency_ms_sum"`
	LatencyMsCount int64   `ch:"latency_ms_count"`
	AvgLatencyMs   float64 // set by service
	TotalTokens    float64 `ch:"total_tokens"`
}

// aiSessionRow is one grouped session/conversation bucket from GenAI spans.
type aiSessionRow struct {
	SessionID         string    `ch:"session_id"`
	GenerationCount   uint64    `ch:"generation_count"`
	TraceCount        uint64    `ch:"trace_count"`
	FirstStart        time.Time `ch:"first_start"`
	LastStart         time.Time `ch:"last_start"`
	TotalInputTokens  float64   `ch:"total_input_tokens"`
	TotalOutputTokens float64   `ch:"total_output_tokens"`
	ErrorCount        uint64    `ch:"error_count"`
	DominantModel     string    `ch:"dominant_model"`
	DominantService   string    `ch:"dominant_service"`
}
