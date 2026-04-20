package explorer

import "time"

// aiCallRow maps a ClickHouse result row for an AI/LLM span.
// Token fields arrive as raw JSON attribute strings; the service parses
// them to float64 Go-side so no float cast is needed in SQL.
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
	AISystem        string `ch:"ai_system"`
	AIRequestModel  string `ch:"ai_request_model"`
	AIResponseModel string `ch:"ai_response_model"`
	AIOperation     string `ch:"ai_operation"`
	InputTokensRaw  string `ch:"input_tokens_raw"`
	OutputTokensRaw string `ch:"output_tokens_raw"`
	TotalTokensRaw  string `ch:"total_tokens_raw"`
	Temperature     string `ch:"temperature"`
	MaxTokens       string `ch:"max_tokens"`
	FinishReason    string `ch:"finish_reason"`
	ErrorType       string `ch:"error_type"`
}

// aiSummaryRow holds aggregated stats for the current query window. It is
// composed by the repository from the three parallel summary-leg scans
// (totals / errors / services) rather than a single combinator query.
// AvgLatencyMs is computed in the service from LatencyMsSum/LatencyMsCount.
// Percentile slots are zero-filled and overwritten from sketch.Querier.
// Services carries the distinct set of service_names in scope so the service
// layer can merge only relevant SpanLatencyService sketches.
type aiSummaryRow struct {
	TotalCalls        uint64
	ErrorCalls        uint64
	LatencyMsSum      float64
	LatencyMsCount    uint64
	AvgLatencyMs      float64
	P50LatencyMs      float64
	P95LatencyMs      float64
	P99LatencyMs      float64
	Services          []string
	TotalInputTokens  float64
	TotalOutputTokens float64
}

// aiSummaryTotalsRow is the CH scan target for the totals leg of the
// summary scan. latency_ms_count == total_calls, so we don't project it.
type aiSummaryTotalsRow struct {
	TotalCalls        uint64  `ch:"total_calls"`
	LatencyMsSum      float64 `ch:"latency_ms_sum"`
	TotalInputTokens  float64 `ch:"total_input_tokens"`
	TotalOutputTokens float64 `ch:"total_output_tokens"`
}

// aiCountRow is a 1-row/1-column count result used by error-leg scans and
// pagination totals.
type aiCountRow struct {
	Total uint64 `ch:"total"`
}

// aiServiceRow carries a single distinct service_name row from the services
// leg of the summary scan.
type aiServiceRow struct {
	ServiceName string `ch:"service_name"`
}

// aiFacetRow represents a single facet bucket returned by the UNION ALL facet query.
type aiFacetRow struct {
	FacetKey   string `ch:"facet_key"`
	FacetValue string `ch:"facet_value"`
	Count      uint64 `ch:"count"`
}

// aiTrendRow represents one time bucket in the trend timeseries after the
// totals + errors legs have been merged Go-side.
// AvgLatencyMs is computed in the service from LatencyMsSum/LatencyMsCount.
type aiTrendRow struct {
	TimeBucket     string
	TotalCalls     uint64
	ErrorCalls     uint64
	LatencyMsSum   float64
	LatencyMsCount uint64
	AvgLatencyMs   float64
	TotalTokens    float64
}

// aiTrendTotalsRow is the CH scan target for the totals leg of the trend.
type aiTrendTotalsRow struct {
	TimeBucket   string  `ch:"time_bucket"`
	TotalCalls   uint64  `ch:"total_calls"`
	LatencyMsSum float64 `ch:"latency_ms_sum"`
	TotalTokens  float64 `ch:"total_tokens"`
}

// aiTrendErrorRow is the CH scan target for the error leg of the trend.
type aiTrendErrorRow struct {
	TimeBucket string `ch:"time_bucket"`
	ErrorCalls uint64 `ch:"error_calls"`
}

// aiSessionRawRow is one GenAI span row used for Go-side session aggregation.
// Tokens and session-id candidates stay raw strings; the service parses the
// non-empty-first session id and sums tokens.
type aiSessionRawRow struct {
	SessionIDPrimary   string    `ch:"session_id_primary"`
	SessionIDSecondary string    `ch:"session_id_secondary"`
	SessionIDTertiary  string    `ch:"session_id_tertiary"`
	TraceID            string    `ch:"trace_id"`
	StartTime          time.Time `ch:"start_time"`
	Status             string    `ch:"status"`
	HasError           bool      `ch:"has_error"`
	InputTokensRaw     string    `ch:"input_tokens_raw"`
	OutputTokensRaw    string    `ch:"output_tokens_raw"`
	AIRequestModel     string    `ch:"ai_request_model"`
	ServiceName        string    `ch:"service_name"`
}

// aiSessionRow is one grouped session/conversation bucket from GenAI spans,
// built Go-side by aggregating aiSessionRawRow values.
type aiSessionRow struct {
	SessionID         string
	GenerationCount   uint64
	TraceCount        uint64
	FirstStart        time.Time
	LastStart         time.Time
	TotalInputTokens  float64
	TotalOutputTokens float64
	ErrorCount        uint64
	DominantModel     string
	DominantService   string
}
