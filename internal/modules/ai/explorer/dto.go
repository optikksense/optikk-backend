package explorer

import "time"

// QueryRequest is the request body for POST /ai/explorer/query.
type QueryRequest struct {
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
	Query     string `json:"query"`
	Limit     int    `json:"limit"`
	Offset    int    `json:"offset"`
	Step      string `json:"step"`
}

// AICall is a single LLM call record returned to the frontend.
type AICall struct {
	SpanID        string    `json:"span_id"`
	TraceID       string    `json:"trace_id"`
	ServiceName   string    `json:"service_name"`
	OperationName string    `json:"operation_name"`
	StartTime     time.Time `json:"start_time"`
	DurationMs    float64   `json:"duration_ms"`
	Status        string    `json:"status"`
	StatusMessage string    `json:"status_message,omitempty"`

	AISystem        string  `json:"ai_system"`
	AIRequestModel  string  `json:"ai_request_model"`
	AIResponseModel string  `json:"ai_response_model,omitempty"`
	AIOperation     string  `json:"ai_operation"`
	InputTokens     float64 `json:"input_tokens"`
	OutputTokens    float64 `json:"output_tokens"`
	TotalTokens     float64 `json:"total_tokens"`
	Temperature     string  `json:"temperature,omitempty"`
	MaxTokens       string  `json:"max_tokens,omitempty"`
	FinishReason    string  `json:"finish_reason,omitempty"`
	ErrorType       string  `json:"error_type,omitempty"`
}

// AISummary holds aggregated statistics for the query window.
type AISummary struct {
	TotalCalls        uint64  `json:"total_calls"`
	ErrorCalls        uint64  `json:"error_calls"`
	AvgLatencyMs      float64 `json:"avg_latency_ms"`
	P50LatencyMs      float64 `json:"p50_latency_ms"`
	P95LatencyMs      float64 `json:"p95_latency_ms"`
	P99LatencyMs      float64 `json:"p99_latency_ms"`
	TotalInputTokens  float64 `json:"total_input_tokens"`
	TotalOutputTokens float64 `json:"total_output_tokens"`
}

// FacetBucket is a single value+count pair within a facet group.
type FacetBucket struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
}

// AIExplorerFacets holds all facet groups.
type AIExplorerFacets struct {
	AISystem         []FacetBucket `json:"ai_system"`
	AIModel          []FacetBucket `json:"ai_model"`
	AIOperation      []FacetBucket `json:"ai_operation"`
	ServiceName      []FacetBucket `json:"service_name"`
	Status           []FacetBucket `json:"status"`
	FinishReason     []FacetBucket `json:"finish_reason"`
	PromptTemplate   []FacetBucket `json:"prompt_template"`
}

// AITrendBucket is one time-bucketed data point for the trend timeseries.
type AITrendBucket struct {
	TimeBucket   string  `json:"time_bucket"`
	TotalCalls   uint64  `json:"total_calls"`
	ErrorCalls   uint64  `json:"error_calls"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	TotalTokens  float64 `json:"total_tokens"`
}

// PageInfo provides pagination metadata.
type PageInfo struct {
	Total  uint64 `json:"total"`
	Offset int    `json:"offset"`
	Limit  int    `json:"limit"`
}

// Response is the top-level response for the AI explorer query.
type Response struct {
	Results  []AICall         `json:"results"`
	Summary  AISummary        `json:"summary"`
	Facets   AIExplorerFacets `json:"facets"`
	Trend    []AITrendBucket  `json:"trend"`
	PageInfo PageInfo         `json:"pageInfo"`
}

// SessionsQueryRequest is the body for POST /ai/explorer/sessions/query.
type SessionsQueryRequest struct {
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
	Query     string `json:"query"`
	Limit     int    `json:"limit"`
	Offset    int    `json:"offset"`
}

// SessionRow is one aggregated session/conversation in the time window.
type SessionRow struct {
	SessionID         string    `json:"session_id"`
	GenerationCount   uint64    `json:"generation_count"`
	TraceCount        uint64    `json:"trace_count"`
	FirstStart        time.Time `json:"first_start"`
	LastStart         time.Time `json:"last_start"`
	TotalInputTokens  float64   `json:"total_input_tokens"`
	TotalOutputTokens float64   `json:"total_output_tokens"`
	ErrorCount        uint64    `json:"error_count"`
	DominantModel     string    `json:"dominant_model"`
	DominantService   string    `json:"dominant_service"`
}

// SessionsResponse is returned by POST /ai/explorer/sessions/query.
type SessionsResponse struct {
	Results  []SessionRow `json:"results"`
	PageInfo PageInfo     `json:"pageInfo"`
}
