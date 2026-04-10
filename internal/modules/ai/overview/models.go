package overview

import "time"

// -------- DB scan models (unexported) --------

type summaryRow struct {
	TotalCount   int64   `ch:"total_count"`
	ErrorCount   int64   `ch:"error_count"`
	AvgLatencyMs float64 `ch:"avg_latency_ms"`
	P50Ms        float64 `ch:"p50_ms"`
	P95Ms        float64 `ch:"p95_ms"`
	P99Ms        float64 `ch:"p99_ms"`
	TotalInTok   int64   `ch:"total_input_tokens"`
	TotalOutTok  int64   `ch:"total_output_tokens"`
	UniqueModels int64   `ch:"unique_models"`
	UniqueOps    int64   `ch:"unique_operations"`
	UniqueSvcs   int64   `ch:"unique_services"`
}

type modelRow struct {
	Model        string  `ch:"model"`
	Provider     string  `ch:"provider"`
	RequestCount int64   `ch:"request_count"`
	AvgLatencyMs float64 `ch:"avg_latency_ms"`
	P95Ms        float64 `ch:"p95_ms"`
	ErrorCount   int64   `ch:"error_count"`
	ErrorRate    float64 `ch:"error_rate"`
	InputTokens  int64   `ch:"input_tokens"`
	OutputTokens int64   `ch:"output_tokens"`
}

type operationRow struct {
	Operation    string  `ch:"operation"`
	RequestCount int64   `ch:"request_count"`
	AvgLatencyMs float64 `ch:"avg_latency_ms"`
	ErrorCount   int64   `ch:"error_count"`
	ErrorRate    float64 `ch:"error_rate"`
	InputTokens  int64   `ch:"input_tokens"`
	OutputTokens int64   `ch:"output_tokens"`
}

type serviceRow struct {
	ServiceName  string `ch:"service_name"`
	RequestCount int64  `ch:"request_count"`
	Models       string `ch:"models"`
	ErrorCount   int64  `ch:"error_count"`
}

type timeseriesRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Series    string    `ch:"series"`
	Value     float64   `ch:"value"`
}

type timeseriesDualRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Series    string    `ch:"series"`
	Value1    float64   `ch:"value1"`
	Value2    float64   `ch:"value2"`
}

type modelHealthRow struct {
	Model        string  `ch:"model"`
	Provider     string  `ch:"provider"`
	RequestCount int64   `ch:"request_count"`
	AvgLatencyMs float64 `ch:"avg_latency_ms"`
	P95Ms        float64 `ch:"p95_ms"`
	ErrorRate    float64 `ch:"error_rate"`
}

type topSlowRow struct {
	Model        string  `ch:"model"`
	Operation    string  `ch:"operation"`
	P95Ms        float64 `ch:"p95_ms"`
	RequestCount int64   `ch:"request_count"`
}

type topErrorRow struct {
	Model        string  `ch:"model"`
	Operation    string  `ch:"operation"`
	ErrorCount   int64   `ch:"error_count"`
	ErrorRate    float64 `ch:"error_rate"`
	RequestCount int64   `ch:"request_count"`
}

type finishReasonRow struct {
	FinishReason string `ch:"finish_reason"`
	Count        int64  `ch:"count"`
}
