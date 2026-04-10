package explorer

import "time"

// -------- DB scan models --------

type spanRow struct {
	SpanID        string    `ch:"span_id"`
	TraceID       string    `ch:"trace_id"`
	ParentSpanID  string    `ch:"parent_span_id"`
	ServiceName   string    `ch:"service_name"`
	OperationName string    `ch:"operation_name"`
	Model         string    `ch:"model"`
	Provider      string    `ch:"provider"`
	OperationType string    `ch:"operation_type"`
	Timestamp     time.Time `ch:"timestamp"`
	DurationMs    float64   `ch:"duration_ms"`
	InputTokens   int64     `ch:"input_tokens"`
	OutputTokens  int64     `ch:"output_tokens"`
	HasError      bool      `ch:"has_error"`
	StatusMessage string    `ch:"status_message"`
	FinishReason  string    `ch:"finish_reason"`
	Temperature   float64   `ch:"temperature"`
}

type facetRow struct {
	Value string `ch:"value"`
	Count int64  `ch:"count"`
}

type explorerSummaryRow struct {
	TotalSpans   int64   `ch:"total_spans"`
	ErrorCount   int64   `ch:"error_count"`
	AvgLatencyMs float64 `ch:"avg_latency_ms"`
	P95Ms        float64 `ch:"p95_ms"`
	TotalTokens  int64   `ch:"total_tokens"`
	UniqueModels int64   `ch:"unique_models"`
}

type histogramRow struct {
	Timestamp time.Time `ch:"timestamp"`
	Count     int64     `ch:"count"`
}
