package analytics

import "time"

// -------- DB scan models --------

type modelCatalogRow struct {
	Model        string  `ch:"model"`
	Provider     string  `ch:"provider"`
	RequestCount int64   `ch:"request_count"`
	AvgLatencyMs float64 `ch:"avg_latency_ms"`
	P50Ms        float64 `ch:"p50_ms"`
	P95Ms        float64 `ch:"p95_ms"`
	P99Ms        float64 `ch:"p99_ms"`
	ErrorRate    float64 `ch:"error_rate"`
	InputTokens  int64   `ch:"input_tokens"`
	OutputTokens int64   `ch:"output_tokens"`
	TopOps       string  `ch:"top_ops"`
	TopSvcs      string  `ch:"top_svcs"`
}

type latencyBucketRow struct {
	Model     string `ch:"model"`
	BucketMs  int64  `ch:"bucket_ms"`
	Count     int64  `ch:"count"`
}

type paramImpactRow struct {
	Temperature float64 `ch:"temperature"`
	AvgLatency  float64 `ch:"avg_latency"`
	ErrorRate   float64 `ch:"error_rate"`
	Count       int64   `ch:"count"`
}

type costRow struct {
	Model        string `ch:"model"`
	InputTokens  int64  `ch:"input_tokens"`
	OutputTokens int64  `ch:"output_tokens"`
}

type costTimeseriesRow struct {
	Timestamp    time.Time `ch:"timestamp"`
	Model        string    `ch:"model"`
	InputTokens  int64     `ch:"input_tokens"`
	OutputTokens int64     `ch:"output_tokens"`
}

type tokenEconomicsRow struct {
	TotalInput   int64   `ch:"total_input"`
	TotalOutput  int64   `ch:"total_output"`
	AvgPerReq    float64 `ch:"avg_per_req"`
	RequestCount int64   `ch:"request_count"`
}

type errorPatternRow struct {
	Model         string    `ch:"model"`
	Operation     string    `ch:"operation"`
	StatusMessage string    `ch:"status_message"`
	ErrorCount    int64     `ch:"error_count"`
	FirstSeen     time.Time `ch:"first_seen"`
	LastSeen      time.Time `ch:"last_seen"`
}

type errorTimeseriesRow struct {
	Timestamp  time.Time `ch:"timestamp"`
	Model      string    `ch:"model"`
	ErrorCount int64     `ch:"error_count"`
	ErrorRate  float64   `ch:"error_rate"`
}

type finishReasonTrendRow struct {
	Timestamp    time.Time `ch:"timestamp"`
	FinishReason string    `ch:"finish_reason"`
	Count        int64     `ch:"count"`
}

type conversationListRow struct {
	ConversationID string    `ch:"conversation_id"`
	ServiceName    string    `ch:"service_name"`
	Model          string    `ch:"model"`
	TurnCount      int64     `ch:"turn_count"`
	TotalTokens    int64     `ch:"total_tokens"`
	HasError       bool      `ch:"has_error"`
	FirstTurn      time.Time `ch:"first_turn"`
	LastTurn       time.Time `ch:"last_turn"`
}

type conversationTurnRow struct {
	SpanID       string    `ch:"span_id"`
	Timestamp    time.Time `ch:"timestamp"`
	Model        string    `ch:"model"`
	DurationMs   float64   `ch:"duration_ms"`
	InputTokens  int64     `ch:"input_tokens"`
	OutputTokens int64     `ch:"output_tokens"`
	HasError     bool      `ch:"has_error"`
	FinishReason string    `ch:"finish_reason"`
}

type conversationSummaryRow struct {
	TurnCount   int64     `ch:"turn_count"`
	TotalTokens int64     `ch:"total_tokens"`
	TotalMs     float64   `ch:"total_ms"`
	Models      string    `ch:"models"`
	ErrorTurns  int64     `ch:"error_turns"`
	FirstTurn   time.Time `ch:"first_turn"`
	LastTurn    time.Time `ch:"last_turn"`
}

type modelTimeseriesRow struct {
	Timestamp    time.Time `ch:"timestamp"`
	RequestCount int64     `ch:"request_count"`
	AvgLatencyMs float64   `ch:"avg_latency_ms"`
	P95Ms        float64   `ch:"p95_ms"`
	ErrorRate    float64   `ch:"error_rate"`
	InputTokens  int64     `ch:"input_tokens"`
	OutputTokens int64     `ch:"output_tokens"`
}
