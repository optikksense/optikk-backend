package overview

import "time"

// -------- API response DTOs --------

type SummaryDTO struct {
	TotalRequests   int64   `json:"totalRequests"`
	ErrorCount      int64   `json:"errorCount"`
	ErrorRate       float64 `json:"errorRate"`
	AvgLatencyMs    float64 `json:"avgLatencyMs"`
	P50Ms           float64 `json:"p50Ms"`
	P95Ms           float64 `json:"p95Ms"`
	P99Ms           float64 `json:"p99Ms"`
	TotalInputTok   int64   `json:"totalInputTokens"`
	TotalOutputTok  int64   `json:"totalOutputTokens"`
	TotalTokens     int64   `json:"totalTokens"`
	AvgTokensPerSec float64 `json:"avgTokensPerSec"`
	UniqueModels    int64   `json:"uniqueModels"`
	UniqueServices  int64   `json:"uniqueServices"`
	UniqueOps       int64   `json:"uniqueOperations"`
}

type TimeseriesPointDTO struct {
	Timestamp time.Time `json:"timestamp"`
	Series    string    `json:"series"`
	Value     float64   `json:"value"`
}

type TimeseriesDualPointDTO struct {
	Timestamp time.Time `json:"timestamp"`
	Series    string    `json:"series"`
	Value1    float64   `json:"value1"`
	Value2    float64   `json:"value2"`
}

type ModelDTO struct {
	Model        string  `json:"model"`
	Provider     string  `json:"provider"`
	RequestCount int64   `json:"requestCount"`
	AvgLatencyMs float64 `json:"avgLatencyMs"`
	P95Ms        float64 `json:"p95Ms"`
	ErrorCount   int64   `json:"errorCount"`
	ErrorRate    float64 `json:"errorRate"`
	InputTokens  int64   `json:"inputTokens"`
	OutputTokens int64   `json:"outputTokens"`
	TotalTokens  int64   `json:"totalTokens"`
	TokensPerSec float64 `json:"tokensPerSec"`
}

type OperationDTO struct {
	Operation    string  `json:"operation"`
	RequestCount int64   `json:"requestCount"`
	AvgLatencyMs float64 `json:"avgLatencyMs"`
	ErrorCount   int64   `json:"errorCount"`
	ErrorRate    float64 `json:"errorRate"`
	InputTokens  int64   `json:"inputTokens"`
	OutputTokens int64   `json:"outputTokens"`
	TotalTokens  int64   `json:"totalTokens"`
}

type ServiceDTO struct {
	ServiceName  string `json:"serviceName"`
	RequestCount int64  `json:"requestCount"`
	Models       string `json:"models"`
	ErrorCount   int64  `json:"errorCount"`
}

type ModelHealthDTO struct {
	Model        string  `json:"model"`
	Provider     string  `json:"provider"`
	RequestCount int64   `json:"requestCount"`
	AvgLatencyMs float64 `json:"avgLatencyMs"`
	P95Ms        float64 `json:"p95Ms"`
	ErrorRate    float64 `json:"errorRate"`
	Health       string  `json:"health"` // "healthy", "degraded", "critical"
}

type TopSlowDTO struct {
	Model        string  `json:"model"`
	Operation    string  `json:"operation"`
	P95Ms        float64 `json:"p95Ms"`
	RequestCount int64   `json:"requestCount"`
}

type TopErrorDTO struct {
	Model        string  `json:"model"`
	Operation    string  `json:"operation"`
	ErrorCount   int64   `json:"errorCount"`
	ErrorRate    float64 `json:"errorRate"`
	RequestCount int64   `json:"requestCount"`
}

type FinishReasonDTO struct {
	FinishReason string  `json:"finishReason"`
	Count        int64   `json:"count"`
	Percentage   float64 `json:"percentage"`
}

// -------- Filter params --------

type OverviewFilter struct {
	TeamID    int64
	StartMs   int64
	EndMs     int64
	Service   string
	Model     string
	Operation string
	Provider  string
}
