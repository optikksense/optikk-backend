package analytics

import "time"

// -------- API response DTOs --------

// --- Model Catalog ---

type ModelCatalogDTO struct {
	Model        string  `json:"model"`
	Provider     string  `json:"provider"`
	RequestCount int64   `json:"requestCount"`
	AvgLatencyMs float64 `json:"avgLatencyMs"`
	P50Ms        float64 `json:"p50Ms"`
	P95Ms        float64 `json:"p95Ms"`
	P99Ms        float64 `json:"p99Ms"`
	ErrorRate    float64 `json:"errorRate"`
	InputTokens  int64   `json:"inputTokens"`
	OutputTokens int64   `json:"outputTokens"`
	TotalTokens  int64   `json:"totalTokens"`
	TokensPerSec float64 `json:"tokensPerSec"`
	TopOps       string  `json:"topOperations"`
	TopSvcs      string  `json:"topServices"`
	EstCost      float64 `json:"estimatedCost"`
	Health       string  `json:"health"`
}

type LatencyBucketDTO struct {
	Model    string `json:"model"`
	BucketMs int64  `json:"bucketMs"`
	Count    int64  `json:"count"`
}

type ParamImpactDTO struct {
	Temperature float64 `json:"temperature"`
	AvgLatency  float64 `json:"avgLatency"`
	ErrorRate   float64 `json:"errorRate"`
	Count       int64   `json:"count"`
}

type ModelTimeseriesDTO struct {
	Timestamp    time.Time `json:"timestamp"`
	RequestCount int64     `json:"requestCount"`
	AvgLatencyMs float64   `json:"avgLatencyMs"`
	P95Ms        float64   `json:"p95Ms"`
	ErrorRate    float64   `json:"errorRate"`
	InputTokens  int64     `json:"inputTokens"`
	OutputTokens int64     `json:"outputTokens"`
}

// --- Cost ---

type CostSummaryDTO struct {
	TotalEstCost float64        `json:"totalEstimatedCost"`
	CostPerReq   float64        `json:"costPerRequest"`
	TotalInput   int64          `json:"totalInputTokens"`
	TotalOutput  int64          `json:"totalOutputTokens"`
	ByModel      []ModelCostDTO `json:"byModel"`
}

type ModelCostDTO struct {
	Model        string  `json:"model"`
	InputTokens  int64   `json:"inputTokens"`
	OutputTokens int64   `json:"outputTokens"`
	EstCost      float64 `json:"estimatedCost"`
}

type CostTimeseriesPointDTO struct {
	Timestamp time.Time `json:"timestamp"`
	Model     string    `json:"model"`
	EstCost   float64   `json:"estimatedCost"`
}

type TokenEconomicsDTO struct {
	TotalInput     int64   `json:"totalInput"`
	TotalOutput    int64   `json:"totalOutput"`
	InputOutputRat float64 `json:"inputOutputRatio"`
	AvgPerReq      float64 `json:"avgTokensPerRequest"`
	RequestCount   int64   `json:"requestCount"`
}

// --- Errors ---

type ErrorPatternDTO struct {
	Model         string    `json:"model"`
	Operation     string    `json:"operation"`
	StatusMessage string    `json:"statusMessage"`
	ErrorCount    int64     `json:"errorCount"`
	FirstSeen     time.Time `json:"firstSeen"`
	LastSeen      time.Time `json:"lastSeen"`
}

type ErrorTimeseriesPointDTO struct {
	Timestamp  time.Time `json:"timestamp"`
	Model      string    `json:"model"`
	ErrorCount int64     `json:"errorCount"`
	ErrorRate  float64   `json:"errorRate"`
}

type FinishReasonTrendDTO struct {
	Timestamp    time.Time `json:"timestamp"`
	FinishReason string    `json:"finishReason"`
	Count        int64     `json:"count"`
}

// --- Conversations ---

type ConversationDTO struct {
	ConversationID string    `json:"conversationId"`
	ServiceName    string    `json:"serviceName"`
	Model          string    `json:"model"`
	TurnCount      int64     `json:"turnCount"`
	TotalTokens    int64     `json:"totalTokens"`
	HasError       bool      `json:"hasError"`
	FirstTurn      time.Time `json:"firstTurn"`
	LastTurn       time.Time `json:"lastTurn"`
}

type ConversationTurnDTO struct {
	SpanID       string    `json:"spanId"`
	Timestamp    time.Time `json:"timestamp"`
	Model        string    `json:"model"`
	DurationMs   float64   `json:"durationMs"`
	InputTokens  int64     `json:"inputTokens"`
	OutputTokens int64     `json:"outputTokens"`
	TotalTokens  int64     `json:"totalTokens"`
	HasError     bool      `json:"hasError"`
	FinishReason string    `json:"finishReason"`
}

type ConversationSummaryDTO struct {
	TurnCount   int64     `json:"turnCount"`
	TotalTokens int64     `json:"totalTokens"`
	TotalMs     float64   `json:"totalMs"`
	Models      string    `json:"models"`
	ErrorTurns  int64     `json:"errorTurns"`
	FirstTurn   time.Time `json:"firstTurn"`
	LastTurn    time.Time `json:"lastTurn"`
}

// -------- Filter params --------

type AnalyticsFilter struct {
	TeamID    int64
	StartMs   int64
	EndMs     int64
	Service   string
	Model     string
	Provider  string
	Operation string
}
