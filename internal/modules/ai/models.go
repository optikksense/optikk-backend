package ai

// AISummary represents an aggregate performance/cost/security summary for all AI models.
type AISummary struct {
	TotalRequests      int64   `json:"total_requests"`
	AvgQps             float64 `json:"avg_qps"`
	AvgLatencyMs       float64 `json:"avg_latency_ms"`
	P95LatencyMs       float64 `json:"p95_latency_ms"`
	TimeoutCount       int64   `json:"timeout_count"`
	ErrorCount         int64   `json:"error_count"`
	TotalTokens        int64   `json:"total_tokens"`
	TotalCostUsd       float64 `json:"total_cost_usd"`
	AvgCostPerQuery    float64 `json:"avg_cost_per_query"`
	CacheHitRate       float64 `json:"cache_hit_rate"`
	PiiDetectionRate   float64 `json:"pii_detection_rate"`
	GuardrailBlockRate float64 `json:"guardrail_block_rate"`
	AvgTokensPerSec    float64 `json:"avg_tokens_per_sec"`
	ActiveModels       int64   `json:"active_models"`
}

// AIModel represents a distinct AI model active in the time window.
type AIModel struct {
	ModelName     string `json:"model_name"`
	ModelProvider string `json:"model_provider"`
}

// AIPerformanceMetric represents per-model latency, throughput, error and timeout rates.
type AIPerformanceMetric struct {
	ModelName       string  `json:"model_name"`
	ModelProvider   string  `json:"model_provider"`
	RequestType     string  `json:"request_type"`
	TotalRequests   int64   `json:"total_requests"`
	AvgQps          float64 `json:"avg_qps"`
	AvgLatencyMs    float64 `json:"avg_latency_ms"`
	P50LatencyMs    float64 `json:"p50_latency_ms"`
	P95LatencyMs    float64 `json:"p95_latency_ms"`
	P99LatencyMs    float64 `json:"p99_latency_ms"`
	MaxLatencyMs    float64 `json:"max_latency_ms"`
	TimeoutCount    int64   `json:"timeout_count"`
	ErrorCount      int64   `json:"error_count"`
	TimeoutRate     float64 `json:"timeout_rate"`
	ErrorRate       float64 `json:"error_rate"`
	AvgTokensPerSec float64 `json:"avg_tokens_per_sec"`
	AvgRetryCount   float64 `json:"avg_retry_count"`
}

// AIPerformanceTimeSeries represents per-model latency / throughput time series.
type AIPerformanceTimeSeries struct {
	ModelName    string  `json:"model_name"`
	Timestamp    string  `json:"timestamp"`
	RequestCount int64   `json:"request_count"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
	TimeoutCount int64   `json:"timeout_count"`
	ErrorCount   int64   `json:"error_count"`
	TokensPerSec float64 `json:"tokens_per_sec"`
}

// AILatencyHistogram represents latency distribution (100ms buckets) per model.
type AILatencyHistogram struct {
	ModelName    string `json:"model_name"`
	BucketMs     int64  `json:"bucket_ms"`
	RequestCount int64  `json:"request_count"`
}

// AICostMetric represents per-model token usage and cost breakdown.
type AICostMetric struct {
	ModelName             string  `json:"model_name"`
	ModelProvider         string  `json:"model_provider"`
	TotalRequests         int64   `json:"total_requests"`
	TotalCostUsd          float64 `json:"total_cost_usd"`
	AvgCostPerQuery       float64 `json:"avg_cost_per_query"`
	MaxCostPerQuery       float64 `json:"max_cost_per_query"`
	TotalPromptTokens     int64   `json:"total_prompt_tokens"`
	TotalCompletionTokens int64   `json:"total_completion_tokens"`
	TotalTokens           int64   `json:"total_tokens"`
	AvgPromptTokens       float64 `json:"avg_prompt_tokens"`
	AvgCompletionTokens   float64 `json:"avg_completion_tokens"`
	CacheHitRate          float64 `json:"cache_hit_rate"`
	TotalCacheTokens      int64   `json:"total_cache_tokens"`
}

// AICostTimeSeries represents cost and token usage over time per model.
type AICostTimeSeries struct {
	ModelName        string  `json:"model_name"`
	Timestamp        string  `json:"timestamp"`
	CostPerInterval  float64 `json:"cost_per_interval"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	RequestCount     int64   `json:"request_count"`
}

// AITokenBreakdown represents token type breakdown per model.
type AITokenBreakdown struct {
	ModelName        string `json:"model_name"`
	PromptTokens     int64  `json:"prompt_tokens"`
	CompletionTokens int64  `json:"completion_tokens"`
	SystemTokens     int64  `json:"system_tokens"`
	CacheTokens      int64  `json:"cache_tokens"`
}

// AISecurityMetric represents PII detection and guardrail block rates per model.
type AISecurityMetric struct {
	ModelName             string  `json:"model_name"`
	ModelProvider         string  `json:"model_provider"`
	TotalRequests         int64   `json:"total_requests"`
	PiiDetectedCount      int64   `json:"pii_detected_count"`
	PiiDetectionRate      float64 `json:"pii_detection_rate"`
	GuardrailBlockedCount int64   `json:"guardrail_blocked_count"`
	GuardrailBlockRate    float64 `json:"guardrail_block_rate"`
	ContentPolicyCount    int64   `json:"content_policy_count"`
	ContentPolicyRate     float64 `json:"content_policy_rate"`
}

// AISecurityTimeSeries represents security-event time series per model.
type AISecurityTimeSeries struct {
	ModelName          string `json:"model_name"`
	Timestamp          string `json:"timestamp"`
	TotalRequests      int64  `json:"total_requests"`
	PiiCount           int64  `json:"pii_count"`
	GuardrailCount     int64  `json:"guardrail_count"`
	ContentPolicyCount int64  `json:"content_policy_count"`
}

// AIPiiCategory represents PII category breakdown for detected events.
type AIPiiCategory struct {
	ModelName      string `json:"model_name"`
	PiiCategories  string `json:"pii_categories"`
	DetectionCount int64  `json:"detection_count"`
}
