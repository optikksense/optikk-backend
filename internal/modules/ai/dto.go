package ai

import "time"

type QueryRequest struct {
	StartTime int64  `json:"startTime"`
	EndTime   int64  `json:"endTime"`
	Query     string `json:"query"`
	Limit     int    `json:"limit"`
	Offset    int    `json:"offset"`
	Step      string `json:"step"`
	OrderBy   string `json:"orderBy,omitempty"`
	OrderDir  string `json:"orderDir,omitempty"`
}

type FacetBucket struct {
	Value string `json:"value"`
	Count uint64 `json:"count"`
}

type AIPageInfo struct {
	Total   uint64 `json:"total"`
	Offset  int    `json:"offset"`
	Limit   int    `json:"limit"`
	HasMore bool   `json:"hasMore"`
}

type AIOverview struct {
	TotalRuns        uint64  `json:"total_runs" ch:"total_runs"`
	ErrorRuns        uint64  `json:"error_runs" ch:"error_runs"`
	ErrorRatePct     float64 `json:"error_rate_pct"`
	AvgLatencyMs     float64 `json:"avg_latency_ms" ch:"avg_latency_ms"`
	P95LatencyMs     float64 `json:"p95_latency_ms" ch:"p95_latency_ms"`
	AvgTTFTMs        float64 `json:"avg_ttft_ms" ch:"avg_ttft_ms"`
	TotalTokens      float64 `json:"total_tokens" ch:"total_tokens"`
	TotalCostUSD     float64 `json:"total_cost_usd" ch:"total_cost_usd"`
	AvgQualityScore  float64 `json:"avg_quality_score" ch:"avg_quality_score"`
	ProviderCount    uint64  `json:"provider_count" ch:"provider_count"`
	ModelCount       uint64  `json:"model_count" ch:"model_count"`
	PromptCount      uint64  `json:"prompt_count" ch:"prompt_count"`
	GuardrailBlocked uint64  `json:"guardrail_blocked" ch:"guardrail_blocked"`
}

type AITrendPoint struct {
	TimeBucket       time.Time `json:"time_bucket" ch:"time_bucket"`
	Requests         uint64    `json:"requests" ch:"requests"`
	ErrorRuns        uint64    `json:"error_runs" ch:"error_runs"`
	ErrorRatePct     float64   `json:"error_rate_pct"`
	P95LatencyMs     float64   `json:"p95_latency_ms" ch:"p95_latency_ms"`
	AvgTTFTMs        float64   `json:"avg_ttft_ms" ch:"avg_ttft_ms"`
	TotalTokens      float64   `json:"total_tokens" ch:"total_tokens"`
	TotalCostUSD     float64   `json:"total_cost_usd" ch:"total_cost_usd"`
	AvgQualityScore  float64   `json:"avg_quality_score" ch:"avg_quality_score"`
	GuardrailBlocked uint64    `json:"guardrail_blocked" ch:"guardrail_blocked"`
}

type AIModelBreakdown struct {
	Provider        string  `json:"provider" ch:"provider"`
	RequestModel    string  `json:"request_model" ch:"request_model"`
	Requests        uint64  `json:"requests" ch:"requests"`
	ErrorRuns       uint64  `json:"error_runs" ch:"error_runs"`
	ErrorRatePct    float64 `json:"error_rate_pct"`
	AvgLatencyMs    float64 `json:"avg_latency_ms" ch:"avg_latency_ms"`
	AvgTTFTMs       float64 `json:"avg_ttft_ms" ch:"avg_ttft_ms"`
	TotalTokens     float64 `json:"total_tokens" ch:"total_tokens"`
	TotalCostUSD    float64 `json:"total_cost_usd" ch:"total_cost_usd"`
	AvgQualityScore float64 `json:"avg_quality_score" ch:"avg_quality_score"`
}

type AIPromptBreakdown struct {
	PromptTemplate        string  `json:"prompt_template" ch:"prompt_template"`
	PromptTemplateVersion string  `json:"prompt_template_version" ch:"prompt_template_version"`
	Requests              uint64  `json:"requests" ch:"requests"`
	ErrorRatePct          float64 `json:"error_rate_pct"`
	AvgLatencyMs          float64 `json:"avg_latency_ms" ch:"avg_latency_ms"`
	AvgQualityScore       float64 `json:"avg_quality_score" ch:"avg_quality_score"`
	TotalCostUSD          float64 `json:"total_cost_usd" ch:"total_cost_usd"`
}

type AIQualityBucket struct {
	Bucket string `json:"bucket" ch:"bucket"`
	Count  uint64 `json:"count" ch:"count"`
}

type AIGuardrailBreakdown struct {
	GuardrailState string `json:"guardrail_state" ch:"guardrail_state"`
	Count          uint64 `json:"count" ch:"count"`
}

type AIQualitySummary struct {
	AvgQualityScore    float64                `json:"avg_quality_score"`
	AvgFeedbackScore   float64                `json:"avg_feedback_score"`
	ScoredRuns         uint64                 `json:"scored_runs"`
	FeedbackRuns       uint64                 `json:"feedback_runs"`
	GuardrailBlocks    uint64                 `json:"guardrail_blocks"`
	GuardrailBreakdown []AIGuardrailBreakdown `json:"guardrail_breakdown"`
	QualityBuckets     []AIQualityBucket      `json:"quality_buckets"`
}

type AIRunRow struct {
	RunID                 string    `json:"run_id" ch:"run_id"`
	TraceID               string    `json:"trace_id" ch:"trace_id"`
	SpanID                string    `json:"span_id" ch:"span_id"`
	ServiceName           string    `json:"service_name" ch:"service_name"`
	Provider              string    `json:"provider" ch:"provider"`
	RequestModel          string    `json:"request_model" ch:"request_model"`
	ResponseModel         string    `json:"response_model" ch:"response_model"`
	Operation             string    `json:"operation" ch:"operation"`
	PromptTemplate        string    `json:"prompt_template" ch:"prompt_template"`
	PromptTemplateVersion string    `json:"prompt_template_version" ch:"prompt_template_version"`
	ConversationID        string    `json:"conversation_id" ch:"conversation_id"`
	SessionID             string    `json:"session_id" ch:"session_id"`
	FinishReason          string    `json:"finish_reason" ch:"finish_reason"`
	GuardrailState        string    `json:"guardrail_state" ch:"guardrail_state"`
	Status                string    `json:"status" ch:"status"`
	StatusMessage         string    `json:"status_message" ch:"status_message"`
	HasError              bool      `json:"has_error" ch:"has_error"`
	StartTime             time.Time `json:"start_time" ch:"start_time"`
	LatencyMs             float64   `json:"latency_ms" ch:"latency_ms"`
	TTFTMs                float64   `json:"ttft_ms" ch:"ttft_ms"`
	InputTokens           float64   `json:"input_tokens" ch:"input_tokens"`
	OutputTokens          float64   `json:"output_tokens" ch:"output_tokens"`
	TotalTokens           float64   `json:"total_tokens" ch:"total_tokens"`
	CostUSD               float64   `json:"cost_usd" ch:"cost_usd"`
	QualityScore          float64   `json:"quality_score" ch:"quality_score"`
	FeedbackScore         float64   `json:"feedback_score" ch:"feedback_score"`
	QualityBucket         string    `json:"quality_bucket" ch:"quality_bucket"`
}

type AIExplorerFacets struct {
	Provider       []FacetBucket `json:"provider"`
	RequestModel   []FacetBucket `json:"request_model"`
	Operation      []FacetBucket `json:"operation"`
	ServiceName    []FacetBucket `json:"service_name"`
	PromptTemplate []FacetBucket `json:"prompt_template"`
	FinishReason   []FacetBucket `json:"finish_reason"`
	GuardrailState []FacetBucket `json:"guardrail_state"`
	QualityBucket  []FacetBucket `json:"quality_bucket"`
	Status         []FacetBucket `json:"status"`
}

type AICorrelations struct {
	TopModels  []FacetBucket `json:"top_models,omitempty"`
	TopPrompts []FacetBucket `json:"top_prompts,omitempty"`
}

type AIExplorerResponse struct {
	Results      []AIRunRow       `json:"results"`
	Summary      AIOverview       `json:"summary"`
	Facets       AIExplorerFacets `json:"facets"`
	Trend        []AITrendPoint   `json:"trend"`
	PageInfo     AIPageInfo       `json:"pageInfo"`
	Correlations AICorrelations   `json:"correlations,omitempty"`
}

type AIRunDetail struct {
	AIRunRow
	SpanName        string            `json:"span_name" ch:"span_name"`
	ServiceVersion  string            `json:"service_version" ch:"service_version"`
	Environment     string            `json:"environment" ch:"environment"`
	PromptSnippet   string            `json:"prompt_snippet" ch:"prompt_snippet"`
	ResponseSnippet string            `json:"response_snippet" ch:"response_snippet"`
	CacheHit        bool              `json:"cache_hit" ch:"cache_hit"`
	RawAttributes   map[string]string `json:"raw_attributes" ch:"attributes"`
}

type AIRelatedLog struct {
	Timestamp    time.Time `json:"timestamp"`
	SeverityText string    `json:"severity_text"`
	Body         string    `json:"body"`
	ServiceName  string    `json:"service_name"`
	TraceID      string    `json:"trace_id"`
	SpanID       string    `json:"span_id"`
}

type AIAlertTargetRef struct {
	AlertID       int64          `json:"alert_id"`
	RuleName      string         `json:"rule_name"`
	ConditionType string         `json:"condition_type"`
	RuleState     string         `json:"rule_state"`
	TargetRef     map[string]any `json:"target_ref,omitempty"`
}

type AIRunRelated struct {
	RunID          string             `json:"run_id"`
	TraceID        string             `json:"trace_id"`
	SpanID         string             `json:"span_id"`
	ServiceName    string             `json:"service_name"`
	ServiceVersion string             `json:"service_version"`
	Environment    string             `json:"environment"`
	Logs           []AIRelatedLog     `json:"logs"`
	Alerts         []AIAlertTargetRef `json:"alerts"`
}
