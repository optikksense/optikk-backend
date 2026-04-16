package overview

// AIModelBreakdown is one row of the top-models breakdown.
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

// AIPromptBreakdown is one row of the top-prompts breakdown.
type AIPromptBreakdown struct {
	PromptTemplate        string  `json:"prompt_template" ch:"prompt_template"`
	PromptTemplateVersion string  `json:"prompt_template_version" ch:"prompt_template_version"`
	Requests              uint64  `json:"requests" ch:"requests"`
	ErrorRatePct          float64 `json:"error_rate_pct"`
	AvgLatencyMs          float64 `json:"avg_latency_ms" ch:"avg_latency_ms"`
	AvgQualityScore       float64 `json:"avg_quality_score" ch:"avg_quality_score"`
	TotalCostUSD          float64 `json:"total_cost_usd" ch:"total_cost_usd"`
}

// AIQualityBucket is one bucket in the quality score distribution.
type AIQualityBucket struct {
	Bucket string `json:"bucket" ch:"bucket"`
	Count  uint64 `json:"count" ch:"count"`
}

// AIGuardrailBreakdown is one bucket in the guardrail status distribution.
type AIGuardrailBreakdown struct {
	GuardrailState string `json:"guardrail_state" ch:"guardrail_state"`
	Count          uint64 `json:"count" ch:"count"`
}

// AIQualitySummary is the response for the quality overview endpoint.
type AIQualitySummary struct {
	AvgQualityScore    float64                `json:"avg_quality_score"`
	AvgFeedbackScore   float64                `json:"avg_feedback_score"`
	ScoredRuns         uint64                 `json:"scored_runs"`
	FeedbackRuns       uint64                 `json:"feedback_runs"`
	GuardrailBlocks    uint64                 `json:"guardrail_blocks"`
	GuardrailBreakdown []AIGuardrailBreakdown `json:"guardrail_breakdown"`
	QualityBuckets     []AIQualityBucket      `json:"quality_buckets"`
}
