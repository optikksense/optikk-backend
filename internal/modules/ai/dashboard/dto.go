package dashboard

type aiSummaryDTO struct {
	TotalRequests      int64   `ch:"total_requests"`
	AvgQPS             float64 `ch:"avg_qps"`
	AvgLatencyMs       float64 `ch:"avg_latency_ms"`
	P95LatencyMs       float64 `ch:"p95_latency_ms"`
	TimeoutCount       int64   `ch:"timeout_count"`
	ErrorCount         int64   `ch:"error_count"`
	TotalTokens        int64   `ch:"total_tokens"`
	TotalCostUSD       float64 `ch:"total_cost_usd"`
	AvgCostPerQuery    float64 `ch:"avg_cost_per_query"`
	CacheHitRate       float64 `ch:"cache_hit_rate"`
	PIIDetectionRate   float64 `ch:"pii_detection_rate"`
	GuardrailBlockRate float64 `ch:"guardrail_block_rate"`
	AvgTokensPerSec    float64 `ch:"avg_tokens_per_sec"`
	ActiveModels       int64   `ch:"active_models"`
}

func (d aiSummaryDTO) toModel() *AISummary {
	return &AISummary{
		TotalRequests:      d.TotalRequests,
		AvgQps:             d.AvgQPS,
		AvgLatencyMs:       d.AvgLatencyMs,
		P95LatencyMs:       d.P95LatencyMs,
		TimeoutCount:       d.TimeoutCount,
		ErrorCount:         d.ErrorCount,
		TotalTokens:        d.TotalTokens,
		TotalCostUsd:       d.TotalCostUSD,
		AvgCostPerQuery:    d.AvgCostPerQuery,
		CacheHitRate:       d.CacheHitRate,
		PiiDetectionRate:   d.PIIDetectionRate,
		GuardrailBlockRate: d.GuardrailBlockRate,
		AvgTokensPerSec:    d.AvgTokensPerSec,
		ActiveModels:       d.ActiveModels,
	}
}

type aiModelDTO struct {
	ModelName     string `ch:"model_name"`
	ModelProvider string `ch:"model_provider"`
}

func mapAIModelDTOs(rows []aiModelDTO) []AIModel {
	out := make([]AIModel, len(rows))
	for i, row := range rows {
		out[i] = AIModel(row)
	}
	return out
}

type aiPerformanceMetricDTO struct {
	ModelName       string  `ch:"model_name"`
	ModelProvider   string  `ch:"model_provider"`
	RequestType     string  `ch:"request_type"`
	TotalRequests   int64   `ch:"total_requests"`
	AvgQPS          float64 `ch:"avg_qps"`
	AvgLatencyMs    float64 `ch:"avg_latency_ms"`
	P50LatencyMs    float64 `ch:"p50_latency_ms"`
	P95LatencyMs    float64 `ch:"p95_latency_ms"`
	P99LatencyMs    float64 `ch:"p99_latency_ms"`
	MaxLatencyMs    float64 `ch:"max_latency_ms"`
	TimeoutCount    int64   `ch:"timeout_count"`
	ErrorCount      int64   `ch:"error_count"`
	TimeoutRate     float64 `ch:"timeout_rate"`
	ErrorRate       float64 `ch:"error_rate"`
	AvgTokensPerSec float64 `ch:"avg_tokens_per_sec"`
	AvgRetryCount   float64 `ch:"avg_retry_count"`
}

func mapAIPerformanceMetricDTOs(rows []aiPerformanceMetricDTO) []AIPerformanceMetric {
	out := make([]AIPerformanceMetric, len(rows))
	for i, row := range rows {
		out[i] = AIPerformanceMetric{
			ModelName:       row.ModelName,
			ModelProvider:   row.ModelProvider,
			RequestType:     row.RequestType,
			TotalRequests:   row.TotalRequests,
			AvgQps:          row.AvgQPS,
			AvgLatencyMs:    row.AvgLatencyMs,
			P50LatencyMs:    row.P50LatencyMs,
			P95LatencyMs:    row.P95LatencyMs,
			P99LatencyMs:    row.P99LatencyMs,
			MaxLatencyMs:    row.MaxLatencyMs,
			TimeoutCount:    row.TimeoutCount,
			ErrorCount:      row.ErrorCount,
			TimeoutRate:     row.TimeoutRate,
			ErrorRate:       row.ErrorRate,
			AvgTokensPerSec: row.AvgTokensPerSec,
			AvgRetryCount:   row.AvgRetryCount,
		}
	}
	return out
}

type aiPerformanceTimeSeriesDTO struct {
	ModelName    string  `ch:"model_name"`
	TimeBucket   string  `ch:"time_bucket"`
	RequestCount int64   `ch:"request_count"`
	AvgLatencyMs float64 `ch:"avg_latency_ms"`
	P95LatencyMs float64 `ch:"p95_latency_ms"`
	TimeoutCount int64   `ch:"timeout_count"`
	ErrorCount   int64   `ch:"error_count"`
	TokensPerSec float64 `ch:"tokens_per_sec"`
}

func mapAIPerformanceTimeSeriesDTOs(rows []aiPerformanceTimeSeriesDTO) []AIPerformanceTimeSeries {
	out := make([]AIPerformanceTimeSeries, len(rows))
	for i, row := range rows {
		out[i] = AIPerformanceTimeSeries{
			ModelName:    row.ModelName,
			Timestamp:    row.TimeBucket,
			RequestCount: row.RequestCount,
			AvgLatencyMs: row.AvgLatencyMs,
			P95LatencyMs: row.P95LatencyMs,
			TimeoutCount: row.TimeoutCount,
			ErrorCount:   row.ErrorCount,
			TokensPerSec: row.TokensPerSec,
		}
	}
	return out
}

type aiLatencyHistogramDTO struct {
	ModelName    string `ch:"model_name"`
	BucketMs     int64  `ch:"bucket_ms"`
	RequestCount int64  `ch:"request_count"`
}

func mapAILatencyHistogramDTOs(rows []aiLatencyHistogramDTO) []AILatencyHistogram {
	out := make([]AILatencyHistogram, len(rows))
	for i, row := range rows {
		out[i] = AILatencyHistogram(row)
	}
	return out
}

type aiCostMetricDTO struct {
	ModelName             string  `ch:"model_name"`
	ModelProvider         string  `ch:"model_provider"`
	TotalRequests         int64   `ch:"total_requests"`
	TotalCostUSD          float64 `ch:"total_cost_usd"`
	AvgCostPerQuery       float64 `ch:"avg_cost_per_query"`
	MaxCostPerQuery       float64 `ch:"max_cost_per_query"`
	TotalPromptTokens     int64   `ch:"total_prompt_tokens"`
	TotalCompletionTokens int64   `ch:"total_completion_tokens"`
	TotalTokens           int64   `ch:"total_tokens"`
	AvgPromptTokens       float64 `ch:"avg_prompt_tokens"`
	AvgCompletionTokens   float64 `ch:"avg_completion_tokens"`
	CacheHitRate          float64 `ch:"cache_hit_rate"`
	TotalCacheTokens      int64   `ch:"total_cache_tokens"`
}

func mapAICostMetricDTOs(rows []aiCostMetricDTO) []AICostMetric {
	out := make([]AICostMetric, len(rows))
	for i, row := range rows {
		out[i] = AICostMetric{
			ModelName:             row.ModelName,
			ModelProvider:         row.ModelProvider,
			TotalRequests:         row.TotalRequests,
			TotalCostUsd:          row.TotalCostUSD,
			AvgCostPerQuery:       row.AvgCostPerQuery,
			MaxCostPerQuery:       row.MaxCostPerQuery,
			TotalPromptTokens:     row.TotalPromptTokens,
			TotalCompletionTokens: row.TotalCompletionTokens,
			TotalTokens:           row.TotalTokens,
			AvgPromptTokens:       row.AvgPromptTokens,
			AvgCompletionTokens:   row.AvgCompletionTokens,
			CacheHitRate:          row.CacheHitRate,
			TotalCacheTokens:      row.TotalCacheTokens,
		}
	}
	return out
}

type aiCostTimeSeriesDTO struct {
	ModelName        string  `ch:"model_name"`
	TimeBucket       string  `ch:"time_bucket"`
	CostPerInterval  float64 `ch:"cost_per_interval"`
	PromptTokens     int64   `ch:"prompt_tokens"`
	CompletionTokens int64   `ch:"completion_tokens"`
	RequestCount     int64   `ch:"request_count"`
}

func mapAICostTimeSeriesDTOs(rows []aiCostTimeSeriesDTO) []AICostTimeSeries {
	out := make([]AICostTimeSeries, len(rows))
	for i, row := range rows {
		out[i] = AICostTimeSeries{
			ModelName:        row.ModelName,
			Timestamp:        row.TimeBucket,
			CostPerInterval:  row.CostPerInterval,
			PromptTokens:     row.PromptTokens,
			CompletionTokens: row.CompletionTokens,
			RequestCount:     row.RequestCount,
		}
	}
	return out
}

type aiTokenBreakdownDTO struct {
	ModelName        string `ch:"model_name"`
	PromptTokens     int64  `ch:"prompt_tokens"`
	CompletionTokens int64  `ch:"completion_tokens"`
	SystemTokens     int64  `ch:"system_tokens"`
	CacheTokens      int64  `ch:"cache_tokens"`
}

func mapAITokenBreakdownDTOs(rows []aiTokenBreakdownDTO) []AITokenBreakdown {
	out := make([]AITokenBreakdown, len(rows))
	for i, row := range rows {
		out[i] = AITokenBreakdown(row)
	}
	return out
}

type aiSecurityMetricDTO struct {
	ModelName             string  `ch:"model_name"`
	ModelProvider         string  `ch:"model_provider"`
	TotalRequests         int64   `ch:"total_requests"`
	PIIDetectedCount      int64   `ch:"pii_detected_count"`
	PIIDetectionRate      float64 `ch:"pii_detection_rate"`
	GuardrailBlockedCount int64   `ch:"guardrail_blocked_count"`
	GuardrailBlockRate    float64 `ch:"guardrail_block_rate"`
	ContentPolicyCount    int64   `ch:"content_policy_count"`
	ContentPolicyRate     float64 `ch:"content_policy_rate"`
}

func mapAISecurityMetricDTOs(rows []aiSecurityMetricDTO) []AISecurityMetric {
	out := make([]AISecurityMetric, len(rows))
	for i, row := range rows {
		out[i] = AISecurityMetric{
			ModelName:             row.ModelName,
			ModelProvider:         row.ModelProvider,
			TotalRequests:         row.TotalRequests,
			PiiDetectedCount:      row.PIIDetectedCount,
			PiiDetectionRate:      row.PIIDetectionRate,
			GuardrailBlockedCount: row.GuardrailBlockedCount,
			GuardrailBlockRate:    row.GuardrailBlockRate,
			ContentPolicyCount:    row.ContentPolicyCount,
			ContentPolicyRate:     row.ContentPolicyRate,
		}
	}
	return out
}

type aiSecurityTimeSeriesDTO struct {
	ModelName          string `ch:"model_name"`
	TimeBucket         string `ch:"time_bucket"`
	TotalRequests      int64  `ch:"total_requests"`
	PIICount           int64  `ch:"pii_count"`
	GuardrailCount     int64  `ch:"guardrail_count"`
	ContentPolicyCount int64  `ch:"content_policy_count"`
}

func mapAISecurityTimeSeriesDTOs(rows []aiSecurityTimeSeriesDTO) []AISecurityTimeSeries {
	out := make([]AISecurityTimeSeries, len(rows))
	for i, row := range rows {
		out[i] = AISecurityTimeSeries{
			ModelName:          row.ModelName,
			Timestamp:          row.TimeBucket,
			TotalRequests:      row.TotalRequests,
			PiiCount:           row.PIICount,
			GuardrailCount:     row.GuardrailCount,
			ContentPolicyCount: row.ContentPolicyCount,
		}
	}
	return out
}

type aiPIICategoryDTO struct {
	ModelName      string `ch:"model_name"`
	PIICategories  string `ch:"pii_categories"`
	DetectionCount int64  `ch:"detection_count"`
}

func mapAIPIICategoryDTOs(rows []aiPIICategoryDTO) []AIPiiCategory {
	out := make([]AIPiiCategory, len(rows))
	for i, row := range rows {
		out[i] = AIPiiCategory{
			ModelName:      row.ModelName,
			PiiCategories:  row.PIICategories,
			DetectionCount: row.DetectionCount,
		}
	}
	return out
}
