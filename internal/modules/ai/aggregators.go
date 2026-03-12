package ai

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type MetricAggregator interface {
	Aggregate(rows []map[string]any) (any, error)
}

type PerformanceMetricAggregator struct{}

func NewPerformanceMetricAggregator() *PerformanceMetricAggregator {
	return &PerformanceMetricAggregator{}
}

func (a *PerformanceMetricAggregator) Aggregate(rows []map[string]any) (any, error) {
	metrics := make([]AIPerformanceMetric, len(rows))
	for i, row := range rows {
		metrics[i] = AIPerformanceMetric{
			ModelName:       dbutil.StringFromAny(row["model_name"]),
			ModelProvider:   dbutil.StringFromAny(row["model_provider"]),
			RequestType:     dbutil.StringFromAny(row["request_type"]),
			TotalRequests:   dbutil.Int64FromAny(row["total_requests"]),
			AvgQps:          dbutil.Float64FromAny(row["avg_qps"]),
			AvgLatencyMs:    dbutil.Float64FromAny(row["avg_latency_ms"]),
			P50LatencyMs:    dbutil.Float64FromAny(row["p50_latency_ms"]),
			P95LatencyMs:    dbutil.Float64FromAny(row["p95_latency_ms"]),
			P99LatencyMs:    dbutil.Float64FromAny(row["p99_latency_ms"]),
			MaxLatencyMs:    dbutil.Float64FromAny(row["max_latency_ms"]),
			TimeoutCount:    dbutil.Int64FromAny(row["timeout_count"]),
			ErrorCount:      dbutil.Int64FromAny(row["error_count"]),
			TimeoutRate:     dbutil.Float64FromAny(row["timeout_rate"]),
			ErrorRate:       dbutil.Float64FromAny(row["error_rate"]),
			AvgTokensPerSec: dbutil.Float64FromAny(row["avg_tokens_per_sec"]),
			AvgRetryCount:   dbutil.Float64FromAny(row["avg_retry_count"]),
		}
	}
	return metrics, nil
}

type CostMetricAggregator struct{}

func NewCostMetricAggregator() *CostMetricAggregator {
	return &CostMetricAggregator{}
}

func (a *CostMetricAggregator) Aggregate(rows []map[string]any) (any, error) {
	metrics := make([]AICostMetric, len(rows))
	for i, row := range rows {
		metrics[i] = AICostMetric{
			ModelName:             dbutil.StringFromAny(row["model_name"]),
			ModelProvider:         dbutil.StringFromAny(row["model_provider"]),
			TotalRequests:         dbutil.Int64FromAny(row["total_requests"]),
			TotalCostUsd:          dbutil.Float64FromAny(row["total_cost_usd"]),
			AvgCostPerQuery:       dbutil.Float64FromAny(row["avg_cost_per_query"]),
			MaxCostPerQuery:       dbutil.Float64FromAny(row["max_cost_per_query"]),
			TotalPromptTokens:     dbutil.Int64FromAny(row["total_prompt_tokens"]),
			TotalCompletionTokens: dbutil.Int64FromAny(row["total_completion_tokens"]),
			TotalTokens:           dbutil.Int64FromAny(row["total_tokens"]),
			AvgPromptTokens:       dbutil.Float64FromAny(row["avg_prompt_tokens"]),
			AvgCompletionTokens:   dbutil.Float64FromAny(row["avg_completion_tokens"]),
			CacheHitRate:          dbutil.Float64FromAny(row["cache_hit_rate"]),
			TotalCacheTokens:      dbutil.Int64FromAny(row["total_cache_tokens"]),
		}
	}
	return metrics, nil
}

type SecurityMetricAggregator struct{}

func NewSecurityMetricAggregator() *SecurityMetricAggregator {
	return &SecurityMetricAggregator{}
}

func (a *SecurityMetricAggregator) Aggregate(rows []map[string]any) (any, error) {
	metrics := make([]AISecurityMetric, len(rows))
	for i, row := range rows {
		metrics[i] = AISecurityMetric{
			ModelName:             dbutil.StringFromAny(row["model_name"]),
			ModelProvider:         dbutil.StringFromAny(row["model_provider"]),
			TotalRequests:         dbutil.Int64FromAny(row["total_requests"]),
			PiiDetectedCount:      dbutil.Int64FromAny(row["pii_detected_count"]),
			PiiDetectionRate:      dbutil.Float64FromAny(row["pii_detection_rate"]),
			GuardrailBlockedCount: dbutil.Int64FromAny(row["guardrail_blocked_count"]),
			GuardrailBlockRate:    dbutil.Float64FromAny(row["guardrail_block_rate"]),
			ContentPolicyCount:    dbutil.Int64FromAny(row["content_policy_count"]),
			ContentPolicyRate:     dbutil.Float64FromAny(row["content_policy_rate"]),
		}
	}
	return metrics, nil
}

type TimeSeriesAggregator struct {
	aggregateType string // "performance", "cost", "security"
}

func NewTimeSeriesAggregator(aggregateType string) *TimeSeriesAggregator {
	return &TimeSeriesAggregator{
		aggregateType: aggregateType,
	}
}

func (a *TimeSeriesAggregator) Aggregate(rows []map[string]any) (any, error) {
	switch a.aggregateType {
	case "performance":
		return a.aggregatePerformanceTimeSeries(rows), nil
	case "cost":
		return a.aggregateCostTimeSeries(rows), nil
	case "security":
		return a.aggregateSecurityTimeSeries(rows), nil
	default:
		return nil, nil
	}
}

func (a *TimeSeriesAggregator) aggregatePerformanceTimeSeries(rows []map[string]any) []AIPerformanceTimeSeries {
	timeseries := make([]AIPerformanceTimeSeries, len(rows))
	for i, row := range rows {
		timeseries[i] = AIPerformanceTimeSeries{
			ModelName:    dbutil.StringFromAny(row["model_name"]),
			Timestamp:    dbutil.StringFromAny(firstNonNil(row["time_bucket"], row["timestamp"])),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			AvgLatencyMs: dbutil.Float64FromAny(row["avg_latency_ms"]),
			P95LatencyMs: dbutil.Float64FromAny(row["p95_latency_ms"]),
			TimeoutCount: dbutil.Int64FromAny(row["timeout_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			TokensPerSec: dbutil.Float64FromAny(row["tokens_per_sec"]),
		}
	}
	return timeseries
}

func (a *TimeSeriesAggregator) aggregateCostTimeSeries(rows []map[string]any) []AICostTimeSeries {
	timeseries := make([]AICostTimeSeries, len(rows))
	for i, row := range rows {
		timeseries[i] = AICostTimeSeries{
			ModelName:        dbutil.StringFromAny(row["model_name"]),
			Timestamp:        dbutil.StringFromAny(firstNonNil(row["time_bucket"], row["timestamp"])),
			CostPerInterval:  dbutil.Float64FromAny(row["cost_per_interval"]),
			PromptTokens:     dbutil.Int64FromAny(row["prompt_tokens"]),
			CompletionTokens: dbutil.Int64FromAny(row["completion_tokens"]),
			RequestCount:     dbutil.Int64FromAny(row["request_count"]),
		}
	}
	return timeseries
}

func (a *TimeSeriesAggregator) aggregateSecurityTimeSeries(rows []map[string]any) []AISecurityTimeSeries {
	timeseries := make([]AISecurityTimeSeries, len(rows))
	for i, row := range rows {
		timeseries[i] = AISecurityTimeSeries{
			ModelName:          dbutil.StringFromAny(row["model_name"]),
			Timestamp:          dbutil.StringFromAny(firstNonNil(row["time_bucket"], row["timestamp"])),
			TotalRequests:      dbutil.Int64FromAny(row["total_requests"]),
			PiiCount:           dbutil.Int64FromAny(row["pii_count"]),
			GuardrailCount:     dbutil.Int64FromAny(row["guardrail_count"]),
			ContentPolicyCount: dbutil.Int64FromAny(row["content_policy_count"]),
		}
	}
	return timeseries
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

type HistogramAggregator struct{}

func NewHistogramAggregator() *HistogramAggregator {
	return &HistogramAggregator{}
}

func (a *HistogramAggregator) Aggregate(rows []map[string]any) (any, error) {
	histogram := make([]AILatencyHistogram, len(rows))
	for i, row := range rows {
		histogram[i] = AILatencyHistogram{
			ModelName:    dbutil.StringFromAny(row["model_name"]),
			BucketMs:     dbutil.Int64FromAny(row["bucket_ms"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
		}
	}
	return histogram, nil
}

type ModelListAggregator struct{}

func NewModelListAggregator() *ModelListAggregator {
	return &ModelListAggregator{}
}

func (a *ModelListAggregator) Aggregate(rows []map[string]any) (any, error) {
	models := make([]AIModel, len(rows))
	for i, row := range rows {
		models[i] = AIModel{
			ModelName:     dbutil.StringFromAny(row["model_name"]),
			ModelProvider: dbutil.StringFromAny(row["model_provider"]),
		}
	}
	return models, nil
}

type TokenBreakdownAggregator struct{}

func NewTokenBreakdownAggregator() *TokenBreakdownAggregator {
	return &TokenBreakdownAggregator{}
}

func (a *TokenBreakdownAggregator) Aggregate(rows []map[string]any) (any, error) {
	breakdown := make([]AITokenBreakdown, len(rows))
	for i, row := range rows {
		breakdown[i] = AITokenBreakdown{
			ModelName:        dbutil.StringFromAny(row["model_name"]),
			PromptTokens:     dbutil.Int64FromAny(row["prompt_tokens"]),
			CompletionTokens: dbutil.Int64FromAny(row["completion_tokens"]),
			SystemTokens:     dbutil.Int64FromAny(row["system_tokens"]),
			CacheTokens:      dbutil.Int64FromAny(row["cache_tokens"]),
		}
	}
	return breakdown, nil
}

type PIICategoryAggregator struct{}

func NewPIICategoryAggregator() *PIICategoryAggregator {
	return &PIICategoryAggregator{}
}

func (a *PIICategoryAggregator) Aggregate(rows []map[string]any) (any, error) {
	categories := make([]AIPiiCategory, len(rows))
	for i, row := range rows {
		categories[i] = AIPiiCategory{
			ModelName:      dbutil.StringFromAny(row["model_name"]),
			PiiCategories:  dbutil.StringFromAny(row["pii_categories"]),
			DetectionCount: dbutil.Int64FromAny(row["detection_count"]),
		}
	}
	return categories, nil
}

type SummaryAggregator struct{}

func NewSummaryAggregator() *SummaryAggregator {
	return &SummaryAggregator{}
}

func (a *SummaryAggregator) Aggregate(rows []map[string]any) (any, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	row := rows[0]
	return &AISummary{
		TotalRequests:      dbutil.Int64FromAny(row["total_requests"]),
		AvgQps:             dbutil.Float64FromAny(row["avg_qps"]),
		AvgLatencyMs:       dbutil.Float64FromAny(row["avg_latency_ms"]),
		P95LatencyMs:       dbutil.Float64FromAny(row["p95_latency_ms"]),
		TimeoutCount:       dbutil.Int64FromAny(row["timeout_count"]),
		ErrorCount:         dbutil.Int64FromAny(row["error_count"]),
		TotalTokens:        dbutil.Int64FromAny(row["total_tokens"]),
		TotalCostUsd:       dbutil.Float64FromAny(row["total_cost_usd"]),
		AvgCostPerQuery:    dbutil.Float64FromAny(row["avg_cost_per_query"]),
		CacheHitRate:       dbutil.Float64FromAny(row["cache_hit_rate"]),
		PiiDetectionRate:   dbutil.Float64FromAny(row["pii_detection_rate"]),
		GuardrailBlockRate: dbutil.Float64FromAny(row["guardrail_block_rate"]),
		AvgTokensPerSec:    dbutil.Float64FromAny(row["avg_tokens_per_sec"]),
		ActiveModels:       dbutil.Int64FromAny(row["active_models"]),
	}, nil
}
