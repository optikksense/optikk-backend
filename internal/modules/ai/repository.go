package ai

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// aiBucketExpr returns a ClickHouse time-bucketing expression for adaptive granularity.
func aiBucketExpr(startMs, endMs int64) string {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return "DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00')"
	case hours <= 24:
		return "toStartOfFiveMinutes(timestamp)"
	case hours <= 168:
		return "toStartOfHour(timestamp)"
	default:
		return "toStartOfDay(timestamp)"
	}
}

// Repository encapsulates data access logic for AI models.
type Repository interface {
	GetAISummary(teamUUID string, startMs, endMs int64) (*AISummary, error)
	GetAIModels(teamUUID string, startMs, endMs int64) ([]AIModel, error)
	GetAIPerformanceMetrics(teamUUID string, startMs, endMs int64) ([]AIPerformanceMetric, error)
	GetAIPerformanceTimeSeries(teamUUID string, startMs, endMs int64) ([]AIPerformanceTimeSeries, error)
	GetAILatencyHistogram(teamUUID string, modelName string, startMs, endMs int64) ([]AILatencyHistogram, error)
	GetAICostMetrics(teamUUID string, startMs, endMs int64) ([]AICostMetric, error)
	GetAICostTimeSeries(teamUUID string, startMs, endMs int64) ([]AICostTimeSeries, error)
	GetAITokenBreakdown(teamUUID string, startMs, endMs int64) ([]AITokenBreakdown, error)
	GetAISecurityMetrics(teamUUID string, startMs, endMs int64) ([]AISecurityMetric, error)
	GetAISecurityTimeSeries(teamUUID string, startMs, endMs int64) ([]AISecurityTimeSeries, error)
	GetAIPiiCategories(teamUUID string, startMs, endMs int64) ([]AIPiiCategory, error)
}

// ClickHouseRepository encapsulates data access logic for AI models.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new AI Repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// GetAISummary returns an aggregate performance/cost/security summary for all AI models.
func (r *ClickHouseRepository) GetAISummary(teamUUID string, startMs, endMs int64) (*AISummary, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT COUNT(*) as total_requests,
		       COUNT(*) / GREATEST(TIMESTAMPDIFF(SECOND, ?, ?) ,1) as avg_qps,
		       AVG(duration_ms) as avg_latency_ms,
		       AVG(duration_ms) as p95_latency_ms,
		       SUM(CASE WHEN timeout = 1 THEN 1 ELSE 0 END) as timeout_count,
		       SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END) as error_count,
		       SUM(COALESCE(tokens_prompt,0) + COALESCE(tokens_completion,0)) as total_tokens,
		       SUM(COALESCE(cost_usd,0)) as total_cost_usd,
		       AVG(COALESCE(cost_usd,0)) as avg_cost_per_query,
		       IF(COUNT(*)>0, SUM(CASE WHEN cache_hit=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as cache_hit_rate,
		       IF(COUNT(*)>0, SUM(CASE WHEN pii_detected=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as pii_detection_rate,
		       IF(COUNT(*)>0, SUM(CASE WHEN guardrail_blocked=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as guardrail_block_rate,
		       AVG(CASE WHEN duration_ms > 0 THEN COALESCE(tokens_completion,0) / (duration_ms/1000.0) ELSE 0 END) as avg_tokens_per_sec,
		       COUNT(DISTINCT model_name) as active_models
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> ''
	`, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil || len(row) == 0 {
		return nil, err
	}

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

// GetAIModels returns distinct AI models active in the time window.
func (r *ClickHouseRepository) GetAIModels(teamUUID string, startMs, endMs int64) ([]AIModel, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT DISTINCT model_name, model_provider
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> ''
		ORDER BY model_name ASC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	models := make([]AIModel, len(rows))
	for i, row := range rows {
		models[i] = AIModel{
			ModelName:     dbutil.StringFromAny(row["model_name"]),
			ModelProvider: dbutil.StringFromAny(row["model_provider"]),
		}
	}
	return models, nil
}

// GetAIPerformanceMetrics returns per-model latency, throughput, error and timeout rates.
func (r *ClickHouseRepository) GetAIPerformanceMetrics(teamUUID string, startMs, endMs int64) ([]AIPerformanceMetric, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT model_name, model_provider, request_type,
		       COUNT(*) as total_requests,
		       COUNT(*) / GREATEST(TIMESTAMPDIFF(SECOND, ?, ?),1) as avg_qps,
		       AVG(duration_ms) as avg_latency_ms,
		       AVG(duration_ms) as p50_latency_ms,
		       AVG(duration_ms) as p95_latency_ms,
		       AVG(duration_ms) as p99_latency_ms,
		       MAX(duration_ms) as max_latency_ms,
		       SUM(CASE WHEN timeout=1 THEN 1 ELSE 0 END) as timeout_count,
		       SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END) as error_count,
		       IF(COUNT(*)>0, SUM(CASE WHEN timeout=1 THEN 1 ELSE 0 END)*100.0/COUNT(*),0) as timeout_rate,
		       IF(COUNT(*)>0, SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END)*100.0/COUNT(*),0) as error_rate,
		       AVG(CASE WHEN duration_ms>0 THEN COALESCE(tokens_completion,0)/(duration_ms/1000.0) ELSE 0 END) as avg_tokens_per_sec,
		       AVG(COALESCE(retry_count,0)) as avg_retry_count
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> ''
		GROUP BY model_name, model_provider, request_type
		ORDER BY total_requests DESC
		LIMIT 100
	`, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

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

// GetAIPerformanceTimeSeries returns per-model latency / throughput time series with adaptive bucketing.
func (r *ClickHouseRepository) GetAIPerformanceTimeSeries(teamUUID string, startMs, endMs int64) ([]AIPerformanceTimeSeries, error) {
	bucket := aiBucketExpr(startMs, endMs)
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT model_name,
		       %s as timestamp,
		       COUNT(*) as request_count,
		       AVG(duration_ms) as avg_latency_ms,
		       AVG(duration_ms) as p95_latency_ms,
		       SUM(CASE WHEN timeout=1 THEN 1 ELSE 0 END) as timeout_count,
		       SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END) as error_count,
		       AVG(CASE WHEN duration_ms>0 THEN COALESCE(tokens_completion,0)/(duration_ms/1000.0) ELSE 0 END) as tokens_per_sec
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> ''
		GROUP BY model_name, %s
		ORDER BY timestamp ASC
	`, bucket, bucket), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	timeseries := make([]AIPerformanceTimeSeries, len(rows))
	for i, row := range rows {
		timeseries[i] = AIPerformanceTimeSeries{
			ModelName:    dbutil.StringFromAny(row["model_name"]),
			Timestamp:    dbutil.StringFromAny(row["timestamp"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			AvgLatencyMs: dbutil.Float64FromAny(row["avg_latency_ms"]),
			P95LatencyMs: dbutil.Float64FromAny(row["p95_latency_ms"]),
			TimeoutCount: dbutil.Int64FromAny(row["timeout_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			TokensPerSec: dbutil.Float64FromAny(row["tokens_per_sec"]),
		}
	}
	return timeseries, nil
}

// GetAILatencyHistogram returns latency distribution (100ms buckets) per
func (r *ClickHouseRepository) GetAILatencyHistogram(teamUUID string, modelName string, startMs, endMs int64) ([]AILatencyHistogram, error) {
	query := `
		SELECT model_name,
		       FLOOR(duration_ms / 100) * 100 as bucket_ms,
		       COUNT(*) as request_count
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> ''`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if modelName != "" {
		query += ` AND model_name = ?`
		args = append(args, modelName)
	}
	query += ` GROUP BY model_name, FLOOR(duration_ms / 100) * 100 ORDER BY model_name, bucket_ms ASC LIMIT 200`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

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

// GetAICostMetrics returns per-model token usage and cost breakdown.
func (r *ClickHouseRepository) GetAICostMetrics(teamUUID string, startMs, endMs int64) ([]AICostMetric, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT model_name, model_provider,
		       COUNT(*) as total_requests,
		       SUM(COALESCE(cost_usd,0)) as total_cost_usd,
		       AVG(COALESCE(cost_usd,0)) as avg_cost_per_query,
		       MAX(COALESCE(cost_usd,0)) as max_cost_per_query,
		       SUM(COALESCE(tokens_prompt,0)) as total_prompt_tokens,
		       SUM(COALESCE(tokens_completion,0)) as total_completion_tokens,
		       SUM(COALESCE(tokens_prompt,0)+COALESCE(tokens_completion,0)) as total_tokens,
		       AVG(COALESCE(tokens_prompt,0)) as avg_prompt_tokens,
		       AVG(COALESCE(tokens_completion,0)) as avg_completion_tokens,
		       IF(COUNT(*)>0, SUM(CASE WHEN cache_hit=1 THEN 1 ELSE 0 END)*100.0/COUNT(*),0) as cache_hit_rate,
		       SUM(COALESCE(cache_tokens,0)) as total_cache_tokens
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> ''
		GROUP BY model_name, model_provider
		ORDER BY total_cost_usd DESC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

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

// GetAICostTimeSeries returns cost and token usage over time per model with adaptive bucketing.
func (r *ClickHouseRepository) GetAICostTimeSeries(teamUUID string, startMs, endMs int64) ([]AICostTimeSeries, error) {
	bucket := aiBucketExpr(startMs, endMs)
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT model_name,
		       %s as timestamp,
		       SUM(COALESCE(cost_usd,0)) as cost_per_interval,
		       SUM(COALESCE(tokens_prompt,0)) as prompt_tokens,
		       SUM(COALESCE(tokens_completion,0)) as completion_tokens,
		       COUNT(*) as request_count
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> ''
		GROUP BY model_name, %s
		ORDER BY timestamp ASC
	`, bucket, bucket), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	timeseries := make([]AICostTimeSeries, len(rows))
	for i, row := range rows {
		timeseries[i] = AICostTimeSeries{
			ModelName:        dbutil.StringFromAny(row["model_name"]),
			Timestamp:        dbutil.StringFromAny(row["timestamp"]),
			CostPerInterval:  dbutil.Float64FromAny(row["cost_per_interval"]),
			PromptTokens:     dbutil.Int64FromAny(row["prompt_tokens"]),
			CompletionTokens: dbutil.Int64FromAny(row["completion_tokens"]),
			RequestCount:     dbutil.Int64FromAny(row["request_count"]),
		}
	}
	return timeseries, nil
}

// GetAITokenBreakdown returns token type breakdown per
func (r *ClickHouseRepository) GetAITokenBreakdown(teamUUID string, startMs, endMs int64) ([]AITokenBreakdown, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT model_name,
		       SUM(COALESCE(tokens_prompt,0)) as prompt_tokens,
		       SUM(COALESCE(tokens_completion,0)) as completion_tokens,
		       SUM(COALESCE(tokens_system,0)) as system_tokens,
		       SUM(COALESCE(cache_tokens,0)) as cache_tokens
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> ''
		GROUP BY model_name
		ORDER BY (prompt_tokens + completion_tokens) DESC
		LIMIT 50
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

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

// GetAISecurityMetrics returns PII detection and guardrail block rates per
func (r *ClickHouseRepository) GetAISecurityMetrics(teamUUID string, startMs, endMs int64) ([]AISecurityMetric, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT model_name, model_provider,
		       COUNT(*) as total_requests,
		       SUM(CASE WHEN pii_detected=1 THEN 1 ELSE 0 END) as pii_detected_count,
		       IF(COUNT(*)>0, SUM(CASE WHEN pii_detected=1 THEN 1 ELSE 0 END)*100.0/COUNT(*),0) as pii_detection_rate,
		       SUM(CASE WHEN guardrail_blocked=1 THEN 1 ELSE 0 END) as guardrail_blocked_count,
		       IF(COUNT(*)>0, SUM(CASE WHEN guardrail_blocked=1 THEN 1 ELSE 0 END)*100.0/COUNT(*),0) as guardrail_block_rate,
		       SUM(CASE WHEN content_policy=1 THEN 1 ELSE 0 END) as content_policy_count,
		       IF(COUNT(*)>0, SUM(CASE WHEN content_policy=1 THEN 1 ELSE 0 END)*100.0/COUNT(*),0) as content_policy_rate
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> ''
		GROUP BY model_name, model_provider
		ORDER BY pii_detected_count DESC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

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

// GetAISecurityTimeSeries returns security-event time series per model with adaptive bucketing.
func (r *ClickHouseRepository) GetAISecurityTimeSeries(teamUUID string, startMs, endMs int64) ([]AISecurityTimeSeries, error) {
	bucket := aiBucketExpr(startMs, endMs)
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT model_name,
		       %s as timestamp,
		       COUNT(*) as total_requests,
		       SUM(CASE WHEN pii_detected=1 THEN 1 ELSE 0 END) as pii_count,
		       SUM(CASE WHEN guardrail_blocked=1 THEN 1 ELSE 0 END) as guardrail_count,
		       SUM(CASE WHEN content_policy=1 THEN 1 ELSE 0 END) as content_policy_count
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> ''
		GROUP BY model_name, %s
		ORDER BY timestamp ASC
	`, bucket, bucket), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

	timeseries := make([]AISecurityTimeSeries, len(rows))
	for i, row := range rows {
		timeseries[i] = AISecurityTimeSeries{
			ModelName:          dbutil.StringFromAny(row["model_name"]),
			Timestamp:          dbutil.StringFromAny(row["timestamp"]),
			TotalRequests:      dbutil.Int64FromAny(row["total_requests"]),
			PiiCount:           dbutil.Int64FromAny(row["pii_count"]),
			GuardrailCount:     dbutil.Int64FromAny(row["guardrail_count"]),
			ContentPolicyCount: dbutil.Int64FromAny(row["content_policy_count"]),
		}
	}
	return timeseries, nil
}

// GetAIPiiCategories returns PII category breakdown for detected events.
func (r *ClickHouseRepository) GetAIPiiCategories(teamUUID string, startMs, endMs int64) ([]AIPiiCategory, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT model_name, pii_categories, COUNT(*) as detection_count
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> '' AND pii_detected = 1
		GROUP BY model_name, pii_categories
		ORDER BY detection_count DESC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))

	if err != nil {
		return nil, err
	}

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
