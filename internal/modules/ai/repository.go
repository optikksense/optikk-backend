package ai

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// Repository encapsulates data access logic for AI models.
type Repository struct {
	db dbutil.Querier
}

// NewRepository creates a new AI Repository.
func NewRepository(db dbutil.Querier) *Repository {
	return &Repository{db: db}
}

// GetAISummary returns an aggregate performance/cost/security summary for all AI models.
func (r *Repository) GetAISummary(teamUUID string, startMs, endMs int64) (map[string]any, error) {
	return dbutil.QueryMap(r.db, `
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
}

// GetAIModels returns distinct AI models active in the time window.
func (r *Repository) GetAIModels(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT DISTINCT model_name, model_provider
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> ''
		ORDER BY model_name ASC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

// GetAIPerformanceMetrics returns per-model latency, throughput, error and timeout rates.
func (r *Repository) GetAIPerformanceMetrics(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
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
}

// GetAIPerformanceTimeSeries returns per-model latency / throughput time series.
func (r *Repository) GetAIPerformanceTimeSeries(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT model_name,
		       DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00') as timestamp,
		       COUNT(*) as request_count,
		       AVG(duration_ms) as avg_latency_ms,
		       AVG(duration_ms) as p95_latency_ms,
		       SUM(CASE WHEN timeout=1 THEN 1 ELSE 0 END) as timeout_count,
		       SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END) as error_count,
		       AVG(CASE WHEN duration_ms>0 THEN COALESCE(tokens_completion,0)/(duration_ms/1000.0) ELSE 0 END) as tokens_per_sec
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> ''
		GROUP BY model_name, DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00')
		ORDER BY timestamp ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

// GetAILatencyHistogram returns latency distribution (100ms buckets) per model.
func (r *Repository) GetAILatencyHistogram(teamUUID string, modelName string, startMs, endMs int64) ([]map[string]any, error) {
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
	return dbutil.QueryMaps(r.db, query, args...)
}

// GetAICostMetrics returns per-model token usage and cost breakdown.
func (r *Repository) GetAICostMetrics(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
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
}

// GetAICostTimeSeries returns cost and token usage over time per model.
func (r *Repository) GetAICostTimeSeries(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT model_name,
		       DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00') as timestamp,
		       SUM(COALESCE(cost_usd,0)) as cost_per_interval,
		       SUM(COALESCE(tokens_prompt,0)) as prompt_tokens,
		       SUM(COALESCE(tokens_completion,0)) as completion_tokens,
		       COUNT(*) as request_count
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> ''
		GROUP BY model_name, DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00')
		ORDER BY timestamp ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

// GetAITokenBreakdown returns token type breakdown per model.
func (r *Repository) GetAITokenBreakdown(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
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
}

// GetAISecurityMetrics returns PII detection and guardrail block rates per model.
func (r *Repository) GetAISecurityMetrics(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
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
}

// GetAISecurityTimeSeries returns security-event time series per model.
func (r *Repository) GetAISecurityTimeSeries(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT model_name,
		       DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00') as timestamp,
		       COUNT(*) as total_requests,
		       SUM(CASE WHEN pii_detected=1 THEN 1 ELSE 0 END) as pii_count,
		       SUM(CASE WHEN guardrail_blocked=1 THEN 1 ELSE 0 END) as guardrail_count,
		       SUM(CASE WHEN content_policy=1 THEN 1 ELSE 0 END) as content_policy_count
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> ''
		GROUP BY model_name, DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00')
		ORDER BY timestamp ASC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}

// GetAIPiiCategories returns PII category breakdown for detected events.
func (r *Repository) GetAIPiiCategories(teamUUID string, startMs, endMs int64) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT model_name, pii_categories, COUNT(*) as detection_count
		FROM ai_requests
		WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND model_name <> '' AND pii_detected = 1
		GROUP BY model_name, pii_categories
		ORDER BY detection_count DESC
		LIMIT 100
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
}
