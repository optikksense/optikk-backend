package dashboard

import (
	"fmt"
)

// --- Performance Metrics ---

type PerformanceMetricsQueryBuilder struct {
	*BaseQueryBuilder
}

func NewPerformanceMetricsQueryBuilder(teamID int64, startMs, endMs int64) *PerformanceMetricsQueryBuilder {
	return &PerformanceMetricsQueryBuilder{BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs)}
}

func (b *PerformanceMetricsQueryBuilder) Build() (query string, args []any) {
	var where string
	where, args = b.baseWhereClause()

	durationMs := attrFlt("duration_ms")
	outputTokens := attrInt("gen.ai.usage.output_tokens")
	retryCount := attrInt("ai.retry_count")
	aiTimeout := attrInt("ai.timeout")

	query = fmt.Sprintf(`
		SELECT %s as model_name, %s as model_provider, %s as request_type,
		       toInt64(COUNT(*)) as total_requests,
		       COUNT(*) / GREATEST(dateDiff('second', @start, @end), 1) as avg_qps,
		       AVG(%s) as avg_latency_ms,
		       AVG(%s) as p50_latency_ms,
		       AVG(%s) as p95_latency_ms,
		       AVG(%s) as p99_latency_ms,
		       MAX(%s) as max_latency_ms,
		       toInt64(SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END)) as timeout_count,
		       toInt64(countIf(has_error)) as error_count,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as timeout_rate,
		       IF(COUNT(*) > 0, countIf(has_error) * 100.0 / COUNT(*), 0) as error_rate,
		       AVG(CASE WHEN %s > 0 THEN COALESCE(%s, 0) / (%s / 1000.0) ELSE 0 END) as avg_tokens_per_sec,
		       AVG(COALESCE(%s, 0)) as avg_retry_count
		FROM %s %s
		GROUP BY model_name, model_provider, request_type
		ORDER BY total_requests DESC LIMIT %d
	`,
		attrStr("gen.ai.request.model"), attrStr("server.address"), attrStr("gen.ai.operation.name"),
		durationMs, durationMs, durationMs, durationMs, durationMs,
		aiTimeout, aiTimeout,
		durationMs, outputTokens, durationMs,
		retryCount,
		tableMetrics, where, b.limit)

	return query, args
}

// --- Cost Metrics ---

type CostMetricsQueryBuilder struct{ *BaseQueryBuilder }

func NewCostMetricsQueryBuilder(teamID int64, startMs, endMs int64) *CostMetricsQueryBuilder {
	return &CostMetricsQueryBuilder{BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs)}
}

func (b *CostMetricsQueryBuilder) Build() (query string, args []any) {
	var where string
	where, args = b.baseWhereClause()
	costUSD := attrFlt("ai.cost_usd")
	inputTokens := attrInt("gen.ai.usage.input_tokens")
	outputTokens := attrInt("gen.ai.usage.output_tokens")
	cacheReadTokens := attrInt("gen.ai.usage.cache_read_input_tokens")
	cacheHit := attrInt("ai.cache_hit")

	query = fmt.Sprintf(`
		SELECT %s as model_name, %s as model_provider,
		       toInt64(COUNT(*)) as total_requests,
		       SUM(COALESCE(%s, 0)) as total_cost_usd,
		       AVG(COALESCE(%s, 0)) as avg_cost_per_query,
		       MAX(COALESCE(%s, 0)) as max_cost_per_query,
		       toInt64(SUM(COALESCE(%s, 0))) as total_prompt_tokens,
		       toInt64(SUM(COALESCE(%s, 0))) as total_completion_tokens,
		       toInt64(SUM(COALESCE(%s, 0) + COALESCE(%s, 0))) as total_tokens,
		       AVG(COALESCE(%s, 0)) as avg_prompt_tokens,
		       AVG(COALESCE(%s, 0)) as avg_completion_tokens,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as cache_hit_rate,
		       toInt64(SUM(COALESCE(%s, 0))) as total_cache_tokens
		FROM %s %s
		GROUP BY model_name, model_provider ORDER BY total_cost_usd DESC LIMIT %d
	`,
		attrStr("gen.ai.request.model"), attrStr("server.address"),
		costUSD, costUSD, costUSD,
		inputTokens, outputTokens, inputTokens, outputTokens,
		inputTokens, outputTokens, cacheHit, cacheReadTokens,
		tableMetrics, where, b.limit)

	return query, args
}

// --- Security Metrics ---

type SecurityMetricsQueryBuilder struct{ *BaseQueryBuilder }

func NewSecurityMetricsQueryBuilder(teamID int64, startMs, endMs int64) *SecurityMetricsQueryBuilder {
	return &SecurityMetricsQueryBuilder{BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs)}
}

func (b *SecurityMetricsQueryBuilder) Build() (query string, args []any) {
	var where string
	where, args = b.baseWhereClause()
	piiDetected := attrInt("ai.security.pii_detected")
	guardrailBlocked := attrInt("ai.security.guardrail_blocked")
	contentPolicy := attrInt("ai.security.content_policy")

	query = fmt.Sprintf(`
		SELECT %s as model_name, %s as model_provider,
		       toInt64(COUNT(*)) as total_requests,
		       toInt64(SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END)) as pii_detected_count,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as pii_detection_rate,
		       toInt64(SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END)) as guardrail_blocked_count,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as guardrail_block_rate,
		       toInt64(SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END)) as content_policy_count,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as content_policy_rate
		FROM %s %s
		GROUP BY model_name, model_provider ORDER BY pii_detected_count DESC LIMIT %d
	`,
		attrStr("gen.ai.request.model"), attrStr("server.address"),
		piiDetected, piiDetected,
		guardrailBlocked, guardrailBlocked,
		contentPolicy, contentPolicy,
		tableMetrics, where, b.limit)

	return query, args
}

// --- Token Breakdown ---

type TokenBreakdownQueryBuilder struct{ *BaseQueryBuilder }

func NewTokenBreakdownQueryBuilder(teamID int64, startMs, endMs int64) *TokenBreakdownQueryBuilder {
	return &TokenBreakdownQueryBuilder{BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs).WithLimit(50)}
}

func (b *TokenBreakdownQueryBuilder) Build() (query string, args []any) {
	var where string
	where, args = b.baseWhereClause()
	query = fmt.Sprintf(`
		SELECT %s as model_name,
		       toInt64(SUM(COALESCE(%s, 0))) as prompt_tokens,
		       toInt64(SUM(COALESCE(%s, 0))) as completion_tokens,
		       toInt64(SUM(COALESCE(%s, 0))) as system_tokens,
		       toInt64(SUM(COALESCE(%s, 0))) as cache_tokens
		FROM %s %s
		GROUP BY model_name ORDER BY (prompt_tokens + completion_tokens) DESC LIMIT %d
	`,
		attrStr("gen.ai.request.model"),
		attrInt("gen.ai.usage.input_tokens"),
		attrInt("gen.ai.usage.output_tokens"),
		attrInt("ai.tokens_system"),
		attrInt("gen.ai.usage.cache_read_input_tokens"),
		tableMetrics, where, b.limit)
	return query, args
}

// --- PII Category ---

type PIICategoryQueryBuilder struct{ *BaseQueryBuilder }

func NewPIICategoryQueryBuilder(teamID int64, startMs, endMs int64) *PIICategoryQueryBuilder {
	return &PIICategoryQueryBuilder{BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs)}
}

func (b *PIICategoryQueryBuilder) Build() (query string, args []any) {
	var where string
	where, args = b.baseWhereClause()
	piiDetected := attrInt("ai.security.pii_detected")

	query = fmt.Sprintf(`
		SELECT %s as model_name, %s as pii_categories, toInt64(COUNT(*)) as detection_count
		FROM %s %s AND %s = 1
		GROUP BY model_name, pii_categories ORDER BY detection_count DESC LIMIT %d
	`,
		attrStr("gen.ai.request.model"), attrStr("ai.security.pii_categories"),
		tableMetrics, where, piiDetected, b.limit)

	return query, args
}
