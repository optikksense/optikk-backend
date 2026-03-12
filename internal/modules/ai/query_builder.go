package ai

import (
	"fmt"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type QueryBuilder interface {
	Build() (string, []any)
}

type BaseQueryBuilder struct {
	teamID    int64
	startMs   int64
	endMs     int64
	modelName string
	limit     int
}

func NewBaseQueryBuilder(teamID int64, startMs, endMs int64) *BaseQueryBuilder {
	return &BaseQueryBuilder{
		teamID: teamID,
		startMs:  startMs,
		endMs:    endMs,
		limit:    100,
	}
}

// WithModelName adds model name filter
func (b *BaseQueryBuilder) WithModelName(modelName string) *BaseQueryBuilder {
	b.modelName = modelName
	return b
}

// WithLimit sets the query limit
func (b *BaseQueryBuilder) WithLimit(limit int) *BaseQueryBuilder {
	b.limit = limit
	return b
}

func attrStr(key string) string {
	return "attributes.'" + key + "'::String"
}

func attrInt(key string) string {
	return "attributes.'" + key + "'::Int64"
}

func attrFlt(key string) string {
	return "attributes.'" + key + "'::Float64"
}

func (b *BaseQueryBuilder) baseWhereClause() (string, []any) {
	modelExpr := attrStr("gen.ai.request.model")
	where := fmt.Sprintf("WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND %s <> ''", modelExpr)
	args := []any{b.teamID, dbutil.SqlTime(b.startMs), dbutil.SqlTime(b.endMs)}

	if b.modelName != "" {
		where += fmt.Sprintf(" AND %s = ?", modelExpr)
		args = append(args, b.modelName)
	}

	return where, args
}

type PerformanceMetricsQueryBuilder struct {
	*BaseQueryBuilder
	includePercentiles bool
}

func NewPerformanceMetricsQueryBuilder(teamID int64, startMs, endMs int64) *PerformanceMetricsQueryBuilder {
	return &PerformanceMetricsQueryBuilder{
		BaseQueryBuilder:   NewBaseQueryBuilder(teamID, startMs, endMs),
		includePercentiles: true,
	}
}

// Build constructs the performance metrics query
func (b *PerformanceMetricsQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	// Calculate time range in seconds for QPS calculation
	timeRangeArgs := []any{dbutil.SqlTime(b.startMs), dbutil.SqlTime(b.endMs)}
	args = append(timeRangeArgs, args...)

	durationMs := attrFlt("duration_ms")
	outputTokens := attrInt("gen.ai.usage.output_tokens")
	retryCount := attrInt("ai.retry_count")
	aiTimeout := attrInt("ai.timeout")

	query := fmt.Sprintf(`
		SELECT %s as model_name,
		       %s as model_provider,
		       %s as request_type,
		       COUNT(*) as total_requests,
		       COUNT(*) / GREATEST(dateDiff('second', ?, ?), 1) as avg_qps,
		       AVG(%s) as avg_latency_ms,
		       AVG(%s) as p50_latency_ms,
		       AVG(%s) as p95_latency_ms,
		       AVG(%s) as p99_latency_ms,
		       MAX(%s) as max_latency_ms,
		       SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) as timeout_count,
		       countIf(has_error) as error_count,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as timeout_rate,
		       IF(COUNT(*) > 0, countIf(has_error) * 100.0 / COUNT(*), 0) as error_rate,
		       AVG(CASE WHEN %s > 0 THEN COALESCE(%s, 0) / (%s / 1000.0) ELSE 0 END) as avg_tokens_per_sec,
		       AVG(COALESCE(%s, 0)) as avg_retry_count
		FROM %s
		%s
		GROUP BY model_name, model_provider, request_type
		ORDER BY total_requests DESC
		LIMIT %d
	`,
		attrStr("gen.ai.request.model"),
		attrStr("server.address"),
		attrStr("gen.ai.operation.name"),
		durationMs, durationMs, durationMs, durationMs, durationMs,
		aiTimeout,
		aiTimeout,
		durationMs, outputTokens, durationMs,
		retryCount,
		TableMetrics, where, b.limit)

	return query, args
}

type CostMetricsQueryBuilder struct {
	*BaseQueryBuilder
}

func NewCostMetricsQueryBuilder(teamID int64, startMs, endMs int64) *CostMetricsQueryBuilder {
	return &CostMetricsQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs),
	}
}

// Build constructs the cost metrics query
func (b *CostMetricsQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	costUSD := attrFlt("ai.cost_usd")
	inputTokens := attrInt("gen.ai.usage.input_tokens")
	outputTokens := attrInt("gen.ai.usage.output_tokens")
	cacheReadTokens := attrInt("gen.ai.usage.cache_read_input_tokens")
	cacheHit := attrInt("ai.cache_hit")

	query := fmt.Sprintf(`
		SELECT %s as model_name,
		       %s as model_provider,
		       COUNT(*) as total_requests,
		       SUM(COALESCE(%s, 0)) as total_cost_usd,
		       AVG(COALESCE(%s, 0)) as avg_cost_per_query,
		       MAX(COALESCE(%s, 0)) as max_cost_per_query,
		       SUM(COALESCE(%s, 0)) as total_prompt_tokens,
		       SUM(COALESCE(%s, 0)) as total_completion_tokens,
		       SUM(COALESCE(%s, 0) + COALESCE(%s, 0)) as total_tokens,
		       AVG(COALESCE(%s, 0)) as avg_prompt_tokens,
		       AVG(COALESCE(%s, 0)) as avg_completion_tokens,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as cache_hit_rate,
		       SUM(COALESCE(%s, 0)) as total_cache_tokens
		FROM %s
		%s
		GROUP BY model_name, model_provider
		ORDER BY total_cost_usd DESC
		LIMIT %d
	`,
		attrStr("gen.ai.request.model"),
		attrStr("server.address"),
		costUSD, costUSD, costUSD,
		inputTokens,
		outputTokens,
		inputTokens, outputTokens,
		inputTokens,
		outputTokens,
		cacheHit,
		cacheReadTokens,
		TableMetrics, where, b.limit)

	return query, args
}

type SecurityMetricsQueryBuilder struct {
	*BaseQueryBuilder
}

func NewSecurityMetricsQueryBuilder(teamID int64, startMs, endMs int64) *SecurityMetricsQueryBuilder {
	return &SecurityMetricsQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs),
	}
}

// Build constructs the security metrics query
func (b *SecurityMetricsQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	piiDetected := attrInt("ai.security.pii_detected")
	guardrailBlocked := attrInt("ai.security.guardrail_blocked")
	contentPolicy := attrInt("ai.security.content_policy")

	query := fmt.Sprintf(`
		SELECT %s as model_name,
		       %s as model_provider,
		       COUNT(*) as total_requests,
		       SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) as pii_detected_count,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as pii_detection_rate,
		       SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) as guardrail_blocked_count,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as guardrail_block_rate,
		       SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) as content_policy_count,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as content_policy_rate
		FROM %s
		%s
		GROUP BY model_name, model_provider
		ORDER BY pii_detected_count DESC
		LIMIT %d
	`,
		attrStr("gen.ai.request.model"),
		attrStr("server.address"),
		piiDetected, piiDetected,
		guardrailBlocked, guardrailBlocked,
		contentPolicy, contentPolicy,
		TableMetrics, where, b.limit)

	return query, args
}

type TimeSeriesQueryBuilder struct {
	*BaseQueryBuilder
	bucketStrategy TimeBucketStrategy
	selectFields   []string
	groupByFields  []string
	orderBy        string
}

func NewTimeSeriesQueryBuilder(teamID int64, startMs, endMs int64, strategy TimeBucketStrategy) *TimeSeriesQueryBuilder {
	return &TimeSeriesQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs),
		bucketStrategy:   strategy,
		orderBy:          "time_bucket ASC, model_name ASC",
	}
}

// WithSelectFields sets the SELECT fields
func (b *TimeSeriesQueryBuilder) WithSelectFields(fields []string) *TimeSeriesQueryBuilder {
	b.selectFields = fields
	return b
}

// WithGroupByFields sets additional GROUP BY fields
func (b *TimeSeriesQueryBuilder) WithGroupByFields(fields []string) *TimeSeriesQueryBuilder {
	b.groupByFields = fields
	return b
}

// Build constructs the time series query
func (b *TimeSeriesQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	bucketExpr := b.bucketStrategy.GetBucketExpression()

	selectClause := strings.Join(b.selectFields, ",\n       ")
	groupByClause := "model_name, " + bucketExpr
	if len(b.groupByFields) > 0 {
		groupByClause = strings.Join(append([]string{"model_name", bucketExpr}, b.groupByFields...), ", ")
	}

	query := fmt.Sprintf(`
		SELECT %s as model_name,
		       %s as time_bucket,
		       %s
		FROM %s
		%s
		GROUP BY %s
		ORDER BY %s
	`, attrStr("gen.ai.request.model"), bucketExpr, selectClause, TableMetrics, where, groupByClause, b.orderBy)

	return query, args
}

type LatencyHistogramQueryBuilder struct {
	*BaseQueryBuilder
	bucketSizeMs int64
}

func NewLatencyHistogramQueryBuilder(teamID int64, startMs, endMs int64) *LatencyHistogramQueryBuilder {
	return &LatencyHistogramQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs).WithLimit(200),
		bucketSizeMs:     100, // 100ms buckets by default
	}
}

// WithBucketSize sets the histogram bucket size in milliseconds
func (b *LatencyHistogramQueryBuilder) WithBucketSize(sizeMs int64) *LatencyHistogramQueryBuilder {
	b.bucketSizeMs = sizeMs
	return b
}

// Build constructs the latency histogram query
func (b *LatencyHistogramQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	durationMs := attrFlt("duration_ms")

	query := fmt.Sprintf(`
		SELECT %s as model_name,
		       FLOOR(%s / %d) * %d as bucket_ms,
		       COUNT(*) as request_count
		FROM %s
		%s
		GROUP BY model_name, bucket_ms
		ORDER BY model_name, bucket_ms ASC
		LIMIT %d
	`, attrStr("gen.ai.request.model"), durationMs, b.bucketSizeMs, b.bucketSizeMs, TableMetrics, where, b.limit)

	return query, args
}

type SummaryQueryBuilder struct {
	*BaseQueryBuilder
}

func NewSummaryQueryBuilder(teamID int64, startMs, endMs int64) *SummaryQueryBuilder {
	return &SummaryQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs),
	}
}

// Build constructs the summary query
func (b *SummaryQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	// Calculate time range in seconds for QPS calculation
	timeRangeArgs := []any{dbutil.SqlTime(b.startMs), dbutil.SqlTime(b.endMs)}
	args = append(timeRangeArgs, args...)

	durationMs := attrFlt("duration_ms")
	inputTokens := attrInt("gen.ai.usage.input_tokens")
	outputTokens := attrInt("gen.ai.usage.output_tokens")
	costUSD := attrFlt("ai.cost_usd")
	cacheHit := attrInt("ai.cache_hit")
	aiTimeout := attrInt("ai.timeout")
	piiDetected := attrInt("ai.security.pii_detected")
	guardrailBlocked := attrInt("ai.security.guardrail_blocked")

	query := fmt.Sprintf(`
		SELECT COUNT(*) as total_requests,
		       COUNT(*) / GREATEST(dateDiff('second', ?, ?), 1) as avg_qps,
		       AVG(%s) as avg_latency_ms,
		       AVG(%s) as p95_latency_ms,
		       SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) as timeout_count,
		       countIf(has_error) as error_count,
		       SUM(COALESCE(%s, 0) + COALESCE(%s, 0)) as total_tokens,
		       SUM(COALESCE(%s, 0)) as total_cost_usd,
		       AVG(COALESCE(%s, 0)) as avg_cost_per_query,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as cache_hit_rate,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as pii_detection_rate,
		       IF(COUNT(*) > 0, SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) as guardrail_block_rate,
		       AVG(CASE WHEN %s > 0 THEN COALESCE(%s, 0) / (%s / 1000.0) ELSE 0 END) as avg_tokens_per_sec,
		       COUNT(DISTINCT %s) as active_models
		FROM %s
		%s
	`,
		durationMs, durationMs,
		aiTimeout,
		inputTokens, outputTokens,
		costUSD,
		costUSD,
		cacheHit,
		piiDetected,
		guardrailBlocked,
		durationMs, outputTokens, durationMs,
		attrStr("gen.ai.request.model"),
		TableMetrics, where)

	return query, args
}

type ModelListQueryBuilder struct {
	*BaseQueryBuilder
}

func NewModelListQueryBuilder(teamID int64, startMs, endMs int64) *ModelListQueryBuilder {
	return &ModelListQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs),
	}
}

// Build constructs the model list query
func (b *ModelListQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	query := fmt.Sprintf(`
		SELECT %s as model_name,
		       %s as model_provider
		FROM %s
		%s
		GROUP BY model_name, model_provider
		ORDER BY model_name ASC
		LIMIT %d
	`, attrStr("gen.ai.request.model"), attrStr("server.address"), TableMetrics, where, b.limit)

	return query, args
}

type TokenBreakdownQueryBuilder struct {
	*BaseQueryBuilder
}

func NewTokenBreakdownQueryBuilder(teamID int64, startMs, endMs int64) *TokenBreakdownQueryBuilder {
	return &TokenBreakdownQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs).WithLimit(50),
	}
}

// Build constructs the token breakdown query
func (b *TokenBreakdownQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	query := fmt.Sprintf(`
		SELECT %s as model_name,
		       SUM(COALESCE(%s, 0)) as prompt_tokens,
		       SUM(COALESCE(%s, 0)) as completion_tokens,
		       SUM(COALESCE(%s, 0)) as system_tokens,
		       SUM(COALESCE(%s, 0)) as cache_tokens
		FROM %s
		%s
		GROUP BY model_name
		ORDER BY (prompt_tokens + completion_tokens) DESC
		LIMIT %d
	`,
		attrStr("gen.ai.request.model"),
		attrInt("gen.ai.usage.input_tokens"),
		attrInt("gen.ai.usage.output_tokens"),
		attrInt("ai.tokens_system"),
		attrInt("gen.ai.usage.cache_read_input_tokens"),
		TableMetrics, where, b.limit)

	return query, args
}

type PIICategoryQueryBuilder struct {
	*BaseQueryBuilder
}

func NewPIICategoryQueryBuilder(teamID int64, startMs, endMs int64) *PIICategoryQueryBuilder {
	return &PIICategoryQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamID, startMs, endMs),
	}
}

// Build constructs the PII category query
func (b *PIICategoryQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	piiDetected := attrInt("ai.security.pii_detected")

	query := fmt.Sprintf(`
		SELECT %s as model_name,
		       %s as pii_categories,
		       COUNT(*) as detection_count
		FROM %s
		%s AND %s = 1
		GROUP BY model_name, pii_categories
		ORDER BY detection_count DESC
		LIMIT %d
	`,
		attrStr("gen.ai.request.model"),
		attrStr("ai.security.pii_categories"),
		TableMetrics, where, piiDetected, b.limit)

	return query, args
}
