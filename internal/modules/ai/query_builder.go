package ai

import (
	"fmt"
	"strings"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// QueryBuilder defines the interface for building SQL queries
// Following Interface Segregation Principle - clients depend only on methods they use
type QueryBuilder interface {
	Build() (string, []any)
}

// BaseQueryBuilder provides common query building functionality
// Following Single Responsibility Principle - handles base query construction
type BaseQueryBuilder struct {
	teamUUID  string
	startMs   int64
	endMs     int64
	modelName string
	limit     int
}

// NewBaseQueryBuilder creates a new base query builder
func NewBaseQueryBuilder(teamUUID string, startMs, endMs int64) *BaseQueryBuilder {
	return &BaseQueryBuilder{
		teamUUID: teamUUID,
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

// attrStr returns a CH 26+ native JSON path expression reading a String from attributes.
func attrStr(key string) string {
	return "attributes.'" + key + "'::String"
}

// attrInt returns a CH 26+ native JSON path expression reading an Int64 from attributes.
func attrInt(key string) string {
	return "attributes.'" + key + "'::Int64"
}

// attrFlt returns a CH 26+ native JSON path expression reading a Float64 from attributes.
func attrFlt(key string) string {
	return "attributes.'" + key + "'::Float64"
}

// baseWhereClause returns the common WHERE clause and arguments.
// Filters for metrics that carry a gen.ai.request.model attribute (GenAI metrics).
func (b *BaseQueryBuilder) baseWhereClause() (string, []any) {
	modelExpr := attrStr("gen.ai.request.model")
	where := fmt.Sprintf("WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND %s <> ''", modelExpr)
	args := []any{b.teamUUID, dbutil.SqlTime(b.startMs), dbutil.SqlTime(b.endMs)}

	if b.modelName != "" {
		where += fmt.Sprintf(" AND %s = ?", modelExpr)
		args = append(args, b.modelName)
	}

	return where, args
}

// PerformanceMetricsQueryBuilder builds queries for performance metrics
// Following Single Responsibility Principle - only handles performance metric queries
type PerformanceMetricsQueryBuilder struct {
	*BaseQueryBuilder
	includePercentiles bool
}

// NewPerformanceMetricsQueryBuilder creates a new performance metrics query builder
func NewPerformanceMetricsQueryBuilder(teamUUID string, startMs, endMs int64) *PerformanceMetricsQueryBuilder {
	return &PerformanceMetricsQueryBuilder{
		BaseQueryBuilder:   NewBaseQueryBuilder(teamUUID, startMs, endMs),
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

// CostMetricsQueryBuilder builds queries for cost metrics
// Following Single Responsibility Principle - only handles cost metric queries
type CostMetricsQueryBuilder struct {
	*BaseQueryBuilder
}

// NewCostMetricsQueryBuilder creates a new cost metrics query builder
func NewCostMetricsQueryBuilder(teamUUID string, startMs, endMs int64) *CostMetricsQueryBuilder {
	return &CostMetricsQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamUUID, startMs, endMs),
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

// SecurityMetricsQueryBuilder builds queries for security metrics
// Following Single Responsibility Principle - only handles security metric queries
type SecurityMetricsQueryBuilder struct {
	*BaseQueryBuilder
}

// NewSecurityMetricsQueryBuilder creates a new security metrics query builder
func NewSecurityMetricsQueryBuilder(teamUUID string, startMs, endMs int64) *SecurityMetricsQueryBuilder {
	return &SecurityMetricsQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamUUID, startMs, endMs),
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

// TimeSeriesQueryBuilder builds time series queries with adaptive bucketing
// Following Single Responsibility Principle - only handles time series queries
type TimeSeriesQueryBuilder struct {
	*BaseQueryBuilder
	bucketStrategy TimeBucketStrategy
	selectFields   []string
	groupByFields  []string
	orderBy        string
}

// NewTimeSeriesQueryBuilder creates a new time series query builder
func NewTimeSeriesQueryBuilder(teamUUID string, startMs, endMs int64, strategy TimeBucketStrategy) *TimeSeriesQueryBuilder {
	return &TimeSeriesQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamUUID, startMs, endMs),
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

// LatencyHistogramQueryBuilder builds latency histogram queries
// Following Single Responsibility Principle - only handles histogram queries
type LatencyHistogramQueryBuilder struct {
	*BaseQueryBuilder
	bucketSizeMs int64
}

// NewLatencyHistogramQueryBuilder creates a new latency histogram query builder
func NewLatencyHistogramQueryBuilder(teamUUID string, startMs, endMs int64) *LatencyHistogramQueryBuilder {
	return &LatencyHistogramQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamUUID, startMs, endMs).WithLimit(200),
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

// SummaryQueryBuilder builds queries for AI summary metrics
// Following Single Responsibility Principle - only handles summary queries
type SummaryQueryBuilder struct {
	*BaseQueryBuilder
}

// NewSummaryQueryBuilder creates a new summary query builder
func NewSummaryQueryBuilder(teamUUID string, startMs, endMs int64) *SummaryQueryBuilder {
	return &SummaryQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamUUID, startMs, endMs),
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

// ModelListQueryBuilder builds queries for listing AI models
// Following Single Responsibility Principle - only handles model list queries
type ModelListQueryBuilder struct {
	*BaseQueryBuilder
}

// NewModelListQueryBuilder creates a new model list query builder
func NewModelListQueryBuilder(teamUUID string, startMs, endMs int64) *ModelListQueryBuilder {
	return &ModelListQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamUUID, startMs, endMs),
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

// TokenBreakdownQueryBuilder builds queries for token breakdown
// Following Single Responsibility Principle - only handles token breakdown queries
type TokenBreakdownQueryBuilder struct {
	*BaseQueryBuilder
}

// NewTokenBreakdownQueryBuilder creates a new token breakdown query builder
func NewTokenBreakdownQueryBuilder(teamUUID string, startMs, endMs int64) *TokenBreakdownQueryBuilder {
	return &TokenBreakdownQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamUUID, startMs, endMs).WithLimit(50),
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

// PIICategoryQueryBuilder builds queries for PII category breakdown
// Following Single Responsibility Principle - only handles PII category queries
type PIICategoryQueryBuilder struct {
	*BaseQueryBuilder
}

// NewPIICategoryQueryBuilder creates a new PII category query builder
func NewPIICategoryQueryBuilder(teamUUID string, startMs, endMs int64) *PIICategoryQueryBuilder {
	return &PIICategoryQueryBuilder{
		BaseQueryBuilder: NewBaseQueryBuilder(teamUUID, startMs, endMs),
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
