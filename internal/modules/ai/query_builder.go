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

// baseWhereClause returns the common WHERE clause and arguments
// Queries metrics table and filters for GenAI metrics using JSON attribute extraction
func (b *BaseQueryBuilder) baseWhereClause() (string, []any) {
	// Filter for metrics that have gen.ai.request.model attribute (indicates GenAI metrics)
	where := "WHERE team_id = ? AND timestamp BETWEEN ? AND ? AND JSONExtractString(attributes, 'gen.ai.request.model') <> ''"
	args := []any{b.teamUUID, dbutil.SqlTime(b.startMs), dbutil.SqlTime(b.endMs)}

	if b.modelName != "" {
		where += " AND JSONExtractString(attributes, 'gen.ai.request.model') = ?"
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

// Build constructs the performance metrics query using OpenTelemetry column names
func (b *PerformanceMetricsQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	// Calculate time range in seconds for QPS calculation
	timeRangeArgs := []any{dbutil.SqlTime(b.startMs), dbutil.SqlTime(b.endMs)}
	args = append(timeRangeArgs, args...)

	query := fmt.Sprintf(`
		SELECT JSONExtractString(attributes, 'gen.ai.request.model') as model_name,
		       JSONExtractString(attributes, 'server.address') as model_provider,
		       JSONExtractString(attributes, 'gen.ai.operation.name') as request_type,
		       COUNT(*) as total_requests,
		       COUNT(*) / GREATEST(dateDiff('second', ?, ?), 1) as avg_qps,
		       AVG(CAST(JSONExtractString(attributes, 'duration_ms') AS Float64)) as avg_latency_ms,
		       AVG(CAST(JSONExtractString(attributes, 'duration_ms') AS Float64)) as p50_latency_ms,
		       AVG(CAST(JSONExtractString(attributes, 'duration_ms') AS Float64)) as p95_latency_ms,
		       AVG(CAST(JSONExtractString(attributes, 'duration_ms') AS Float64)) as p99_latency_ms,
		       MAX(CAST(JSONExtractString(attributes, 'duration_ms') AS Float64)) as max_latency_ms,
		       SUM(CASE WHEN JSONExtractInt(attributes, 'ai.timeout')=1 THEN 1 ELSE 0 END) as timeout_count,
		       SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END) as error_count,
		       IF(COUNT(*)>0, SUM(CASE WHEN JSONExtractInt(attributes, 'ai.timeout')=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as timeout_rate,
		       IF(COUNT(*)>0, SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as error_rate,
		       AVG(CASE WHEN CAST(JSONExtractString(attributes, 'duration_ms') AS Float64)>0 THEN COALESCE(JSONExtractInt(attributes, 'gen.ai.usage.output_tokens'),0)/(CAST(JSONExtractString(attributes, 'duration_ms') AS Float64)/1000.0) ELSE 0 END) as avg_tokens_per_sec,
		       AVG(COALESCE(JSONExtractInt(attributes, 'ai.retry_count'),0)) as avg_retry_count
		FROM metrics_v5
		%s
		GROUP BY model_name, model_provider, request_type
		ORDER BY total_requests DESC
		LIMIT %d
	`, where, b.limit)

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

// Build constructs the cost metrics query using OpenTelemetry column names
func (b *CostMetricsQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	query := fmt.Sprintf(`
		SELECT JSONExtractString(attributes, 'gen.ai.request.model') as model_name,
		       JSONExtractString(attributes, 'server.address') as model_provider,
		       COUNT(*) as total_requests,
		       SUM(COALESCE(JSONExtractFloat(attributes, 'ai.cost_usd'),0)) as total_cost_usd,
		       AVG(COALESCE(JSONExtractFloat(attributes, 'ai.cost_usd'),0)) as avg_cost_per_query,
		       MAX(COALESCE(JSONExtractFloat(attributes, 'ai.cost_usd'),0)) as max_cost_per_query,
		       SUM(COALESCE(JSONExtractInt(attributes, 'gen.ai.usage.input_tokens'),0)) as total_prompt_tokens,
		       SUM(COALESCE(JSONExtractInt(attributes, 'gen.ai.usage.output_tokens'),0)) as total_completion_tokens,
		       SUM(COALESCE(JSONExtractInt(attributes, 'gen.ai.usage.input_tokens'),0)+COALESCE(JSONExtractInt(attributes, 'gen.ai.usage.output_tokens'),0)) as total_tokens,
		       AVG(COALESCE(JSONExtractInt(attributes, 'gen.ai.usage.input_tokens'),0)) as avg_prompt_tokens,
		       AVG(COALESCE(JSONExtractInt(attributes, 'gen.ai.usage.output_tokens'),0)) as avg_completion_tokens,
		       IF(COUNT(*)>0, SUM(CASE WHEN JSONExtractInt(attributes, 'ai.cache_hit')=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as cache_hit_rate,
		       SUM(COALESCE(JSONExtractInt(attributes, 'gen.ai.usage.cache_read_input_tokens'),0)) as total_cache_tokens
		FROM metrics_v5
		%s
		GROUP BY model_name, model_provider
		ORDER BY total_cost_usd DESC
		LIMIT %d
	`, where, b.limit)

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

// Build constructs the security metrics query using OpenTelemetry column names
func (b *SecurityMetricsQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	query := fmt.Sprintf(`
		SELECT JSONExtractString(attributes, 'gen.ai.request.model') as model_name,
		       JSONExtractString(attributes, 'server.address') as model_provider,
		       COUNT(*) as total_requests,
		       SUM(CASE WHEN JSONExtractInt(attributes, 'ai.security.pii_detected')=1 THEN 1 ELSE 0 END) as pii_detected_count,
		       IF(COUNT(*)>0, SUM(CASE WHEN JSONExtractInt(attributes, 'ai.security.pii_detected')=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as pii_detection_rate,
		       SUM(CASE WHEN JSONExtractInt(attributes, 'ai.security.guardrail_blocked')=1 THEN 1 ELSE 0 END) as guardrail_blocked_count,
		       IF(COUNT(*)>0, SUM(CASE WHEN JSONExtractInt(attributes, 'ai.security.guardrail_blocked')=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as guardrail_block_rate,
		       SUM(CASE WHEN JSONExtractInt(attributes, 'ai.security.content_policy')=1 THEN 1 ELSE 0 END) as content_policy_count,
		       IF(COUNT(*)>0, SUM(CASE WHEN JSONExtractInt(attributes, 'ai.security.content_policy')=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as content_policy_rate
		FROM metrics_v5
		%s
		GROUP BY model_name, model_provider
		ORDER BY pii_detected_count DESC
		LIMIT %d
	`, where, b.limit)

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
		orderBy:          "timestamp ASC",
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

// Build constructs the time series query extracting GenAI attributes from JSON
func (b *TimeSeriesQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	bucketExpr := b.bucketStrategy.GetBucketExpression()

	selectClause := strings.Join(b.selectFields, ",\n       ")
	groupByClause := "model_name, " + bucketExpr
	if len(b.groupByFields) > 0 {
		groupByClause = strings.Join(append([]string{"model_name", bucketExpr}, b.groupByFields...), ", ")
	}

	query := fmt.Sprintf(`
		SELECT JSONExtractString(attributes, 'gen.ai.request.model') as model_name,
		       %s as timestamp,
		       %s
		FROM metrics_v5
		%s
		GROUP BY %s
		ORDER BY %s
	`, bucketExpr, selectClause, where, groupByClause, b.orderBy)

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

// Build constructs the latency histogram query extracting GenAI attributes from JSON
func (b *LatencyHistogramQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	query := fmt.Sprintf(`
		SELECT JSONExtractString(attributes, 'gen.ai.request.model') as model_name,
		       FLOOR(CAST(JSONExtractString(attributes, 'duration_ms') AS Float64) / %d) * %d as bucket_ms,
		       COUNT(*) as request_count
		FROM metrics_v5
		%s
		GROUP BY model_name, bucket_ms
		ORDER BY model_name, bucket_ms ASC
		LIMIT %d
	`, b.bucketSizeMs, b.bucketSizeMs, where, b.limit)

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

// Build constructs the summary query extracting GenAI attributes from JSON
func (b *SummaryQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	// Calculate time range in seconds for QPS calculation
	timeRangeArgs := []any{dbutil.SqlTime(b.startMs), dbutil.SqlTime(b.endMs)}
	args = append(timeRangeArgs, args...)

	query := fmt.Sprintf(`
		SELECT COUNT(*) as total_requests,
		       COUNT(*) / GREATEST(dateDiff('second', ?, ?), 1) as avg_qps,
		       AVG(CAST(JSONExtractString(attributes, 'duration_ms') AS Float64)) as avg_latency_ms,
		       AVG(CAST(JSONExtractString(attributes, 'duration_ms') AS Float64)) as p95_latency_ms,
		       SUM(CASE WHEN JSONExtractInt(attributes, 'ai.timeout') = 1 THEN 1 ELSE 0 END) as timeout_count,
		       SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END) as error_count,
		       SUM(COALESCE(JSONExtractInt(attributes, 'gen.ai.usage.input_tokens'),0) + COALESCE(JSONExtractInt(attributes, 'gen.ai.usage.output_tokens'),0)) as total_tokens,
		       SUM(COALESCE(JSONExtractFloat(attributes, 'ai.cost_usd'),0)) as total_cost_usd,
		       AVG(COALESCE(JSONExtractFloat(attributes, 'ai.cost_usd'),0)) as avg_cost_per_query,
		       IF(COUNT(*)>0, SUM(CASE WHEN JSONExtractInt(attributes, 'ai.cache_hit')=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as cache_hit_rate,
		       IF(COUNT(*)>0, SUM(CASE WHEN JSONExtractInt(attributes, 'ai.security.pii_detected')=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as pii_detection_rate,
		       IF(COUNT(*)>0, SUM(CASE WHEN JSONExtractInt(attributes, 'ai.security.guardrail_blocked')=1 THEN 1 ELSE 0 END)*100.0/COUNT(*), 0) as guardrail_block_rate,
		       AVG(CASE WHEN CAST(JSONExtractString(attributes, 'duration_ms') AS Float64) > 0 THEN COALESCE(JSONExtractInt(attributes, 'gen.ai.usage.output_tokens'),0) / (CAST(JSONExtractString(attributes, 'duration_ms') AS Float64)/1000.0) ELSE 0 END) as avg_tokens_per_sec,
		       COUNT(DISTINCT JSONExtractString(attributes, 'gen.ai.request.model')) as active_models
		FROM metrics_v5
		%s
	`, where)

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

// Build constructs the model list query extracting GenAI attributes from JSON
func (b *ModelListQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	query := fmt.Sprintf(`
		SELECT JSONExtractString(attributes, 'gen.ai.request.model') as model_name,
		       JSONExtractString(attributes, 'server.address') as model_provider
		FROM metrics_v5
		%s
		GROUP BY model_name, model_provider
		ORDER BY model_name ASC
		LIMIT %d
	`, where, b.limit)

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

// Build constructs the token breakdown query extracting GenAI attributes from JSON
func (b *TokenBreakdownQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	query := fmt.Sprintf(`
		SELECT JSONExtractString(attributes, 'gen.ai.request.model') as model_name,
		       SUM(COALESCE(JSONExtractInt(attributes, 'gen.ai.usage.input_tokens'),0)) as prompt_tokens,
		       SUM(COALESCE(JSONExtractInt(attributes, 'gen.ai.usage.output_tokens'),0)) as completion_tokens,
		       SUM(COALESCE(JSONExtractInt(attributes, 'ai.tokens_system'),0)) as system_tokens,
		       SUM(COALESCE(JSONExtractInt(attributes, 'gen.ai.usage.cache_read_input_tokens'),0)) as cache_tokens
		FROM metrics_v5
		%s
		GROUP BY model_name
		ORDER BY (prompt_tokens + completion_tokens) DESC
		LIMIT %d
	`, where, b.limit)

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

// Build constructs the PII category query extracting GenAI attributes from JSON
func (b *PIICategoryQueryBuilder) Build() (string, []any) {
	where, args := b.baseWhereClause()

	query := fmt.Sprintf(`
		SELECT JSONExtractString(attributes, 'gen.ai.request.model') as model_name,
		       JSONExtractString(attributes, 'ai.security.pii_categories') as pii_categories,
		       COUNT(*) as detection_count
		FROM metrics_v5
		%s AND JSONExtractInt(attributes, 'ai.security.pii_detected') = 1
		GROUP BY model_name, pii_categories
		ORDER BY detection_count DESC
		LIMIT %d
	`, where, b.limit)

	return query, args
}
