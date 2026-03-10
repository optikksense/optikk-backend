package ai

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// Repository encapsulates data access logic for AI models following OpenTelemetry GenAI semantic conventions.
// This interface follows the Dependency Inversion Principle - high-level modules depend on abstractions.
// Implementation details are hidden behind this interface, allowing for easy testing and swapping of implementations.

// Repository encapsulates data access logic for AI models.
type Repository interface {
	GetAISummary(teamID int64, startMs, endMs int64) (*AISummary, error)
	GetAIModels(teamID int64, startMs, endMs int64) ([]AIModel, error)
	GetAIPerformanceMetrics(teamID int64, startMs, endMs int64) ([]AIPerformanceMetric, error)
	GetAIPerformanceTimeSeries(teamID int64, startMs, endMs int64) ([]AIPerformanceTimeSeries, error)
	GetAILatencyHistogram(teamID int64, modelName string, startMs, endMs int64) ([]AILatencyHistogram, error)
	GetAICostMetrics(teamID int64, startMs, endMs int64) ([]AICostMetric, error)
	GetAICostTimeSeries(teamID int64, startMs, endMs int64) ([]AICostTimeSeries, error)
	GetAITokenBreakdown(teamID int64, startMs, endMs int64) ([]AITokenBreakdown, error)
	GetAISecurityMetrics(teamID int64, startMs, endMs int64) ([]AISecurityMetric, error)
	GetAISecurityTimeSeries(teamID int64, startMs, endMs int64) ([]AISecurityTimeSeries, error)
	GetAIPiiCategories(teamID int64, startMs, endMs int64) ([]AIPiiCategory, error)
}

// ClickHouseRepository encapsulates data access logic for AI models.
// Following Single Responsibility Principle - only handles data access, delegates query building and aggregation.
type ClickHouseRepository struct {
	db                  dbutil.Querier
	bucketFactory       *TimeBucketStrategyFactory
	summaryAggregator   *SummaryAggregator
	perfAggregator      *PerformanceMetricAggregator
	costAggregator      *CostMetricAggregator
	securityAggregator  *SecurityMetricAggregator
	histogramAggregator *HistogramAggregator
	modelListAggregator *ModelListAggregator
	tokenAggregator     *TokenBreakdownAggregator
	piiAggregator       *PIICategoryAggregator
}

// NewRepository creates a new AI Repository with all necessary dependencies.
// Following Dependency Injection principle for better testability.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{
		db:                  db,
		bucketFactory:       NewTimeBucketStrategyFactory(),
		summaryAggregator:   NewSummaryAggregator(),
		perfAggregator:      NewPerformanceMetricAggregator(),
		costAggregator:      NewCostMetricAggregator(),
		securityAggregator:  NewSecurityMetricAggregator(),
		histogramAggregator: NewHistogramAggregator(),
		modelListAggregator: NewModelListAggregator(),
		tokenAggregator:     NewTokenBreakdownAggregator(),
		piiAggregator:       NewPIICategoryAggregator(),
	}
}

// GetAISummary returns an aggregate performance/cost/security summary for all AI models.
// Refactored to use SummaryQueryBuilder and SummaryAggregator following Single Responsibility Principle.
func (r *ClickHouseRepository) GetAISummary(teamID int64, startMs, endMs int64) (*AISummary, error) {
	// Build query using dedicated query builder
	builder := NewSummaryQueryBuilder(teamID, startMs, endMs)
	query, args := builder.Build()

	// Execute query
	row, err := dbutil.QueryMap(r.db, query, args...)
	if err != nil || len(row) == 0 {
		return nil, err
	}

	// Aggregate results using dedicated aggregator
	result, err := r.summaryAggregator.Aggregate([]map[string]any{row})
	if err != nil {
		return nil, err
	}

	return result.(*AISummary), nil
}

// GetAIModels returns distinct AI models active in the time window.
// Refactored to use ModelListQueryBuilder and ModelListAggregator following Single Responsibility Principle.
func (r *ClickHouseRepository) GetAIModels(teamID int64, startMs, endMs int64) ([]AIModel, error) {
	// Build query using dedicated query builder
	builder := NewModelListQueryBuilder(teamID, startMs, endMs)
	query, args := builder.Build()

	// Execute query
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	// Aggregate results using dedicated aggregator
	result, err := r.modelListAggregator.Aggregate(rows)
	if err != nil {
		return nil, err
	}

	return result.([]AIModel), nil
}

// GetAIPerformanceMetrics returns per-model latency, throughput, error and timeout rates.
// Refactored to use QueryBuilder and Aggregator following Single Responsibility Principle.
func (r *ClickHouseRepository) GetAIPerformanceMetrics(teamID int64, startMs, endMs int64) ([]AIPerformanceMetric, error) {
	// Build query using dedicated query builder
	builder := NewPerformanceMetricsQueryBuilder(teamID, startMs, endMs)
	query, args := builder.Build()

	// Execute query
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	// Aggregate results using dedicated aggregator
	result, err := r.perfAggregator.Aggregate(rows)
	if err != nil {
		return nil, err
	}

	return result.([]AIPerformanceMetric), nil
}

// GetAIPerformanceTimeSeries returns per-model latency / throughput time series with adaptive bucketing.
// Refactored to use TimeSeriesQueryBuilder and TimeSeriesAggregator following Single Responsibility Principle.
func (r *ClickHouseRepository) GetAIPerformanceTimeSeries(teamID int64, startMs, endMs int64) ([]AIPerformanceTimeSeries, error) {
	// Create adaptive time bucket strategy
	strategy := r.bucketFactory.CreateStrategy(startMs, endMs)
	durationMs := attrFlt("duration_ms")
	outputTokens := attrInt("gen.ai.usage.output_tokens")
	timeout := attrInt("ai.timeout")

	// Build query using time series query builder
	builder := NewTimeSeriesQueryBuilder(teamID, startMs, endMs, strategy)
	builder.WithSelectFields([]string{
		"COUNT(*) as request_count",
		fmt.Sprintf("AVG(%s) as avg_latency_ms", durationMs),
		fmt.Sprintf("quantile(0.95)(%s) as p95_latency_ms", durationMs),
		fmt.Sprintf("SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) as timeout_count", timeout),
		"countIf(has_error) as error_count",
		fmt.Sprintf("AVG(CASE WHEN %s > 0 THEN COALESCE(%s, 0) / (%s / 1000.0) ELSE 0 END) as tokens_per_sec", durationMs, outputTokens, durationMs),
	})

	query, args := builder.Build()

	// Execute query
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	// Aggregate results using time series aggregator
	aggregator := NewTimeSeriesAggregator("performance")
	result, err := aggregator.Aggregate(rows)
	if err != nil {
		return nil, err
	}

	return result.([]AIPerformanceTimeSeries), nil
}

// GetAILatencyHistogram returns latency distribution (100ms buckets) per model.
// Refactored to use LatencyHistogramQueryBuilder and HistogramAggregator following Single Responsibility Principle.
func (r *ClickHouseRepository) GetAILatencyHistogram(teamID int64, modelName string, startMs, endMs int64) ([]AILatencyHistogram, error) {
	// Build query using dedicated query builder
	builder := NewLatencyHistogramQueryBuilder(teamID, startMs, endMs)
	if modelName != "" {
		builder.WithModelName(modelName)
	}
	query, args := builder.Build()

	// Execute query
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	// Aggregate results using dedicated aggregator
	result, err := r.histogramAggregator.Aggregate(rows)
	if err != nil {
		return nil, err
	}

	return result.([]AILatencyHistogram), nil
}

// GetAICostMetrics returns per-model token usage and cost breakdown.
// Refactored to use CostMetricsQueryBuilder and CostMetricAggregator following Single Responsibility Principle.
func (r *ClickHouseRepository) GetAICostMetrics(teamID int64, startMs, endMs int64) ([]AICostMetric, error) {
	// Build query using dedicated query builder
	builder := NewCostMetricsQueryBuilder(teamID, startMs, endMs)
	query, args := builder.Build()

	// Execute query
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	// Aggregate results using dedicated aggregator
	result, err := r.costAggregator.Aggregate(rows)
	if err != nil {
		return nil, err
	}

	return result.([]AICostMetric), nil
}

// GetAICostTimeSeries returns cost and token usage over time per model with adaptive bucketing.
// Refactored to use TimeSeriesQueryBuilder and TimeSeriesAggregator following Single Responsibility Principle.
func (r *ClickHouseRepository) GetAICostTimeSeries(teamID int64, startMs, endMs int64) ([]AICostTimeSeries, error) {
	// Create adaptive time bucket strategy
	strategy := r.bucketFactory.CreateStrategy(startMs, endMs)
	costUSD := attrFlt("ai.cost_usd")
	inputTokens := attrInt("gen.ai.usage.input_tokens")
	outputTokens := attrInt("gen.ai.usage.output_tokens")

	// Build query using time series query builder
	builder := NewTimeSeriesQueryBuilder(teamID, startMs, endMs, strategy)
	builder.WithSelectFields([]string{
		fmt.Sprintf("SUM(COALESCE(%s, 0)) as cost_per_interval", costUSD),
		fmt.Sprintf("SUM(COALESCE(%s, 0)) as prompt_tokens", inputTokens),
		fmt.Sprintf("SUM(COALESCE(%s, 0)) as completion_tokens", outputTokens),
		"COUNT(*) as request_count",
	})

	query, args := builder.Build()

	// Execute query
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	// Aggregate results using time series aggregator
	aggregator := NewTimeSeriesAggregator("cost")
	result, err := aggregator.Aggregate(rows)
	if err != nil {
		return nil, err
	}

	return result.([]AICostTimeSeries), nil
}

// GetAITokenBreakdown returns token type breakdown per model.
// Refactored to use TokenBreakdownQueryBuilder and TokenBreakdownAggregator following Single Responsibility Principle.
func (r *ClickHouseRepository) GetAITokenBreakdown(teamID int64, startMs, endMs int64) ([]AITokenBreakdown, error) {
	// Build query using dedicated query builder
	builder := NewTokenBreakdownQueryBuilder(teamID, startMs, endMs)
	query, args := builder.Build()

	// Execute query
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	// Aggregate results using dedicated aggregator
	result, err := r.tokenAggregator.Aggregate(rows)
	if err != nil {
		return nil, err
	}

	return result.([]AITokenBreakdown), nil
}

// GetAISecurityMetrics returns PII detection and guardrail block rates per model.
// Refactored to use SecurityMetricsQueryBuilder and SecurityMetricAggregator following Single Responsibility Principle.
func (r *ClickHouseRepository) GetAISecurityMetrics(teamID int64, startMs, endMs int64) ([]AISecurityMetric, error) {
	// Build query using dedicated query builder
	builder := NewSecurityMetricsQueryBuilder(teamID, startMs, endMs)
	query, args := builder.Build()

	// Execute query
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	// Aggregate results using dedicated aggregator
	result, err := r.securityAggregator.Aggregate(rows)
	if err != nil {
		return nil, err
	}

	return result.([]AISecurityMetric), nil
}

// GetAISecurityTimeSeries returns security-event time series per model with adaptive bucketing.
// Refactored to use TimeSeriesQueryBuilder and TimeSeriesAggregator following Single Responsibility Principle.
func (r *ClickHouseRepository) GetAISecurityTimeSeries(teamID int64, startMs, endMs int64) ([]AISecurityTimeSeries, error) {
	// Create adaptive time bucket strategy
	strategy := r.bucketFactory.CreateStrategy(startMs, endMs)
	piiDetected := attrInt("ai.security.pii_detected")
	guardrailBlocked := attrInt("ai.security.guardrail_blocked")
	contentPolicy := attrInt("ai.security.content_policy")

	// Build query using time series query builder
	builder := NewTimeSeriesQueryBuilder(teamID, startMs, endMs, strategy)
	builder.WithSelectFields([]string{
		"COUNT(*) as total_requests",
		fmt.Sprintf("SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) as pii_count", piiDetected),
		fmt.Sprintf("SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) as guardrail_count", guardrailBlocked),
		fmt.Sprintf("SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) as content_policy_count", contentPolicy),
	})

	query, args := builder.Build()

	// Execute query
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	// Aggregate results using time series aggregator
	aggregator := NewTimeSeriesAggregator("security")
	result, err := aggregator.Aggregate(rows)
	if err != nil {
		return nil, err
	}

	return result.([]AISecurityTimeSeries), nil
}

// GetAIPiiCategories returns PII category breakdown for detected events.
// Refactored to use PIICategoryQueryBuilder and PIICategoryAggregator following Single Responsibility Principle.
func (r *ClickHouseRepository) GetAIPiiCategories(teamID int64, startMs, endMs int64) ([]AIPiiCategory, error) {
	// Build query using dedicated query builder
	builder := NewPIICategoryQueryBuilder(teamID, startMs, endMs)
	query, args := builder.Build()

	// Execute query
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	// Aggregate results using dedicated aggregator
	result, err := r.piiAggregator.Aggregate(rows)
	if err != nil {
		return nil, err
	}

	return result.([]AIPiiCategory), nil
}
