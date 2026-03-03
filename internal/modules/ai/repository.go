package ai

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// Repository encapsulates data access logic for AI models following OpenTelemetry GenAI semantic conventions.
// This interface follows the Dependency Inversion Principle - high-level modules depend on abstractions.
// Implementation details are hidden behind this interface, allowing for easy testing and swapping of implementations.

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
func (r *ClickHouseRepository) GetAISummary(teamUUID string, startMs, endMs int64) (*AISummary, error) {
	// Build query using dedicated query builder
	builder := NewSummaryQueryBuilder(teamUUID, startMs, endMs)
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
func (r *ClickHouseRepository) GetAIModels(teamUUID string, startMs, endMs int64) ([]AIModel, error) {
	// Build query using dedicated query builder
	builder := NewModelListQueryBuilder(teamUUID, startMs, endMs)
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
func (r *ClickHouseRepository) GetAIPerformanceMetrics(teamUUID string, startMs, endMs int64) ([]AIPerformanceMetric, error) {
	// Build query using dedicated query builder
	builder := NewPerformanceMetricsQueryBuilder(teamUUID, startMs, endMs)
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
func (r *ClickHouseRepository) GetAIPerformanceTimeSeries(teamUUID string, startMs, endMs int64) ([]AIPerformanceTimeSeries, error) {
	// Create adaptive time bucket strategy
	strategy := r.bucketFactory.CreateStrategy(startMs, endMs)

	// Build query using time series query builder
	builder := NewTimeSeriesQueryBuilder(teamUUID, startMs, endMs, strategy)
	builder.WithSelectFields([]string{
		"COUNT(*) as request_count",
		"AVG(duration_ms) as avg_latency_ms",
		"AVG(duration_ms) as p95_latency_ms",
		"SUM(CASE WHEN timeout=1 THEN 1 ELSE 0 END) as timeout_count",
		"SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END) as error_count",
		"AVG(CASE WHEN duration_ms>0 THEN COALESCE(tokens_completion,0)/(duration_ms/1000.0) ELSE 0 END) as tokens_per_sec",
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
func (r *ClickHouseRepository) GetAILatencyHistogram(teamUUID string, modelName string, startMs, endMs int64) ([]AILatencyHistogram, error) {
	// Build query using dedicated query builder
	builder := NewLatencyHistogramQueryBuilder(teamUUID, startMs, endMs)
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
func (r *ClickHouseRepository) GetAICostMetrics(teamUUID string, startMs, endMs int64) ([]AICostMetric, error) {
	// Build query using dedicated query builder
	builder := NewCostMetricsQueryBuilder(teamUUID, startMs, endMs)
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
func (r *ClickHouseRepository) GetAICostTimeSeries(teamUUID string, startMs, endMs int64) ([]AICostTimeSeries, error) {
	// Create adaptive time bucket strategy
	strategy := r.bucketFactory.CreateStrategy(startMs, endMs)

	// Build query using time series query builder
	builder := NewTimeSeriesQueryBuilder(teamUUID, startMs, endMs, strategy)
	builder.WithSelectFields([]string{
		"SUM(COALESCE(cost_usd,0)) as cost_per_interval",
		"SUM(COALESCE(tokens_prompt,0)) as prompt_tokens",
		"SUM(COALESCE(tokens_completion,0)) as completion_tokens",
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
func (r *ClickHouseRepository) GetAITokenBreakdown(teamUUID string, startMs, endMs int64) ([]AITokenBreakdown, error) {
	// Build query using dedicated query builder
	builder := NewTokenBreakdownQueryBuilder(teamUUID, startMs, endMs)
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
func (r *ClickHouseRepository) GetAISecurityMetrics(teamUUID string, startMs, endMs int64) ([]AISecurityMetric, error) {
	// Build query using dedicated query builder
	builder := NewSecurityMetricsQueryBuilder(teamUUID, startMs, endMs)
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
func (r *ClickHouseRepository) GetAISecurityTimeSeries(teamUUID string, startMs, endMs int64) ([]AISecurityTimeSeries, error) {
	// Create adaptive time bucket strategy
	strategy := r.bucketFactory.CreateStrategy(startMs, endMs)

	// Build query using time series query builder
	builder := NewTimeSeriesQueryBuilder(teamUUID, startMs, endMs, strategy)
	builder.WithSelectFields([]string{
		"COUNT(*) as total_requests",
		"SUM(CASE WHEN pii_detected=1 THEN 1 ELSE 0 END) as pii_count",
		"SUM(CASE WHEN guardrail_blocked=1 THEN 1 ELSE 0 END) as guardrail_count",
		"SUM(CASE WHEN content_policy=1 THEN 1 ELSE 0 END) as content_policy_count",
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
func (r *ClickHouseRepository) GetAIPiiCategories(teamUUID string, startMs, endMs int64) ([]AIPiiCategory, error) {
	// Build query using dedicated query builder
	builder := NewPIICategoryQueryBuilder(teamUUID, startMs, endMs)
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
