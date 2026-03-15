package dashboard

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type TimeBucketStrategyFactory struct{}

func NewTimeBucketStrategyFactory() *TimeBucketStrategyFactory {
	return &TimeBucketStrategyFactory{}
}

func (f *TimeBucketStrategyFactory) CreateStrategy(startMs, endMs int64) timebucket.Strategy {
	return timebucket.NewAdaptiveStrategy(startMs, endMs)
}

type Repository interface {
	GetAISummary(teamID int64, model string, startMs, endMs int64) (*AISummary, error)
	GetAIModels(teamID int64, model string, startMs, endMs int64) ([]AIModel, error)
	GetAIPerformanceMetrics(teamID int64, model string, startMs, endMs int64) ([]AIPerformanceMetric, error)
	GetAIPerformanceTimeSeries(teamID int64, model string, startMs, endMs int64) ([]AIPerformanceTimeSeries, error)
	GetAILatencyHistogram(teamID int64, model string, startMs, endMs int64) ([]AILatencyHistogram, error)
	GetAICostMetrics(teamID int64, model string, startMs, endMs int64) ([]AICostMetric, error)
	GetAICostTimeSeries(teamID int64, model string, startMs, endMs int64) ([]AICostTimeSeries, error)
	GetAITokenBreakdown(teamID int64, model string, startMs, endMs int64) ([]AITokenBreakdown, error)
	GetAISecurityMetrics(teamID int64, model string, startMs, endMs int64) ([]AISecurityMetric, error)
	GetAISecurityTimeSeries(teamID int64, model string, startMs, endMs int64) ([]AISecurityTimeSeries, error)
	GetAIPiiCategories(teamID int64, model string, startMs, endMs int64) ([]AIPiiCategory, error)
}

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

func (r *ClickHouseRepository) queryAndAggregate(builder QueryBuilder, agg MetricAggregator) (any, error) {
	query, args := builder.Build()
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	return agg.Aggregate(rows)
}

func (r *ClickHouseRepository) GetAISummary(teamID int64, model string, startMs, endMs int64) (*AISummary, error) {
	builder := NewSummaryQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		builder.WithModelName(model)
	}
	query, args := builder.Build()
	row, err := dbutil.QueryMap(r.db, query, args...)
	if err != nil || len(row) == 0 {
		return nil, err
	}
	result, err := r.summaryAggregator.Aggregate([]map[string]any{row})
	if err != nil {
		return nil, err
	}
	return result.(*AISummary), nil
}

func (r *ClickHouseRepository) GetAIModels(teamID int64, model string, startMs, endMs int64) ([]AIModel, error) {
	b := NewModelListQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		b.WithModelName(model)
	}
	result, err := r.queryAndAggregate(b, r.modelListAggregator)
	if err != nil {
		return nil, err
	}
	return result.([]AIModel), nil
}

func (r *ClickHouseRepository) GetAIPerformanceMetrics(teamID int64, model string, startMs, endMs int64) ([]AIPerformanceMetric, error) {
	b := NewPerformanceMetricsQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		b.WithModelName(model)
	}
	result, err := r.queryAndAggregate(b, r.perfAggregator)
	if err != nil {
		return nil, err
	}
	return result.([]AIPerformanceMetric), nil
}

func (r *ClickHouseRepository) GetAIPerformanceTimeSeries(teamID int64, model string, startMs, endMs int64) ([]AIPerformanceTimeSeries, error) {
	strategy := r.bucketFactory.CreateStrategy(startMs, endMs)
	durationMs := attrFlt("duration_ms")
	outputTokens := attrInt("gen.ai.usage.output_tokens")
	timeout := attrInt("ai.timeout")

	builder := NewTimeSeriesQueryBuilder(teamID, startMs, endMs, strategy)
	if model != "" {
		builder.WithModelName(model)
	}
	builder.WithSelectFields([]string{
		"COUNT(*) as request_count",
		fmt.Sprintf("AVG(%s) as avg_latency_ms", durationMs),
		fmt.Sprintf("quantile(0.95)(%s) as p95_latency_ms", durationMs),
		fmt.Sprintf("SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) as timeout_count", timeout),
		"countIf(has_error) as error_count",
		fmt.Sprintf("AVG(CASE WHEN %s > 0 THEN COALESCE(%s, 0) / (%s / 1000.0) ELSE 0 END) as tokens_per_sec", durationMs, outputTokens, durationMs),
	})

	agg := NewTimeSeriesAggregator("performance")
	result, err := r.queryAndAggregate(builder, agg)
	if err != nil {
		return nil, err
	}
	return result.([]AIPerformanceTimeSeries), nil
}

func (r *ClickHouseRepository) GetAILatencyHistogram(teamID int64, model string, startMs, endMs int64) ([]AILatencyHistogram, error) {
	builder := NewLatencyHistogramQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		builder.WithModelName(model)
	}
	result, err := r.queryAndAggregate(builder, r.histogramAggregator)
	if err != nil {
		return nil, err
	}
	return result.([]AILatencyHistogram), nil
}

func (r *ClickHouseRepository) GetAICostMetrics(teamID int64, model string, startMs, endMs int64) ([]AICostMetric, error) {
	b := NewCostMetricsQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		b.WithModelName(model)
	}
	result, err := r.queryAndAggregate(b, r.costAggregator)
	if err != nil {
		return nil, err
	}
	return result.([]AICostMetric), nil
}

func (r *ClickHouseRepository) GetAICostTimeSeries(teamID int64, model string, startMs, endMs int64) ([]AICostTimeSeries, error) {
	strategy := r.bucketFactory.CreateStrategy(startMs, endMs)
	costUSD := attrFlt("ai.cost_usd")
	inputTokens := attrInt("gen.ai.usage.input_tokens")
	outputTokens := attrInt("gen.ai.usage.output_tokens")

	builder := NewTimeSeriesQueryBuilder(teamID, startMs, endMs, strategy)
	if model != "" {
		builder.WithModelName(model)
	}
	builder.WithSelectFields([]string{
		fmt.Sprintf("SUM(COALESCE(%s, 0)) as cost_per_interval", costUSD),
		fmt.Sprintf("SUM(COALESCE(%s, 0)) as prompt_tokens", inputTokens),
		fmt.Sprintf("SUM(COALESCE(%s, 0)) as completion_tokens", outputTokens),
		"COUNT(*) as request_count",
	})

	agg := NewTimeSeriesAggregator("cost")
	result, err := r.queryAndAggregate(builder, agg)
	if err != nil {
		return nil, err
	}
	return result.([]AICostTimeSeries), nil
}

func (r *ClickHouseRepository) GetAITokenBreakdown(teamID int64, model string, startMs, endMs int64) ([]AITokenBreakdown, error) {
	b := NewTokenBreakdownQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		b.WithModelName(model)
	}
	result, err := r.queryAndAggregate(b, r.tokenAggregator)
	if err != nil {
		return nil, err
	}
	return result.([]AITokenBreakdown), nil
}

func (r *ClickHouseRepository) GetAISecurityMetrics(teamID int64, model string, startMs, endMs int64) ([]AISecurityMetric, error) {
	b := NewSecurityMetricsQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		b.WithModelName(model)
	}
	result, err := r.queryAndAggregate(b, r.securityAggregator)
	if err != nil {
		return nil, err
	}
	return result.([]AISecurityMetric), nil
}

func (r *ClickHouseRepository) GetAISecurityTimeSeries(teamID int64, model string, startMs, endMs int64) ([]AISecurityTimeSeries, error) {
	strategy := r.bucketFactory.CreateStrategy(startMs, endMs)
	piiDetected := attrInt("ai.security.pii_detected")
	guardrailBlocked := attrInt("ai.security.guardrail_blocked")
	contentPolicy := attrInt("ai.security.content_policy")

	builder := NewTimeSeriesQueryBuilder(teamID, startMs, endMs, strategy)
	if model != "" {
		builder.WithModelName(model)
	}
	builder.WithSelectFields([]string{
		"COUNT(*) as total_requests",
		fmt.Sprintf("SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) as pii_count", piiDetected),
		fmt.Sprintf("SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) as guardrail_count", guardrailBlocked),
		fmt.Sprintf("SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END) as content_policy_count", contentPolicy),
	})

	agg := NewTimeSeriesAggregator("security")
	result, err := r.queryAndAggregate(builder, agg)
	if err != nil {
		return nil, err
	}
	return result.([]AISecurityTimeSeries), nil
}

func (r *ClickHouseRepository) GetAIPiiCategories(teamID int64, model string, startMs, endMs int64) ([]AIPiiCategory, error) {
	b := NewPIICategoryQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		b.WithModelName(model)
	}
	result, err := r.queryAndAggregate(b, r.piiAggregator)
	if err != nil {
		return nil, err
	}
	return result.([]AIPiiCategory), nil
}
