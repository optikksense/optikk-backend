package dashboard

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

type TimeBucketStrategyFactory struct{}

func NewTimeBucketStrategyFactory() *TimeBucketStrategyFactory {
	return &TimeBucketStrategyFactory{}
}

func (f *TimeBucketStrategyFactory) CreateStrategy(startMs, endMs int64) utils.Strategy {
	return utils.NewAdaptiveStrategy(startMs, endMs)
}

type Repository interface {
	GetAISummary(ctx context.Context, teamID int64, model string, startMs, endMs int64) (*aiSummaryDTO, error)
	GetAIModels(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiModelDTO, error)
	GetAIPerformanceMetrics(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiPerformanceMetricDTO, error)
	GetAIPerformanceTimeSeries(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiPerformanceTimeSeriesDTO, error)
	GetAILatencyHistogram(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiLatencyHistogramDTO, error)
	GetAICostMetrics(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiCostMetricDTO, error)
	GetAICostTimeSeries(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiCostTimeSeriesDTO, error)
	GetAITokenBreakdown(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiTokenBreakdownDTO, error)
	GetAISecurityMetrics(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiSecurityMetricDTO, error)
	GetAISecurityTimeSeries(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiSecurityTimeSeriesDTO, error)
	GetAIPiiCategories(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiPIICategoryDTO, error)
}

type ClickHouseRepository struct {
	db            *dbutil.NativeQuerier
	bucketFactory *TimeBucketStrategyFactory
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{
		db:            db,
		bucketFactory: NewTimeBucketStrategyFactory(),
	}
}

func (r *ClickHouseRepository) GetAISummary(ctx context.Context, teamID int64, model string, startMs, endMs int64) (*aiSummaryDTO, error) {
	builder := NewSummaryQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		builder.WithModelName(model)
	}
	query, args := builder.Build()
	var row aiSummaryDTO
	if err := r.db.QueryRow(ctx, &row, query, args...); err != nil {
		return nil, err
	}
	return &row, nil
}

func (r *ClickHouseRepository) GetAIModels(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiModelDTO, error) {
	b := NewModelListQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		b.WithModelName(model)
	}
	query, args := b.Build()
	var rows []aiModelDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetAIPerformanceMetrics(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiPerformanceMetricDTO, error) {
	b := NewPerformanceMetricsQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		b.WithModelName(model)
	}
	query, args := b.Build()
	var rows []aiPerformanceMetricDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetAIPerformanceTimeSeries(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiPerformanceTimeSeriesDTO, error) {
	strategy := r.bucketFactory.CreateStrategy(startMs, endMs)
	durationMs := attrFlt("duration_ms")
	outputTokens := attrInt("gen.ai.usage.output_tokens")
	timeout := attrInt("ai.timeout")

	builder := NewTimeSeriesQueryBuilder(teamID, startMs, endMs, strategy)
	if model != "" {
		builder.WithModelName(model)
	}
	builder.WithSelectFields([]string{
		"toInt64(COUNT(*)) as request_count",
		fmt.Sprintf("AVG(%s) as avg_latency_ms", durationMs),
		fmt.Sprintf("quantile(0.95)(%s) as p95_latency_ms", durationMs),
		fmt.Sprintf("toInt64(SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END)) as timeout_count", timeout),
		"toInt64(countIf(has_error)) as error_count",
		fmt.Sprintf("AVG(CASE WHEN %s > 0 THEN COALESCE(%s, 0) / (%s / 1000.0) ELSE 0 END) as tokens_per_sec", durationMs, outputTokens, durationMs),
	})

	query, args := builder.Build()
	var rows []aiPerformanceTimeSeriesDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetAILatencyHistogram(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiLatencyHistogramDTO, error) {
	builder := NewLatencyHistogramQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		builder.WithModelName(model)
	}
	query, args := builder.Build()
	var rows []aiLatencyHistogramDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetAICostMetrics(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiCostMetricDTO, error) {
	b := NewCostMetricsQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		b.WithModelName(model)
	}
	query, args := b.Build()
	var rows []aiCostMetricDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetAICostTimeSeries(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiCostTimeSeriesDTO, error) {
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
		fmt.Sprintf("toInt64(SUM(COALESCE(%s, 0))) as prompt_tokens", inputTokens),
		fmt.Sprintf("toInt64(SUM(COALESCE(%s, 0))) as completion_tokens", outputTokens),
		"toInt64(COUNT(*)) as request_count",
	})

	query, args := builder.Build()
	var rows []aiCostTimeSeriesDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetAITokenBreakdown(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiTokenBreakdownDTO, error) {
	b := NewTokenBreakdownQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		b.WithModelName(model)
	}
	query, args := b.Build()
	var rows []aiTokenBreakdownDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetAISecurityMetrics(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiSecurityMetricDTO, error) {
	b := NewSecurityMetricsQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		b.WithModelName(model)
	}
	query, args := b.Build()
	var rows []aiSecurityMetricDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetAISecurityTimeSeries(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiSecurityTimeSeriesDTO, error) {
	strategy := r.bucketFactory.CreateStrategy(startMs, endMs)
	piiDetected := attrInt("ai.security.pii_detected")
	guardrailBlocked := attrInt("ai.security.guardrail_blocked")
	contentPolicy := attrInt("ai.security.content_policy")

	builder := NewTimeSeriesQueryBuilder(teamID, startMs, endMs, strategy)
	if model != "" {
		builder.WithModelName(model)
	}
	builder.WithSelectFields([]string{
		"toInt64(COUNT(*)) as total_requests",
		fmt.Sprintf("toInt64(SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END)) as pii_count", piiDetected),
		fmt.Sprintf("toInt64(SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END)) as guardrail_count", guardrailBlocked),
		fmt.Sprintf("toInt64(SUM(CASE WHEN %s = 1 THEN 1 ELSE 0 END)) as content_policy_count", contentPolicy),
	})

	query, args := builder.Build()
	var rows []aiSecurityTimeSeriesDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetAIPiiCategories(ctx context.Context, teamID int64, model string, startMs, endMs int64) ([]aiPIICategoryDTO, error) {
	b := NewPIICategoryQueryBuilder(teamID, startMs, endMs)
	if model != "" {
		b.WithModelName(model)
	}
	query, args := b.Build()
	var rows []aiPIICategoryDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}
