package overview

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	aishared "github.com/Optikk-Org/optikk-backend/internal/modules/ai/shared"
)

// Repository defines the data access methods for the AI overview module.
type Repository interface {
	GetOverview(ctx context.Context, teamID, startMs, endMs int64) (aishared.AIOverview, error)
	GetOverviewTimeseries(ctx context.Context, teamID, startMs, endMs int64, step string) ([]aishared.AITrendPoint, error)
	GetTopModels(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]AIModelBreakdown, error)
	GetTopPrompts(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]AIPromptBreakdown, error)
	GetQualitySummary(ctx context.Context, teamID, startMs, endMs int64) (AIQualitySummary, error)
}

type repository struct {
	db *dbutil.NativeQuerier
}

// NewRepository creates a new overview repository.
func NewRepository(db *dbutil.NativeQuerier) Repository {
	return &repository{db: db}
}

func (r *repository) GetOverview(ctx context.Context, teamID, startMs, endMs int64) (aishared.AIOverview, error) {
	return aishared.SummarizeRuns(ctx, r.db, aishared.QueryContext{TeamID: teamID, Start: startMs, End: endMs})
}

func (r *repository) GetOverviewTimeseries(ctx context.Context, teamID, startMs, endMs int64, step string) ([]aishared.AITrendPoint, error) {
	return aishared.TrendRuns(ctx, r.db, aishared.QueryContext{TeamID: teamID, Start: startMs, End: endMs}, step)
}

func (r *repository) GetTopModels(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]AIModelBreakdown, error) {
	if limit <= 0 {
		limit = aishared.DefaultBreakdownLimit
	}

	where, args := aishared.BuildWhereClause(aishared.QueryContext{TeamID: teamID, Start: startMs, End: endMs})
	query := fmt.Sprintf(`
		SELECT
			ai.provider,
			ai.request_model,
			count() AS requests,
			countIf(ai.has_error) AS error_runs,
			avg(ai.latency_ms) AS avg_latency_ms,
			avg(ai.ttft_ms) AS avg_ttft_ms,
			sum(ai.total_tokens) AS total_tokens,
			sum(ai.cost_usd) AS total_cost_usd,
			if(countIf(ai.quality_score > 0) = 0, 0, avgIf(ai.quality_score, ai.quality_score > 0)) AS avg_quality_score
		FROM %s ai
		WHERE %s AND ai.request_model != ''
		GROUP BY ai.provider, ai.request_model
		ORDER BY requests DESC
		LIMIT %d
	`, aishared.AIRunsSubquery(), where, limit)

	var rows []AIModelBreakdown
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	for idx := range rows {
		rows[idx].ErrorRatePct = aishared.RatioPct(rows[idx].ErrorRuns, rows[idx].Requests)
	}
	return rows, nil
}

func (r *repository) GetTopPrompts(ctx context.Context, teamID, startMs, endMs int64, limit int) ([]AIPromptBreakdown, error) {
	if limit <= 0 {
		limit = aishared.DefaultBreakdownLimit
	}

	where, args := aishared.BuildWhereClause(aishared.QueryContext{TeamID: teamID, Start: startMs, End: endMs})
	query := fmt.Sprintf(`
		SELECT
			ai.prompt_template,
			ai.prompt_template_version,
			count() AS requests,
			avg(ai.latency_ms) AS avg_latency_ms,
			if(countIf(ai.quality_score > 0) = 0, 0, avgIf(ai.quality_score, ai.quality_score > 0)) AS avg_quality_score,
			sum(ai.cost_usd) AS total_cost_usd,
			countIf(ai.has_error) AS error_runs
		FROM %s ai
		WHERE %s AND ai.prompt_template != ''
		GROUP BY ai.prompt_template, ai.prompt_template_version
		ORDER BY requests DESC
		LIMIT %d
	`, aishared.AIRunsSubquery(), where, limit)

	type promptRow struct {
		AIPromptBreakdown
		ErrorRuns uint64 `ch:"error_runs"`
	}

	var rows []promptRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	out := make([]AIPromptBreakdown, 0, len(rows))
	for _, row := range rows {
		item := row.AIPromptBreakdown
		item.ErrorRatePct = aishared.RatioPct(row.ErrorRuns, item.Requests)
		out = append(out, item)
	}
	return out, nil
}

func (r *repository) GetQualitySummary(ctx context.Context, teamID, startMs, endMs int64) (AIQualitySummary, error) {
	where, args := aishared.BuildWhereClause(aishared.QueryContext{TeamID: teamID, Start: startMs, End: endMs})
	source := aishared.AIRunsSubquery()

	summaryQuery := fmt.Sprintf(`
		SELECT
			if(countIf(ai.quality_score > 0) = 0, 0, avgIf(ai.quality_score, ai.quality_score > 0)) AS avg_quality_score,
			if(countIf(ai.feedback_score > 0) = 0, 0, avgIf(ai.feedback_score, ai.feedback_score > 0)) AS avg_feedback_score,
			countIf(ai.quality_score > 0) AS scored_runs,
			countIf(ai.feedback_score > 0) AS feedback_runs,
			countIf(lower(ai.guardrail_state) IN ('blocked', 'rejected', 'failed')) AS guardrail_blocks
		FROM %s ai
		WHERE %s
	`, source, where)

	var summary qualitySummaryRow
	if err := r.db.QueryRow(ctx, &summary, summaryQuery, args...); err != nil {
		return AIQualitySummary{}, err
	}

	guardrailQuery := fmt.Sprintf(`
		SELECT
			if(ai.guardrail_state = '', 'not_evaluated', ai.guardrail_state) AS guardrail_state,
			count() AS count
		FROM %s ai
		WHERE %s
		GROUP BY guardrail_state
		ORDER BY count DESC
	`, source, where)
	var guardrailRows []AIGuardrailBreakdown
	if err := r.db.Select(ctx, &guardrailRows, guardrailQuery, args...); err != nil {
		return AIQualitySummary{}, err
	}

	qualityQuery := fmt.Sprintf(`
		SELECT
			ai.quality_bucket AS bucket,
			count() AS count
		FROM %s ai
		WHERE %s
		GROUP BY ai.quality_bucket
		ORDER BY count DESC
	`, source, where)
	var qualityRows []AIQualityBucket
	if err := r.db.Select(ctx, &qualityRows, qualityQuery, args...); err != nil {
		return AIQualitySummary{}, err
	}

	return AIQualitySummary{
		AvgQualityScore:    summary.AvgQualityScore,
		AvgFeedbackScore:   summary.AvgFeedbackScore,
		ScoredRuns:         summary.ScoredRuns,
		FeedbackRuns:       summary.FeedbackRuns,
		GuardrailBlocks:    summary.GuardrailBlocks,
		GuardrailBreakdown: guardrailRows,
		QualityBuckets:     qualityRows,
	}, nil
}
