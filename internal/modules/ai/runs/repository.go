package runs

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

const tableSpans = "observability.spans"

type Repository interface {
	ListRuns(ctx context.Context, f LLMRunFilters) ([]llmRunRowDTO, error)
	GetRunsSummary(ctx context.Context, f LLMRunFilters) (*llmRunSummaryRowDTO, error)
	ListModels(ctx context.Context, f LLMRunFilters) ([]llmRunModelRowDTO, error)
	ListOperations(ctx context.Context, f LLMRunFilters) ([]llmRunOperationRowDTO, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) ListRuns(ctx context.Context, f LLMRunFilters) ([]llmRunRowDTO, error) {
	where, args := buildWhereClause(f)
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}

	query := fmt.Sprintf(`
		SELECT s.span_id, s.trace_id, s.parent_span_id, s.service_name,
		       s.name AS operation_name,
		       %s AS model, %s AS provider, %s AS operation_type,
		       s.timestamp, s.duration_nano / 1000000.0 AS duration_ms,
		       %s AS input_tokens, %s AS output_tokens,
		       (%s + %s) AS total_tokens,
		       s.has_error, s.status_message,
		       %s AS finish_reason, s.kind_string
		FROM %s s
		%s
		ORDER BY s.timestamp DESC, s.span_id DESC
		LIMIT %d
	`, colModel, colProvider, colOperationType,
		colInputTokens, colOutputTokens,
		colInputTokens, colOutputTokens,
		colFinishReason,
		tableSpans, where, limit)

	var rows []llmRunRowDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetRunsSummary(ctx context.Context, f LLMRunFilters) (*llmRunSummaryRowDTO, error) {
	where, args := buildWhereClause(f)

	query := fmt.Sprintf(`
		SELECT toInt64(COUNT(*)) AS total_runs,
		       toInt64(countIf(s.has_error)) AS error_runs,
		       IF(COUNT(*) > 0, countIf(s.has_error) * 100.0 / COUNT(*), 0) AS error_rate,
		       if(isNaN(AVG(s.duration_nano / 1000000.0)), 0, COALESCE(AVG(s.duration_nano / 1000000.0), 0)) AS avg_latency_ms,
		       if(isNaN(quantile(0.95)(s.duration_nano / 1000000.0)), 0, COALESCE(quantile(0.95)(s.duration_nano / 1000000.0), 0)) AS p95_latency_ms,
		       toInt64(COALESCE(SUM(%s + %s), 0)) AS total_tokens,
		       toInt64(COUNT(DISTINCT %s)) AS unique_models
		FROM %s s
		%s
	`, colInputTokens, colOutputTokens, colModel, tableSpans, where)

	var row llmRunSummaryRowDTO
	if err := r.db.QueryRow(ctx, &row, query, args...); err != nil {
		return nil, err
	}
	return &row, nil
}

func (r *ClickHouseRepository) ListModels(ctx context.Context, f LLMRunFilters) ([]llmRunModelRowDTO, error) {
	where, args := buildWhereClause(f)

	query := fmt.Sprintf(`
		SELECT %s AS model, %s AS provider
		FROM %s s
		%s
		GROUP BY model, provider
		ORDER BY model ASC
		LIMIT 100
	`, colModel, colProvider, tableSpans, where)

	var rows []llmRunModelRowDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) ListOperations(ctx context.Context, f LLMRunFilters) ([]llmRunOperationRowDTO, error) {
	where, args := buildWhereClause(f)

	query := fmt.Sprintf(`
		SELECT %s AS operation_type
		FROM %s s
		%s
		GROUP BY operation_type
		ORDER BY operation_type ASC
		LIMIT 100
	`, colOperationType, tableSpans, where)

	var rows []llmRunOperationRowDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}
