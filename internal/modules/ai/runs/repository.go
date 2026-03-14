package runs

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

const tableSpans = "observability.spans"

type Repository interface {
	ListRuns(f LLMRunFilters) ([]LLMRun, error)
	GetRunsSummary(f LLMRunFilters) (*LLMRunSummary, error)
	ListModels(f LLMRunFilters) ([]LLMRunModel, error)
	ListOperations(f LLMRunFilters) ([]LLMRunOperation, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) ListRuns(f LLMRunFilters) ([]LLMRun, error) {
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

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	runs := make([]LLMRun, 0, len(rows))
	for _, row := range rows {
		runs = append(runs, LLMRun{
			SpanID:        dbutil.StringFromAny(row["span_id"]),
			TraceID:       dbutil.StringFromAny(row["trace_id"]),
			ParentSpanID:  dbutil.StringFromAny(row["parent_span_id"]),
			ServiceName:   dbutil.StringFromAny(row["service_name"]),
			OperationName: dbutil.StringFromAny(row["operation_name"]),
			Model:         dbutil.StringFromAny(row["model"]),
			Provider:      dbutil.StringFromAny(row["provider"]),
			OperationType: dbutil.StringFromAny(row["operation_type"]),
			StartTime:     dbutil.TimeFromAny(row["timestamp"]),
			DurationMs:    dbutil.Float64FromAny(row["duration_ms"]),
			InputTokens:   dbutil.Int64FromAny(row["input_tokens"]),
			OutputTokens:  dbutil.Int64FromAny(row["output_tokens"]),
			TotalTokens:   dbutil.Int64FromAny(row["total_tokens"]),
			HasError:      dbutil.BoolFromAny(row["has_error"]),
			StatusMessage: dbutil.StringFromAny(row["status_message"]),
			FinishReason:  dbutil.StringFromAny(row["finish_reason"]),
			SpanKind:      dbutil.StringFromAny(row["kind_string"]),
		})
	}
	return runs, nil
}

func (r *ClickHouseRepository) GetRunsSummary(f LLMRunFilters) (*LLMRunSummary, error) {
	where, args := buildWhereClause(f)

	query := fmt.Sprintf(`
		SELECT COUNT(*) AS total_runs,
		       countIf(s.has_error) AS error_runs,
		       IF(COUNT(*) > 0, countIf(s.has_error) * 100.0 / COUNT(*), 0) AS error_rate,
		       AVG(s.duration_nano / 1000000.0) AS avg_latency_ms,
		       quantile(0.95)(s.duration_nano / 1000000.0) AS p95_latency_ms,
		       SUM(%s + %s) AS total_tokens,
		       COUNT(DISTINCT %s) AS unique_models
		FROM %s s
		%s
	`, colInputTokens, colOutputTokens, colModel, tableSpans, where)

	row, err := dbutil.QueryMap(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		return &LLMRunSummary{}, nil
	}
	return &LLMRunSummary{
		TotalRuns:    dbutil.Int64FromAny(row["total_runs"]),
		ErrorRuns:    dbutil.Int64FromAny(row["error_runs"]),
		ErrorRate:    dbutil.Float64FromAny(row["error_rate"]),
		AvgLatencyMs: dbutil.Float64FromAny(row["avg_latency_ms"]),
		P95LatencyMs: dbutil.Float64FromAny(row["p95_latency_ms"]),
		TotalTokens:  dbutil.Int64FromAny(row["total_tokens"]),
		UniqueModels: dbutil.Int64FromAny(row["unique_models"]),
	}, nil
}

func (r *ClickHouseRepository) ListModels(f LLMRunFilters) ([]LLMRunModel, error) {
	where, args := buildWhereClause(f)

	query := fmt.Sprintf(`
		SELECT %s AS model, %s AS provider
		FROM %s s
		%s
		GROUP BY model, provider
		ORDER BY model ASC
		LIMIT 100
	`, colModel, colProvider, tableSpans, where)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	models := make([]LLMRunModel, 0, len(rows))
	for _, row := range rows {
		models = append(models, LLMRunModel{
			Model:    dbutil.StringFromAny(row["model"]),
			Provider: dbutil.StringFromAny(row["provider"]),
		})
	}
	return models, nil
}

func (r *ClickHouseRepository) ListOperations(f LLMRunFilters) ([]LLMRunOperation, error) {
	where, args := buildWhereClause(f)

	query := fmt.Sprintf(`
		SELECT %s AS operation_type
		FROM %s s
		%s
		GROUP BY operation_type
		ORDER BY operation_type ASC
		LIMIT 100
	`, colOperationType, tableSpans, where)

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}
	ops := make([]LLMRunOperation, 0, len(rows))
	for _, row := range rows {
		ops = append(ops, LLMRunOperation{
			OperationType: dbutil.StringFromAny(row["operation_type"]),
		})
	}
	return ops, nil
}
