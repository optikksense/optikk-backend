package rundetail

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

const (
	tableSpans       = "observability.spans"
	colModel         = "attributes.'gen_ai.request.model'::String"
	colProvider      = "attributes.'server.address'::String"
	colOperationType = "attributes.'gen_ai.operation.name'::String"
	colInputTokens   = "attributes.'gen_ai.usage.input_tokens'::Int64"
	colOutputTokens  = "attributes.'gen_ai.usage.output_tokens'::Int64"
	colFinishReason  = "attributes.'gen_ai.response.finish_reasons'::String"
)

type Repository interface {
	GetRunDetail(teamID int64, spanID string) (*LLMRunDetail, error)
	GetRunEvents(teamID int64, spanID string) ([]map[string]any, error)
	GetTraceSpans(teamID int64, traceID string) ([]map[string]any, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetRunDetail(teamID int64, spanID string) (*LLMRunDetail, error) {
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
		WHERE s.team_id = ? AND s.span_id = ?
		LIMIT 1
	`, colModel, colProvider, colOperationType,
		colInputTokens, colOutputTokens,
		colInputTokens, colOutputTokens,
		colFinishReason, tableSpans)

	row, err := dbutil.QueryMap(r.db, query, uint32(teamID), spanID)
	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		return nil, nil
	}

	return &LLMRunDetail{
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
	}, nil
}

func (r *ClickHouseRepository) GetRunEvents(teamID int64, spanID string) ([]map[string]any, error) {
	return dbutil.QueryMaps(r.db, `
		SELECT s.span_id, event_json
		FROM observability.spans s
		ARRAY JOIN s.events AS event_json
		WHERE s.team_id = ? AND s.span_id = ?
		LIMIT 200
	`, uint32(teamID), spanID)
}

func (r *ClickHouseRepository) GetTraceSpans(teamID int64, traceID string) ([]map[string]any, error) {
	query := fmt.Sprintf(`
		SELECT s.span_id, s.parent_span_id, s.service_name,
		       s.name AS operation_name, s.timestamp,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       s.has_error, s.kind_string,
		       %s AS model
		FROM %s s
		WHERE s.team_id = ? AND s.trace_id = ?
		ORDER BY s.timestamp ASC
		LIMIT 500
	`, colModel, tableSpans)
	return dbutil.QueryMaps(r.db, query, uint32(teamID), traceID)
}
