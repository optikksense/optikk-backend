package rundetail

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/observability/observability-backend-go/internal/database"
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
	GetRunDetail(ctx context.Context, teamID int64, spanID string) (*runDetailDTO, error)
	GetRunEvents(ctx context.Context, teamID int64, spanID string) ([]runEventDTO, error)
	GetTraceSpans(ctx context.Context, teamID int64, traceID string) ([]traceContextSpanDTO, error)
}

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func baseParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func (r *ClickHouseRepository) GetRunDetail(ctx context.Context, teamID int64, spanID string) (*runDetailDTO, error) {
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
		WHERE s.team_id = @teamID AND s.span_id = @spanID
		LIMIT 1
	`, colModel, colProvider, colOperationType,
		colInputTokens, colOutputTokens,
		colInputTokens, colOutputTokens,
		colFinishReason, tableSpans)

	var row runDetailDTO
	if err := r.db.QueryRow(ctx, &row, query,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("spanID", spanID),
	); err != nil {
		return nil, nil
	}
	return &row, nil
}

func (r *ClickHouseRepository) GetRunEvents(ctx context.Context, teamID int64, spanID string) ([]runEventDTO, error) {
	var rows []runEventDTO
	err := r.db.Select(ctx, &rows, `
		SELECT s.span_id, event_json
		FROM observability.spans s
		ARRAY JOIN s.events AS event_json
		WHERE s.team_id = @teamID AND s.span_id = @spanID
		LIMIT 200
	`,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("spanID", spanID),
	)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetTraceSpans(ctx context.Context, teamID int64, traceID string) ([]traceContextSpanDTO, error) {
	query := fmt.Sprintf(`
		SELECT s.span_id, s.parent_span_id, s.service_name,
		       s.name AS operation_name, s.timestamp,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       s.has_error, s.kind_string,
		       %s AS model
		FROM %s s
		WHERE s.team_id = @teamID AND s.trace_id = @traceID
		ORDER BY s.timestamp ASC
		LIMIT 500
	`, colModel, tableSpans)
	var rows []traceContextSpanDTO
	if err := r.db.Select(ctx, &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
	); err != nil {
		return nil, err
	}
	return rows, nil
}
