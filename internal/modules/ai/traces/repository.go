package traces

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/observability/observability-backend-go/internal/database"
)

const (
	tableSpans      = "observability.spans"
	colModel        = "attributes.'gen_ai.request.model'::String"
	colInputTokens  = "attributes.'gen_ai.usage.input_tokens'::Int64"
	colOutputTokens = "attributes.'gen_ai.usage.output_tokens'::Int64"
)

type Repository interface {
	GetTraceSpans(ctx context.Context, teamID int64, traceID string) ([]traceSpanDTO, error)
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

func (r *ClickHouseRepository) GetTraceSpans(ctx context.Context, teamID int64, traceID string) ([]traceSpanDTO, error) {
	query := fmt.Sprintf(`
		SELECT s.span_id, s.parent_span_id, s.service_name,
		       s.name AS operation_name, s.timestamp,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       s.has_error, s.kind_string,
		       %s AS model,
		       %s AS input_tokens,
		       %s AS output_tokens
		FROM %s s
		WHERE s.team_id = @teamID AND s.trace_id = @traceID
		ORDER BY s.timestamp ASC
		LIMIT 500
	`, colModel, colInputTokens, colOutputTokens, tableSpans)
	var rows []traceSpanDTO
	if err := r.db.Select(ctx, &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
	); err != nil {
		return nil, err
	}
	return rows, nil
}
