package traces

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

const (
	tableSpans     = "observability.spans"
	colModel       = "attributes.'gen_ai.request.model'::String"
	colInputTokens = "attributes.'gen_ai.usage.input_tokens'::Int64"
	colOutputTokens = "attributes.'gen_ai.usage.output_tokens'::Int64"
)

type Repository interface {
	GetTraceSpans(teamID int64, traceID string) ([]map[string]any, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetTraceSpans(teamID int64, traceID string) ([]map[string]any, error) {
	query := fmt.Sprintf(`
		SELECT s.span_id, s.parent_span_id, s.service_name,
		       s.name AS operation_name, s.timestamp,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       s.has_error, s.kind_string,
		       %s AS model,
		       %s AS input_tokens,
		       %s AS output_tokens
		FROM %s s
		WHERE s.team_id = ? AND s.trace_id = ?
		ORDER BY s.timestamp ASC
		LIMIT 500
	`, colModel, colInputTokens, colOutputTokens, tableSpans)
	return dbutil.QueryMaps(r.db, query, uint32(teamID), traceID)
}
