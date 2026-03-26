package tracecompare

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type Repository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *Repository {
	return &Repository{db: db}
}

// FetchTraceSpans returns all spans for a trace, used by the comparison service.
func (r *Repository) FetchTraceSpans(teamID int64, traceID string) ([]internalSpan, error) {
	var rows []internalSpan
	err := r.db.Select(context.Background(), &rows, `
		SELECT s.span_id, s.parent_span_id, s.service_name, s.name AS operation_name,
		       s.kind_string AS span_kind,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       s.status_code_string AS status,
		       s.has_error
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.trace_id = ?
		ORDER BY s.timestamp ASC
		LIMIT 5000
	`, clickhouse.Named("teamID", uint32(teamID)), traceID) //nolint:gosec // G115
	return rows, err
}
