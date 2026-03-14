package tracecompare

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

type Repository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *Repository {
	return &Repository{db: db}
}

// FetchTraceSpans returns all spans for a trace, used by the comparison service.
func (r *Repository) FetchTraceSpans(teamID int64, traceID string) ([]internalSpan, error) {
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT s.span_id, s.parent_span_id, s.service_name, s.name AS operation_name,
		       s.kind_string AS span_kind,
		       s.duration_nano / 1000000.0 AS duration_ms,
		       s.status_code_string AS status,
		       s.has_error
		FROM observability.spans s
		WHERE s.team_id = ? AND s.trace_id = ?
		ORDER BY s.timestamp ASC
		LIMIT 5000
	`), uint32(teamID), traceID)
	if err != nil {
		return nil, err
	}

	spans := make([]internalSpan, 0, len(rows))
	for _, row := range rows {
		spans = append(spans, internalSpan{
			SpanID:     dbutil.StringFromAny(row["span_id"]),
			ParentID:   dbutil.StringFromAny(row["parent_span_id"]),
			Service:    dbutil.StringFromAny(row["service_name"]),
			Operation:  dbutil.StringFromAny(row["operation_name"]),
			SpanKind:   dbutil.StringFromAny(row["span_kind"]),
			DurationMs: dbutil.Float64FromAny(row["duration_ms"]),
			Status:     dbutil.StringFromAny(row["status"]),
			HasError:   dbutil.BoolFromAny(row["has_error"]),
		})
	}
	return spans, nil
}
