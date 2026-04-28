package trace_servicemap //nolint:revive,stylecheck

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

// Repository runs trace-id-scoped point lookups for the per-trace service
// map. Queries only — service.go folds rows into nodes/edges and groups
// errors by exception type.
type Repository interface {
	GetServiceMapSpans(ctx context.Context, teamID int64, traceID string) ([]serviceMapSpanRow, error)
	GetTraceErrors(ctx context.Context, teamID int64, traceID string) ([]traceErrorRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// GetServiceMapSpans returns the minimal per-span projection needed to build a
// per-trace service map (client→server graph) in the service layer.
func (r *ClickHouseRepository) GetServiceMapSpans(ctx context.Context, teamID int64, traceID string) ([]serviceMapSpanRow, error) {
	const query = `
		SELECT span_id,
		       parent_span_id,
		       service,
		       duration_nano / 1000000.0 AS duration_ms,
		       has_error
		FROM observability.spans
		PREWHERE team_id = @teamID
		WHERE ` + traceIDMatchPredicate + `
		ORDER BY timestamp ASC
		LIMIT 10000`
	var rows []serviceMapSpanRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trace_servicemap.GetServiceMapSpans", &rows, query, traceIDArgs(teamID, traceID)...)
}

// GetTraceErrors returns the raw error-span rows for a trace. Aggregation into
// groups by exception_type happens in the service layer.
func (r *ClickHouseRepository) GetTraceErrors(ctx context.Context, teamID int64, traceID string) ([]traceErrorRow, error) {
	const query = `
		SELECT span_id,
		       service,
		       name                       AS operation_name,
		       exception_type,
		       exception_message,
		       status_message,
		       timestamp                  AS start_time,
		       duration_nano / 1000000.0  AS duration_ms
		FROM observability.spans
		PREWHERE team_id = @teamID
		WHERE ` + traceIDMatchPredicate + `
		  AND (has_error = true OR status_code_string = 'ERROR')
		ORDER BY timestamp ASC
		LIMIT 1000`
	var rows []traceErrorRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trace_servicemap.GetTraceErrors", &rows, query, traceIDArgs(teamID, traceID)...)
}

// traceIDMatchPredicate normalizes trace_id across the empty string + 32-char
// all-zero hex sentinel forms OTLP allows, plus hex casing. Inlined per the
// project rule that retired the shared traces/shared/traceidmatch helper.
const traceIDMatchPredicate = `(
		lowerUTF8(trace_id) = lowerUTF8(@traceID)
		OR (length(@traceID) = 0 AND trace_id = '00000000000000000000000000000000')
		OR (lowerUTF8(@traceID) = '00000000000000000000000000000000' AND length(trace_id) = 0)
	)`

func traceIDArgs(teamID int64, traceID string) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("traceID", traceID),
	}
}
