package servicemap

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
}

func (r *Repository) GetServiceMapSpans(ctx context.Context, teamID int64, traceID string) ([]serviceMapSpanRow, error) {
	const query = `
		WITH trace_loc AS (
		    SELECT ts_bucket, fingerprint
		    FROM observability.trace_index
		    PREWHERE trace_id = @traceID AND team_id = @teamID
		)
		SELECT span_id,
		       parent_span_id,
		       service,
		       duration_nano / 1000000.0 AS duration_ms,
		       has_error
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND (ts_bucket, fingerprint) IN (SELECT ts_bucket, fingerprint FROM trace_loc)
		     AND trace_id = @traceID
		ORDER BY timestamp ASC
		LIMIT 10000`
	var rows []serviceMapSpanRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "servicemap.GetServiceMapSpans", &rows, query, traceIDArgs(teamID, traceID)...)
}

func (r *Repository) GetTraceErrors(ctx context.Context, teamID int64, traceID string) ([]traceErrorRow, error) {
	const query = `
		WITH trace_loc AS (
		    SELECT ts_bucket, fingerprint
		    FROM observability.trace_index
		    PREWHERE trace_id = @traceID AND team_id = @teamID
		)
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
		     AND (ts_bucket, fingerprint) IN (SELECT ts_bucket, fingerprint FROM trace_loc)
		     AND trace_id = @traceID
		WHERE has_error = true OR status_code_string = 'ERROR'
		ORDER BY timestamp ASC
		LIMIT 1000`
	var rows []traceErrorRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "servicemap.GetTraceErrors", &rows, query, traceIDArgs(teamID, traceID)...)
}

func traceIDArgs(teamID int64, traceID string) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
	}
}
