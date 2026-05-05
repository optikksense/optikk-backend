package paths

import (
	"context"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

// Repository runs trace-id-scoped point lookups for path reconstruction.
// Queries only — graph traversal (critical path / error chain) lives in
// service.go.
type Repository interface {
	GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]criticalPathRow, error)
	GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]errorPathRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]criticalPathRow, error) {
	const query = `
		WITH trace_loc AS (
		    SELECT ts_bucket, fingerprint
		    FROM observability.trace_index
		    PREWHERE trace_id = @traceID AND team_id = @teamID
		)
		SELECT span_id,
		       parent_span_id,
		       name                       AS operation_name,
		       service,
		       duration_nano / 1000000.0  AS duration_ms,
		       timestamp,
		       duration_nano
		FROM observability.spans
		PREWHERE team_id = @teamID
		     AND (ts_bucket, fingerprint) IN (SELECT ts_bucket, fingerprint FROM trace_loc)
		     AND trace_id = @traceID
		ORDER BY timestamp ASC
		LIMIT 5000`
	var rows []criticalPathRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "paths.GetCriticalPath", &rows, query, traceIDArgs(teamID, traceID)...)
}

func (r *ClickHouseRepository) GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]errorPathRow, error) {
	const query = `
		WITH trace_loc AS (
		    SELECT ts_bucket, fingerprint
		    FROM observability.trace_index
		    PREWHERE trace_id = @traceID AND team_id = @teamID
		)
		SELECT span_id,
		       parent_span_id,
		       name                       AS operation_name,
		       service                    AS service,
		       status_code_string         AS status,
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
	var rows []errorPathRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "paths.GetErrorPath", &rows, query, traceIDArgs(teamID, traceID)...)
}

func traceIDArgs(teamID int64, traceID string) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("traceID", traceID),
	}
}

// isRootParentSpanID treats empty string and all-zero hex as "no parent" —
// both forms appear in real data depending on SDK and ingest path.
func isRootParentSpanID(parentID string) bool {
	trimmed := strings.Trim(parentID, "\x00")
	return trimmed == "" || trimmed == "0000000000000000"
}
