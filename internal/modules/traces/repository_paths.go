package traces

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

<<<<<<< HEAD:internal/modules/traces/repository_paths.go
func (r *ClickHouseRepository) GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]criticalPathRow, error) {
=======
type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository {
	return &Repository{db: db}
}

func (r *Repository) GetCriticalPath(ctx context.Context, teamID int64, traceID string) ([]criticalPathRow, error) {
>>>>>>> f512576e76eb5e661aabd2a3202a40891770b326:internal/modules/traces/paths/repository.go
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

func (r *Repository) GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]errorPathRow, error) {
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
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
	}
}
