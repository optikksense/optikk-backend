package trace_paths //nolint:revive,stylecheck

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
		SELECT span_id,
		       parent_span_id,
		       name                                                AS operation_name,
		       service,
		       duration_nano / 1000000.0                           AS duration_ms,
		       toUnixTimestamp64Nano(timestamp)                    AS start_ns,
		       toUnixTimestamp64Nano(timestamp) + duration_nano    AS end_ns
		FROM observability.spans
		PREWHERE team_id = @teamID
		WHERE ` + traceIDMatchPredicate + `
		ORDER BY start_ns ASC
		LIMIT 5000`
	var rows []criticalPathRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trace_paths.GetCriticalPath", &rows, query, traceIDArgs(teamID, traceID)...)
}

func (r *ClickHouseRepository) GetErrorPath(ctx context.Context, teamID int64, traceID string) ([]errorPathRow, error) {
	const query = `
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
		WHERE ` + traceIDMatchPredicate + `
		  AND (has_error = true OR status_code_string = 'ERROR')
		ORDER BY timestamp ASC
		LIMIT 1000`
	var rows []errorPathRow
	return rows, dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "trace_paths.GetErrorPath", &rows, query, traceIDArgs(teamID, traceID)...)
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

// isRootParentSpanID treats empty string and all-zero hex as "no parent" —
// both forms appear in real data depending on SDK and ingest path.
func isRootParentSpanID(parentID string) bool {
	trimmed := strings.Trim(parentID, "\x00")
	return trimmed == "" || trimmed == "0000000000000000"
}
