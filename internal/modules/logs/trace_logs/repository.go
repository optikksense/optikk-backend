package trace_logs

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// Step 1: resolve (ts_bucket bounds, fingerprint set) from trace_index.
// Sub-millisecond — first two PK slots pinned, returns one tiny aggregate row.
const boundsQuery = `
	SELECT min(ts_bucket)              AS min_b,
	       max(ts_bucket)              AS max_b,
	       groupUniqArray(fingerprint) AS fps,
	       count()                     AS n
	FROM observability.trace_index
	PREWHERE ` + traceIDMatchPredicate + `
	     AND team_id = @teamID`

// Step 2: scan observability.logs within the narrowed window. PREWHERE pins
// three PK slots — (team_id, ts_bucket BETWEEN, fingerprint IN @fps) — so
// granule pruning is tight; the trace_id row-side check filters whatever
// survives within each granule.
const fetchQuery = `
	SELECT ` + models.LogColumns + `
	FROM observability.logs
	PREWHERE team_id = @teamID
	     AND ts_bucket BETWEEN @minB AND @maxB
	     AND fingerprint IN @fps
	     AND ` + traceIDMatchPredicate + `
	ORDER BY timestamp ASC
	LIMIT @limit`

type boundsRow struct {
	MinB  uint32   `ch:"min_b"`
	MaxB  uint32   `ch:"max_b"`
	Fps   []string `ch:"fps"`
	Count uint64   `ch:"n"`
}

// LookupBounds resolves (ts_bucket bounds, fingerprint set, count) for a
// (team_id, trace_id) pair via the trace_index reverse-projection table.
// Returns count=0 when the trace has no logs (or the MV hasn't materialized
// yet).
func (r *Repository) LookupBounds(ctx context.Context, teamID int64, traceID string) (boundsRow, error) {
	var rows []boundsRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "logsTraceLogs.LookupBounds", &rows, boundsQuery, traceIDArgs(teamID, traceID)...); err != nil {
		return boundsRow{}, err
	}
	if len(rows) == 0 {
		return boundsRow{}, nil
	}
	return rows[0], nil
}

// FetchByBounds scans observability.logs for the given (team_id, trace_id)
// constrained to the narrowed (ts_bucket bounds, fingerprint set) supplied by
// LookupBounds. PREWHEREs three PK slots so granule pruning is tight.
func (r *Repository) FetchByBounds(ctx context.Context, teamID int64, traceID string, minB, maxB uint32, fps []string, limit int) ([]models.LogRow, error) {
	args := append(traceIDArgs(teamID, traceID),
		clickhouse.Named("minB", minB),
		clickhouse.Named("maxB", maxB),
		clickhouse.Named("fps", fps),
		clickhouse.Named("limit", uint64(limit)), //nolint:gosec // limit clamped in handler
	)
	var rows []models.LogRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "logsTraceLogs.FetchByBounds", &rows, fetchQuery, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// traceIDMatchPredicate normalizes trace_id across the empty / 32-zero-hex /
// case-insensitive forms OTLP can emit. Mirrors tracedetail's predicate;
// inlined locally per the apm-style convention (no shared helper package).
const traceIDMatchPredicate = `(
		lowerUTF8(trace_id) = lowerUTF8(@traceID)
		OR (length(@traceID) = 0 AND trace_id = '00000000000000000000000000000000')
		OR (lowerUTF8(@traceID) = '00000000000000000000000000000000' AND length(trace_id) = 0)
	)`

func traceIDArgs(teamID int64, traceID string) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // domain-bounded
		clickhouse.Named("traceID", traceID),
	}
}
