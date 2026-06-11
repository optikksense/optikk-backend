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

// boundsQuery resolves ts_bucket bounds and fingerprints from trace_index.
const boundsQuery = `
	SELECT min(ts_bucket)                    AS min_b,
	       max(ts_bucket)                    AS max_b,
	       groupUniqArray(1024)(fingerprint) AS fps,
	       count()                           AS n
	FROM observability.trace_index
	PREWHERE trace_id = @traceID
	     AND team_id = @teamID`

// fetchQuery scans observability.logs within the narrowed bucket window.
const fetchQuery = `
	SELECT ` + models.LogColumns + `
	FROM observability.logs
	PREWHERE team_id = @teamID
	     AND ts_bucket BETWEEN @minB AND @maxB
	     AND fingerprint IN @fps
	     AND trace_id = @traceID
	ORDER BY timestamp ASC
	LIMIT @limit`

type boundsRow struct {
	MinB  uint32   `ch:"min_b"`
	MaxB  uint32   `ch:"max_b"`
	Fps   []uint64 `ch:"fps"`
	Count uint64   `ch:"n"`
}

// LookupBounds resolves bucket bounds and fingerprints for a trace.
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

// FetchByBounds scans observability.logs constrained to the resolved bounds.
func (r *Repository) FetchByBounds(ctx context.Context, teamID int64, traceID string, minB, maxB uint32, fps []uint64, limit int) ([]models.LogRow, error) {
	args := append(traceIDArgs(teamID, traceID),
		clickhouse.Named("minB", minB),
		clickhouse.Named("maxB", maxB),
		clickhouse.Named("fps", fps),
		clickhouse.Named("limit", uint64(limit)),
	)
	var rows []models.LogRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "logsTraceLogs.FetchByBounds", &rows, fetchQuery, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func traceIDArgs(teamID int64, traceID string) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
	}
}
