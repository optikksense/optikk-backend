package explorer

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// listCTEHead / listCTETail / listBareHead — list query in two shapes. With
// resource predicate: active_fps CTE narrows fingerprints first, then the
// main scan PREWHEREs `fingerprint IN active_fps`. Without: skip the CTE
// (active_fps would equal "every fingerprint in window" — pure overhead);
// granule pruning still tight via leading PK (team_id, ts_bucket).
//
// `timestamp BETWEEN @start AND @end` is in PREWHERE so CH uses the
// per-granule DateTime64 min/max stat to prune within a bucket; explicit
// PREWHERE disables CH's auto-promotion, so the move must be manual. The
// same condition stays in WHERE as the base for filter clauses to tack onto.
const listCTEHead = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.logs_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd`
const listCTETail = `
		)
		SELECT ` + models.LogColumns + `
		FROM observability.logs
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end`
const listBareHead = `
		SELECT ` + models.LogColumns + `
		FROM observability.logs
		PREWHERE team_id = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND timestamp BETWEEN @start AND @end
		WHERE timestamp BETWEEN @start AND @end`
const listTail = `
		ORDER BY timestamp DESC, observed_timestamp DESC, trace_id DESC
		LIMIT @pgLimit`

func (r *Repository) getLogs(ctx context.Context, f filter.Filters, limit int, cur models.Cursor) ([]models.LogRow, bool, error) {
	resourceWhere, where, args := filter.BuildClauses(f)
	if !cur.IsZero() {
		where += ` AND (timestamp, observed_timestamp, trace_id) < (@curTs, @curOts, @curTid)`
		args = append(args,
			clickhouse.Named("curTs", cur.Timestamp),
			clickhouse.Named("curOts", cur.ObservedTimestamp),
			clickhouse.Named("curTid", cur.TraceID),
		)
	}
	args = append(args, clickhouse.Named("pgLimit", uint64(limit+1)))

	var query string
	if resourceWhere == "" {
		query = listBareHead + where + listTail
	} else {
		query = listCTEHead + resourceWhere + listCTETail + where + listTail
	}

	var rows []models.LogRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "logs.ListLogs", &rows, query, args...); err != nil {
		return nil, false, err
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	return rows, hasMore, nil
}
