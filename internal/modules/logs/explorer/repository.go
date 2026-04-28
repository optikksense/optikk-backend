package explorer

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/filter"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

// Repository owns the list-only path on raw logs. Single-detail (GetByID),
// facets, summary/trend, and analytics live in their own sibling packages.
type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// ListLogs runs a keyset-paginated scan of raw logs ordered by
// (timestamp, observed_timestamp, trace_id) DESC. The inline CTE resolves
// resource-dim filters in the same round-trip as the main scan.
func (r *Repository) ListLogs(ctx context.Context, f filter.Filters, limit int, cur models.Cursor) ([]models.LogRow, bool, error) {
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

	query := `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.logs_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd` + resourceWhere + `
		)
		SELECT ` + models.LogColumns + `
		FROM observability.logs
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end` + where + `
		ORDER BY timestamp DESC, observed_timestamp DESC, trace_id DESC
		LIMIT @pgLimit`

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
