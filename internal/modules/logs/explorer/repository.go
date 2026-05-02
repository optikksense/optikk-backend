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
