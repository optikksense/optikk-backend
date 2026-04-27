package explorer

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/querycompiler"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/resource"
)

// Repository owns the list-only path on raw logs. Single-detail (GetByID),
// facets, summary/trend, and analytics live in their own sibling packages.
type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// ListLogs runs a keyset-paginated scan of raw logs ordered by
// (timestamp, observed_timestamp, trace_id) DESC.
func (r *Repository) ListLogs(ctx context.Context, f querycompiler.Filters, limit int, cur models.Cursor) ([]models.LogRow, bool, error) {
	compiled := querycompiler.Compile(f, querycompiler.TargetRaw)
	where := compiled.Where
	args := compiled.Args
	preWhere, args, empty, err := resource.WithFingerprints(ctx, r.db, f, compiled.PreWhere, args)
	if err != nil {
		return nil, false, err
	}
	if empty {
		return nil, false, nil
	}
	if !cur.IsZero() {
		where += ` AND (timestamp, observed_timestamp, trace_id) < (@curTs, @curOts, @curTid)`
		args = append(args,
			clickhouse.Named("curTs", cur.Timestamp),
			clickhouse.Named("curOts", cur.ObservedTimestamp),
			clickhouse.Named("curTid", cur.TraceID),
		)
	}

	query := fmt.Sprintf(
		`SELECT %s FROM %s
		PREWHERE %s
		WHERE %s
		ORDER BY timestamp DESC, observed_timestamp DESC, trace_id DESC LIMIT @pgLimit`,
		models.LogColumns, models.RawLogsTable, preWhere, where,
	)
	args = append(args, clickhouse.Named("pgLimit", uint64(limit+1)))
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
