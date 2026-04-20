package search

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
)

type Repository interface {
	GetLogs(ctx context.Context, f shared.LogFilters, limit int, direction string, cursor shared.LogCursor) ([]shared.LogRowDTO, bool, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetLogs(ctx context.Context, f shared.LogFilters, limit int, direction string, cursor shared.LogCursor) ([]shared.LogRowDTO, bool, error) {
	prewhere, where, args := shared.BuildLogWhereSplit(f)
	orderDir := "DESC"
	cmp := "<"
	if direction == "asc" {
		orderDir = "ASC"
		cmp = ">"
	}

	if !cursor.IsZero() {
		where += fmt.Sprintf(` AND (timestamp, observed_timestamp, trace_id) %s (?, ?, ?)`, cmp)
		args = append(args, cursor.Timestamp, cursor.ObservedTimestamp, cursor.TraceID)
	}

	orderBy := fmt.Sprintf(`timestamp %s, observed_timestamp %s, trace_id %s`, orderDir, orderDir, orderDir)
	whereClause := ""
	if where != "" {
		whereClause = " WHERE" + where
	}
	query := fmt.Sprintf(
		`SELECT %s FROM observability.logs PREWHERE%s%s ORDER BY %s LIMIT ?`,
		shared.LogColumns, prewhere, whereClause, orderBy,
	)
	args = append(args, limit+1)

	var rows []shared.LogRowDTO
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, false, err
	}

	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	return rows, hasMore, nil
}
