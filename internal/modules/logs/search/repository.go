package search

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
)

type Repository interface {
	GetLogs(ctx context.Context, f shared.LogFilters, limit int, direction string, cursor shared.LogCursor) ([]shared.LogRowDTO, bool, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetLogs(ctx context.Context, f shared.LogFilters, limit int, direction string, cursor shared.LogCursor) ([]shared.LogRowDTO, bool, error) {
	where, args := shared.BuildLogWhere(f)
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
	query := fmt.Sprintf(
		`SELECT %s FROM observability.logs WHERE%s ORDER BY %s LIMIT ?`,
		shared.LogColumns, where, orderBy,
	)
	args = append(args, limit+1)

	var rows []shared.LogRowDTO
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, false, err
	}

	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	return rows, hasMore, nil
}
