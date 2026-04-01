package search

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
)

type Repository interface {
	GetLogs(ctx context.Context, f shared.LogFilters, limit int, direction string, cursor shared.LogCursor) ([]shared.LogRowDTO, int64, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetLogs(ctx context.Context, f shared.LogFilters, limit int, direction string, cursor shared.LogCursor) ([]shared.LogRowDTO, int64, error) {
	where, args := shared.BuildLogWhere(f)
	orderDir := "DESC"
	if direction == "asc" {
		orderDir = "ASC"
	}

	orderBy := fmt.Sprintf(`timestamp %s, id %s`, orderDir, orderDir)
	offset := cursor.Offset

	query := fmt.Sprintf(`SELECT %s FROM observability.logs WHERE%s ORDER BY %s LIMIT ?`, shared.LogColumns, where, orderBy)
	queryArgs := make([]any, 0, len(args)+2)
	queryArgs = append(queryArgs, args...)
	queryArgs = append(queryArgs, limit)
	if offset > 0 {
		query += ` OFFSET ?`
		queryArgs = append(queryArgs, offset)
	}

	var rows []shared.LogRowDTO
	if err := r.db.Select(ctx, &rows, query, queryArgs...); err != nil {
		return nil, 0, err
	}

	// Always return the full filtered row count so clients can paginate correctly (offset+len(rows) is not a total).
	total := shared.QueryCount(ctx, r.db, `SELECT toInt64(COUNT(*)) AS count FROM observability.logs WHERE`+where, args...)

	return rows, total, nil
}
