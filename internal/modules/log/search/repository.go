package search

import (
	"context"
	"fmt"

	database "github.com/observability/observability-backend-go/internal/database"
	shared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"
)

type Repository interface {
	GetLogs(ctx context.Context, f shared.LogFilters, limit int, direction string, cursor shared.LogCursor) ([]shared.LogRowDTO, int64, error)
}

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) *ClickHouseRepository {
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
	queryArgs := append(args, limit)
	if offset > 0 {
		query += ` OFFSET ?`
		queryArgs = append(queryArgs, offset)
	}

	var rows []shared.LogRowDTO
	if err := r.db.Select(ctx, &rows, query, queryArgs...); err != nil {
		return nil, 0, err
	}

	var total int64
	if offset == 0 {
		total = shared.QueryCount(ctx, r.db, `SELECT COUNT(*) AS count FROM observability.logs WHERE`+where, args...)
	} else {
		total = int64(offset + len(rows))
	}

	return rows, total, nil
}
