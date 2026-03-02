package logs

import (
	"context"
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// ClickHouseRepository is the data access layer for logs.
type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// GetLogs returns paginated log entries. Facets are NOT included here —
// use GetLogFacets for a separate, parallelizable facet request.
func (r *ClickHouseRepository) GetLogs(ctx context.Context, f LogFilters, limit int, direction string, cursor LogCursor) ([]Log, int64, error) {
	where, args := buildLogWhere(f)
	orderDir := "DESC"
	if direction == "asc" {
		orderDir = "ASC"
	}

	orderBy := fmt.Sprintf(
		`timestamp %s, id %s, service_name %s, trace_id %s, span_id %s, message %s`,
		orderDir, orderDir, orderDir, orderDir, orderDir, orderDir,
	)

	offset := 0
	if cursor.Offset > 0 {
		offset = cursor.Offset
	}

	if offset == 0 && cursor.ID > 0 {
		if direction == "desc" {
			if cursor.HasTimestamp() {
				where += ` AND (timestamp < ? OR (timestamp = ? AND id < ?))`
				args = append(args, cursor.Timestamp, cursor.Timestamp, cursor.ID)
			} else {
				where += ` AND id < ?`
				args = append(args, cursor.ID)
			}
		} else {
			if cursor.HasTimestamp() {
				where += ` AND (timestamp > ? OR (timestamp = ? AND id > ?))`
				args = append(args, cursor.Timestamp, cursor.Timestamp, cursor.ID)
			} else {
				where += ` AND id > ?`
				args = append(args, cursor.ID)
			}
		}
	}

	query := fmt.Sprintf(`SELECT %s FROM logs WHERE%s ORDER BY %s LIMIT ?`, logCols, where, orderBy)
	queryArgs := append(args, limit)
	if offset > 0 {
		query += ` OFFSET ?`
		queryArgs = append(queryArgs, offset)
	}

	rows, err := dbutil.QueryMaps(r.db, query, queryArgs...)
	if err != nil {
		return nil, 0, err
	}

	logs := mapRowsToLogs(rows)

	// Count uses the same where clause (no cursor/offset) for total.
	total := dbutil.QueryCount(r.db, `SELECT COUNT(*) FROM logs WHERE`+where, args...)

	return logs, total, nil
}
