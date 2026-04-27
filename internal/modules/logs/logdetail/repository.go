package logdetail

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

// GetByID reads a single log by its deep-link key triple.
func (r *Repository) GetByID(ctx context.Context, teamID int64, traceID, spanID string, tsNs int64) (*models.LogRow, error) {
	query := fmt.Sprintf(
		`SELECT %s FROM %s
		PREWHERE team_id = @teamID AND trace_id = @traceID AND span_id = @spanID
		WHERE toUnixTimestamp64Nano(timestamp) = @tsNs LIMIT 1`,
		models.LogColumns, models.RawLogsTable,
	)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("traceID", traceID),
		clickhouse.Named("spanID", spanID),
		clickhouse.Named("tsNs", tsNs),
	}
	var rows []models.LogRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "logsDetail.GetByID", &rows, query, args...); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}
