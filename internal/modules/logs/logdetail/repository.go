package logdetail

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/logs/shared/models"
)

type Repository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *Repository { return &Repository{db: db} }

const getByIDQuery = `
	SELECT ` + models.LogColumns + `
	FROM observability.logs
	PREWHERE team_id = @teamID
	     AND log_id = @logID
	LIMIT 1`

// GetByID resolves a single log row by its stable log_id.
// It queries ClickHouse by teamID and logID using the log_id skip-index.
func (r *Repository) GetByID(ctx context.Context, teamID int64, logID string) (*models.LogRow, error) {
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("logID", logID),
	}
	var rows []models.LogRow
	if err := dbutil.SelectCH(dbutil.ExplorerCtx(ctx), r.db, "logsDetail.GetByID", &rows, getByIDQuery, args...); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}
