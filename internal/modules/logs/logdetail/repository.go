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

// GetByID resolves a single log row by its stable log_id (FNV-64a hex computed
// by the ingestion mapper from trace_id, timestamp_ns, body, fingerprint).
// PREWHERE is on (team_id, log_id) only — the idx_log_id bloom-filter
// skip-index defined inline in db/clickhouse/02_logs.sql prunes granules
// across the full 30-day partition set without needing a ts_bucket bound.
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
