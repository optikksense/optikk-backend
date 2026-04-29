package logdetail

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
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
	     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
	     AND trace_id = @traceID
	     AND span_id = @spanID
	WHERE timestamp = fromUnixTimestamp64Nano(@tsNs, 'UTC')
	LIMIT 1`

func (r *Repository) GetByID(ctx context.Context, teamID int64, traceID, spanID string, tsNs int64) (*models.LogRow, error) {
	tsBucket := timebucket.BucketStart(tsNs / 1_000_000_000)
	tolerance := uint32(timebucket.BucketSeconds * 3)
	bucketStart := tsBucket - tolerance
	bucketEnd := tsBucket + tolerance
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("traceID", traceID),
		clickhouse.Named("spanID", spanID),
		clickhouse.Named("tsNs", tsNs),
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
