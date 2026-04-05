package detail

import (
	"context"
	"fmt"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
)

type Repository interface {
	GetLogByID(ctx context.Context, teamID int64, logID string) (shared.LogRowDTO, error)
	GetSurroundingBefore(ctx context.Context, teamID int64, service string, tsLow, tsHigh, anchorTs time.Time, logID string, limit int) ([]shared.LogRowDTO, error)
	GetSurroundingAfter(ctx context.Context, teamID int64, service string, tsLow, tsHigh, anchorTs time.Time, logID string, limit int) ([]shared.LogRowDTO, error)
	GetLogByTraceSpanWindow(ctx context.Context, teamID int64, traceID, spanID string, from, to time.Time) (shared.LogRowDTO, error)
	GetContextLogs(ctx context.Context, teamID int64, service string, from, to time.Time) ([]shared.LogRowDTO, error)
}

type ClickHouseRepository struct {
	db *database.NativeQuerier
}

func NewRepository(db *database.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetLogByID(ctx context.Context, teamID int64, logID string) (shared.LogRowDTO, error) {
	var rows []shared.LogRowDTO
	err := r.db.Select(ctx, &rows,
		fmt.Sprintf(`SELECT %s FROM observability.logs WHERE team_id = ? AND id = ? LIMIT 1`, shared.LogColumns),
		teamID, logID,
	)
	if err != nil || len(rows) == 0 {
		return shared.LogRowDTO{}, err
	}
	return rows[0], nil
}

func (r *ClickHouseRepository) GetSurroundingBefore(ctx context.Context, teamID int64, service string, tsLow, tsHigh, anchorTs time.Time, logID string, limit int) ([]shared.LogRowDTO, error) {
	var rows []shared.LogRowDTO
	err := r.db.Select(ctx, &rows,
		fmt.Sprintf(`SELECT %s FROM observability.logs WHERE team_id = ? AND service = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ? AND (timestamp < ? OR (timestamp = ? AND id < ?)) ORDER BY timestamp DESC, id DESC LIMIT ?`, shared.LogColumns),
		teamID, service, timebucket.LogsBucketStart(tsLow.Unix()), timebucket.LogsBucketStart(tsHigh.Unix()), tsLow, tsHigh, anchorTs, anchorTs, logID, limit,
	)
	return rows, err
}

func (r *ClickHouseRepository) GetSurroundingAfter(ctx context.Context, teamID int64, service string, tsLow, tsHigh, anchorTs time.Time, logID string, limit int) ([]shared.LogRowDTO, error) {
	var rows []shared.LogRowDTO
	err := r.db.Select(ctx, &rows,
		fmt.Sprintf(`SELECT %s FROM observability.logs WHERE team_id = ? AND service = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ? AND (timestamp > ? OR (timestamp = ? AND id > ?)) ORDER BY timestamp ASC, id ASC LIMIT ?`, shared.LogColumns),
		teamID, service, timebucket.LogsBucketStart(tsLow.Unix()), timebucket.LogsBucketStart(tsHigh.Unix()), tsLow, tsHigh, anchorTs, anchorTs, logID, limit,
	)
	return rows, err
}

func (r *ClickHouseRepository) GetLogByTraceSpanWindow(ctx context.Context, teamID int64, traceID, spanID string, from, to time.Time) (shared.LogRowDTO, error) {
	var rows []shared.LogRowDTO
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s FROM observability.logs
		WHERE team_id = ? AND trace_id = ? AND span_id = ?
		  AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp DESC LIMIT 1
	`, shared.LogColumns), teamID, traceID, spanID, from, to)
	if err != nil || len(rows) == 0 {
		return shared.LogRowDTO{}, err
	}
	return rows[0], nil
}

func (r *ClickHouseRepository) GetContextLogs(ctx context.Context, teamID int64, service string, from, to time.Time) ([]shared.LogRowDTO, error) {
	bucketLow := timebucket.LogsBucketStart(from.Unix())
	bucketHigh := timebucket.LogsBucketStart(to.Unix())
	var rows []shared.LogRowDTO
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s FROM observability.logs
		WHERE team_id = ? AND service = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp ASC LIMIT 100
	`, shared.LogColumns), teamID, service, bucketLow, bucketHigh, from, to)
	return rows, err
}
