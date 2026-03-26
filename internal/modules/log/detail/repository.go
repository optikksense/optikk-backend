package detail

import (
	"context"
	"fmt"

	"github.com/observability/observability-backend-go/internal/database"
	shared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"
	"github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository interface {
	GetLogByID(ctx context.Context, teamID int64, logID string) (shared.LogRowDTO, error)
	GetSurroundingBefore(ctx context.Context, teamID int64, service string, tsLow, tsHigh, anchorTs uint64, logID string, limit int) ([]shared.LogRowDTO, error)
	GetSurroundingAfter(ctx context.Context, teamID int64, service string, tsLow, tsHigh, anchorTs uint64, logID string, limit int) ([]shared.LogRowDTO, error)
	GetLogByTraceSpanWindow(ctx context.Context, teamID int64, traceID, spanID string, fromNs, toNs uint64) (shared.LogRowDTO, error)
	GetContextLogs(ctx context.Context, teamID int64, service string, fromNs, toNs uint64) ([]shared.LogRowDTO, error)
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

func (r *ClickHouseRepository) GetSurroundingBefore(ctx context.Context, teamID int64, service string, tsLow, tsHigh, anchorTs uint64, logID string, limit int) ([]shared.LogRowDTO, error) {
	var rows []shared.LogRowDTO
	err := r.db.Select(ctx, &rows,
		fmt.Sprintf(`SELECT %s FROM observability.logs WHERE team_id = ? AND service = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ? AND (timestamp < ? OR (timestamp = ? AND id < ?)) ORDER BY timestamp DESC, id DESC LIMIT ?`, shared.LogColumns),
		teamID, service, timebucket.LogsBucketStart(int64(tsLow/1_000_000_000)), timebucket.LogsBucketStart(int64(tsHigh/1_000_000_000)), tsLow, tsHigh, anchorTs, anchorTs, logID, limit, //nolint:gosec // G115
	)
	return rows, err
}

func (r *ClickHouseRepository) GetSurroundingAfter(ctx context.Context, teamID int64, service string, tsLow, tsHigh, anchorTs uint64, logID string, limit int) ([]shared.LogRowDTO, error) {
	var rows []shared.LogRowDTO
	err := r.db.Select(ctx, &rows,
		fmt.Sprintf(`SELECT %s FROM observability.logs WHERE team_id = ? AND service = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ? AND (timestamp > ? OR (timestamp = ? AND id > ?)) ORDER BY timestamp ASC, id ASC LIMIT ?`, shared.LogColumns),
		teamID, service, timebucket.LogsBucketStart(int64(tsLow/1_000_000_000)), timebucket.LogsBucketStart(int64(tsHigh/1_000_000_000)), tsLow, tsHigh, anchorTs, anchorTs, logID, limit, //nolint:gosec // G115
	)
	return rows, err
}

func (r *ClickHouseRepository) GetLogByTraceSpanWindow(ctx context.Context, teamID int64, traceID, spanID string, fromNs, toNs uint64) (shared.LogRowDTO, error) {
	var rows []shared.LogRowDTO
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s FROM observability.logs
		WHERE team_id = ? AND trace_id = ? AND span_id = ?
		  AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp DESC LIMIT 1
	`, shared.LogColumns), teamID, traceID, spanID, fromNs, toNs)
	if err != nil || len(rows) == 0 {
		return shared.LogRowDTO{}, err
	}
	return rows[0], nil
}

func (r *ClickHouseRepository) GetContextLogs(ctx context.Context, teamID int64, service string, fromNs, toNs uint64) ([]shared.LogRowDTO, error) {
	bucketLow := timebucket.LogsBucketStart(int64(fromNs / 1_000_000_000)) //nolint:gosec // G115
	bucketHigh := timebucket.LogsBucketStart(int64(toNs / 1_000_000_000))  //nolint:gosec // G115
	var rows []shared.LogRowDTO
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s FROM observability.logs
		WHERE team_id = ? AND service = ? AND ts_bucket_start BETWEEN ? AND ? AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp ASC LIMIT 100
	`, shared.LogColumns), teamID, service, bucketLow, bucketHigh, fromNs, toNs)
	return rows, err
}
