package tracelogs

import (
	"context"
	"fmt"

	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"

	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
)

type Repository interface {
	GetTraceMeta(ctx context.Context, teamID int64, traceID string) (*TraceMetaRowDTO, error)
	GetLogsByTraceID(ctx context.Context, teamID int64, traceID string) ([]shared.LogRowDTO, error)
	GetLogsByTraceWindow(ctx context.Context, teamID int64, traceID string, startNs, endNs uint64) ([]shared.LogRowDTO, error)
	GetFallbackLogs(ctx context.Context, teamID int64, serviceName string, startNs, endNs uint64, httpMethod, route, routeLike string) ([]shared.LogRowDTO, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetTraceMeta(ctx context.Context, teamID int64, traceID string) (*TraceMetaRowDTO, error) {
	var rows []TraceMetaRowDTO
	err := r.db.Select(ctx, &rows, `
		SELECT min(timestamp) as trace_start,
		       max(timestamp) as trace_end,
		       any(service_name) as service_name,
		       any(http_method) as http_method,
		       any(http_url) as http_url,
		       any(name) as operation_name
		FROM observability.spans
		WHERE team_id = ? AND trace_id = ?
	`, teamID, traceID)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return &rows[0], nil
}

func (r *ClickHouseRepository) GetLogsByTraceID(ctx context.Context, teamID int64, traceID string) ([]shared.LogRowDTO, error) {
	var rows []shared.LogRowDTO
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s FROM observability.logs
		WHERE team_id = ? AND trace_id = ?
		ORDER BY timestamp ASC LIMIT 500
	`, shared.LogColumns), teamID, traceID)
	return rows, err
}

func (r *ClickHouseRepository) GetLogsByTraceWindow(ctx context.Context, teamID int64, traceID string, startNs, endNs uint64) ([]shared.LogRowDTO, error) {
	bucketLow := timebucket.LogsBucketStart(int64(startNs / 1_000_000_000)) //nolint:gosec // G115
	bucketHigh := timebucket.LogsBucketStart(int64(endNs / 1_000_000_000))  //nolint:gosec // G115
	var rows []shared.LogRowDTO
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s FROM observability.logs
		WHERE team_id = ? AND trace_id = ?
		  AND ts_bucket_start BETWEEN ? AND ?
		  AND timestamp BETWEEN ? AND ?
		ORDER BY timestamp ASC LIMIT 500
	`, shared.LogColumns), teamID, traceID, bucketLow, bucketHigh, startNs, endNs)
	return rows, err
}

func (r *ClickHouseRepository) GetFallbackLogs(ctx context.Context, teamID int64, serviceName string, startNs, endNs uint64, httpMethod, route, routeLike string) ([]shared.LogRowDTO, error) {
	bucketLow := timebucket.LogsBucketStart(int64(startNs / 1_000_000_000)) //nolint:gosec // G115
	bucketHigh := timebucket.LogsBucketStart(int64(endNs / 1_000_000_000))  //nolint:gosec // G115
	var rows []shared.LogRowDTO
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s FROM observability.logs
		WHERE team_id = ? AND service = ?
		  AND ts_bucket_start BETWEEN ? AND ?
		  AND timestamp BETWEEN ? AND ?
		  AND (? = '' OR upper(attributes_string['http.method']) = ?)
		  AND (? = '' OR attributes_string['http.route'] = ? OR body LIKE ?)
		ORDER BY timestamp ASC LIMIT 500
	`, shared.LogColumns),
		teamID, serviceName, bucketLow, bucketHigh, startNs, endNs, httpMethod, httpMethod, route, route, routeLike,
	)
	return rows, err
}
