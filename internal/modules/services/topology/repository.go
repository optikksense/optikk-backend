package topology

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

// Repository runs ClickHouse queries that power the runtime service topology.
type Repository interface {
	GetNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]nodeAggRow, error)
	GetEdges(ctx context.Context, teamID int64, startMs, endMs int64) ([]edgeAggRow, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func spanRangeArgs(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// GetNodes aggregates per-service RED metrics. Only inbound spans (SERVER /
// CONSUMER) are counted so that services which also emit CLIENT spans are not
// double-counted.
func (r *ClickHouseRepository) GetNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]nodeAggRow, error) {
	var rows []nodeAggRow
	err := r.db.Select(ctx, &rows, `
		SELECT service_name,
		       toInt64(count()) AS request_count,
		       toInt64(countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400)) AS error_count,
		       quantile(0.50)(duration_nano / 1000000.0) AS p50_ms,
		       quantile(0.95)(duration_nano / 1000000.0) AS p95_ms,
		       quantile(0.99)(duration_nano / 1000000.0) AS p99_ms
		FROM observability.spans
		WHERE team_id = @teamID
		  AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND kind_string IN ('SERVER', 'CONSUMER')
		  AND service_name != ''
		GROUP BY service_name
	`, spanRangeArgs(teamID, startMs, endMs)...)
	return rows, err
}

// GetEdges derives service-to-service call edges from parent→child span joins
// with RED metrics computed on the callee (child) span.
func (r *ClickHouseRepository) GetEdges(ctx context.Context, teamID int64, startMs, endMs int64) ([]edgeAggRow, error) {
	var rows []edgeAggRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.service_name AS source,
		       c.service_name AS target,
		       toInt64(count()) AS call_count,
		       toInt64(countIf(c.has_error = true OR toUInt16OrZero(c.response_status_code) >= 400)) AS error_count,
		       quantile(0.50)(c.duration_nano / 1000000.0) AS p50_ms,
		       quantile(0.95)(c.duration_nano / 1000000.0) AS p95_ms
		FROM observability.spans s
		INNER JOIN observability.spans c
		  ON s.span_id = c.parent_span_id AND s.trace_id = c.trace_id
		WHERE s.team_id = @teamID
		  AND c.team_id = @teamID
		  AND s.service_name != ''
		  AND c.service_name != ''
		  AND s.service_name != c.service_name
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND c.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		  AND c.timestamp BETWEEN @start AND @end
		GROUP BY source, target
	`, spanRangeArgs(teamID, startMs, endMs)...)
	return rows, err
}
