package topology

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

// Repository runs ClickHouse queries that power the runtime service topology.
// Error-rate computation is combinator-free: totals + errors scans run in
// parallel (errgroup) and are merged in Go (mirrors overview/errors).
type Repository interface {
	GetNodeTotals(ctx context.Context, teamID int64, startMs, endMs int64) ([]nodeTotalRow, error)
	GetNodeErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]nodeErrorLegRow, error)
	GetEdgeTotals(ctx context.Context, teamID int64, startMs, endMs int64) ([]edgeTotalRow, error)
	GetEdgeErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]edgeErrorLegRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
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

// GetNodeTotals scans per-service inbound totals. Only SERVER/CONSUMER spans
// are counted so services that also emit CLIENT spans are not double-counted.
// Count stays native uint64; service casts at boundary.
func (r *ClickHouseRepository) GetNodeTotals(ctx context.Context, teamID int64, startMs, endMs int64) ([]nodeTotalRow, error) {
	var rows []nodeTotalRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
		SELECT service_name,
		       count() AS request_count
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

// GetNodeErrors scans per-service error counts. Uses a UInt16 comparison on
// http_status_code (materialized column) so no toUInt16OrZero cast is needed.
func (r *ClickHouseRepository) GetNodeErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]nodeErrorLegRow, error) {
	var rows []nodeErrorLegRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
		SELECT service_name,
		       count() AS error_count
		FROM observability.spans
		WHERE team_id = @teamID
		  AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND timestamp BETWEEN @start AND @end
		  AND kind_string IN ('SERVER', 'CONSUMER')
		  AND service_name != ''
		  AND (has_error = true OR http_status_code >= 400)
		GROUP BY service_name
	`, spanRangeArgs(teamID, startMs, endMs)...)
	return rows, err
}

// GetEdgeTotals derives service-to-service call totals from parent→child span
// joins. Percentiles come from sketch.Querier (see service.go).
func (r *ClickHouseRepository) GetEdgeTotals(ctx context.Context, teamID int64, startMs, endMs int64) ([]edgeTotalRow, error) {
	var rows []edgeTotalRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
		SELECT s.service_name AS source,
		       c.service_name AS target,
		       count() AS call_count
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

// GetEdgeErrors scans error-only parent→child joins to merge with edge totals.
func (r *ClickHouseRepository) GetEdgeErrors(ctx context.Context, teamID int64, startMs, endMs int64) ([]edgeErrorLegRow, error) {
	var rows []edgeErrorLegRow
	err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, `
		SELECT s.service_name AS source,
		       c.service_name AS target,
		       count() AS error_count
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
		  AND (c.has_error = true OR c.http_status_code >= 400)
		GROUP BY source, target
	`, spanRangeArgs(teamID, startMs, endMs)...)
	return rows, err
}
