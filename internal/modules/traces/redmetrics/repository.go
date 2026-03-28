package redmetrics

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
)

type Repository interface {
	GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) ([]redSummaryServiceRow, error)
	GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]apdexRow, error)
	GetTopSlowOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]slowOperationDTO, error)
	GetTopErrorOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]errorOperationDTO, error)
	GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]serviceRatePointDTO, error)
	GetErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]serviceErrorRatePointDTO, error)
	GetP95LatencyTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]serviceLatencyPointDTO, error)
	GetSpanKindBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]spanKindPointDTO, error)
	GetErrorsByRoute(ctx context.Context, teamID int64, startMs, endMs int64) ([]errorByRoutePointDTO, error)
	GetLatencyBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]latencyBreakdownRow, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) ([]redSummaryServiceRow, error) {
	var rows []redSummaryServiceRow
	err := r.db.Select(ctx, &rows, `
		SELECT service_name,
		       toInt64(count())                                                           AS total_count,
		       toInt64(countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400)) AS error_count,
		       quantileExact(0.95)(duration_nano / 1000000.0)                            AS p95_ms
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND `+rootspan.Condition("s")+` AND s.timestamp BETWEEN @start AND @end
		GROUP BY service_name
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]apdexRow, error) {
	var rows []apdexRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.service_name AS service_name,
		       toInt64(countIf(s.duration_nano / 1000000.0 <= @satisfiedMs)) AS satisfied,
		       toInt64(countIf(s.duration_nano / 1000000.0 > @satisfiedMs AND s.duration_nano / 1000000.0 <= @toleratingMs)) AS tolerating,
		       toInt64(countIf(s.duration_nano / 1000000.0 > @toleratingMs)) AS frustrated,
		       toInt64(count()) AS total_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND `+rootspan.Condition("s")+` AND s.timestamp BETWEEN @start AND @end
		GROUP BY s.service_name
		ORDER BY total_count DESC
	`,
		clickhouse.Named("satisfiedMs", satisfiedMs),
		clickhouse.Named("toleratingMs", toleratingMs),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}
