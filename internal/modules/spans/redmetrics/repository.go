package redmetrics

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	rootspan "github.com/observability/observability-backend-go/internal/modules/spans/shared/rootspan"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

type Repository interface {
	GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) ([]redSummaryServiceRow, error)
	GetServiceScorecard(ctx context.Context, teamID int64, startMs, endMs int64) ([]scorecardRow, error)
	GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]apdexRow, error)
	GetHTTPStatusDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]httpStatusBucketDTO, error)
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

func (r *ClickHouseRepository) GetServiceScorecard(ctx context.Context, teamID int64, startMs, endMs int64) ([]scorecardRow, error) {
	var rows []scorecardRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.service_name AS service_name,
		       toInt64(count()) AS total_count,
		       toInt64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)) AS error_count,
		       quantileExact(0.95)(s.duration_nano / 1000000.0)                             AS p95_ms
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND `+rootspan.Condition("s")+` AND s.timestamp BETWEEN @start AND @end
		GROUP BY s.service_name
		ORDER BY total_count DESC
		LIMIT 1000
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

func (r *ClickHouseRepository) GetHTTPStatusDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]HTTPStatusBucket, error) {
	var rows []HTTPStatusBucket
	err := r.db.Select(ctx, &rows, `
		SELECT toInt64(toUInt16OrZero(response_status_code)) AS status_code,
		       toInt64(count())                               AS span_count
		FROM observability.spans
		WHERE team_id = @teamID AND ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND timestamp BETWEEN @start AND @end
		  AND response_status_code != ''
		GROUP BY status_code
		ORDER BY status_code ASC
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}
