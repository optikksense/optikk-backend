package redmetrics

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	rootspan "github.com/Optikk-Org/optikk-backend/internal/modules/traces/shared/rootspan"
)

type Repository interface {
	GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) ([]redSummaryServiceRow, error)
	GetApdex(ctx context.Context, teamID int64, startMs, endMs int64, satisfiedMs, toleratingMs float64) ([]apdexRow, error)
	GetTopSlowOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]slowOperationRow, error)
	GetTopErrorOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]errorOperationRow, error)
	GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error)
	GetErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error)
	GetP95LatencyTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error)
	GetSpanKindBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]SpanKindPoint, error)
	GetErrorsByRoute(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error)
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
		       toInt64(count())                                                               AS total_count,
		       toInt64(countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400)) AS error_count,
		       quantileExact(0.50)(duration_nano / 1000000.0)                                AS p50_ms,
		       quantileExact(0.95)(duration_nano / 1000000.0)                                AS p95_ms,
		       quantileExact(0.99)(duration_nano / 1000000.0)                                AS p99_ms
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

func (r *ClickHouseRepository) GetTopSlowOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]slowOperationRow, error) {
	var rows []slowOperationRow
	err := r.db.Select(ctx, &rows, `
		SELECT service_name,
		       name                                           AS operation_name,
		       toInt64(count())                               AS span_count,
		       quantileExact(0.50)(duration_nano / 1000000.0) AS p50_ms,
		       quantileExact(0.95)(duration_nano / 1000000.0) AS p95_ms,
		       quantileExact(0.99)(duration_nano / 1000000.0) AS p99_ms
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND `+rootspan.Condition("s")+` AND s.timestamp BETWEEN @start AND @end
		GROUP BY service_name, operation_name
		ORDER BY p95_ms DESC
		LIMIT @limit
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("limit", limit),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetTopErrorOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]errorOperationRow, error) {
	var rows []errorOperationRow
	err := r.db.Select(ctx, &rows, `
		SELECT service_name,
		       name             AS operation_name,
		       toInt64(count()) AS total_count,
		       toInt64(countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400)) AS error_count,
		       error_count / total_count AS error_rate
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND `+rootspan.Condition("s")+` AND s.timestamp BETWEEN @start AND @end
		  AND (has_error = true OR toUInt16OrZero(response_status_code) >= 400)
		GROUP BY service_name, operation_name
		ORDER BY error_count DESC
		LIMIT @limit
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("limit", limit),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	var rows []ServiceRatePoint
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s                               AS timestamp,
		       service_name,
		       toInt64(count()) / 60.0          AS rps
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND `+rootspan.Condition("s")+` AND s.timestamp BETWEEN @start AND @end
		GROUP BY timestamp, service_name
		ORDER BY timestamp ASC
	`, bucket),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	var rows []ServiceErrorRatePoint
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s                                                                      AS timestamp,
		       service_name,
		       toInt64(count())                                                        AS request_count,
		       toInt64(countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400)) AS error_count,
		       error_count * 100.0 / request_count                                    AS error_pct
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND `+rootspan.Condition("s")+` AND s.timestamp BETWEEN @start AND @end
		GROUP BY timestamp, service_name
		ORDER BY timestamp ASC
	`, bucket),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetP95LatencyTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	var rows []ServiceLatencyPoint
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s                                              AS timestamp,
		       service_name,
		       quantileExact(0.95)(duration_nano / 1000000.0) AS p95_ms
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND `+rootspan.Condition("s")+` AND s.timestamp BETWEEN @start AND @end
		GROUP BY timestamp, service_name
		ORDER BY timestamp ASC
	`, bucket),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetSpanKindBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]SpanKindPoint, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	var rows []SpanKindPoint
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s                        AS timestamp,
		       kind_string               AS kind_string,
		       toInt64(count())          AS span_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		GROUP BY timestamp, kind_string
		ORDER BY timestamp ASC
	`, bucket),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetErrorsByRoute(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error) {
	bucket := timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
	var rows []ErrorByRoutePoint
	err := r.db.Select(ctx, &rows, fmt.Sprintf(`
		SELECT %s                                                                      AS timestamp,
		       mat_http_route                                                          AS http_route,
		       toInt64(count())                                                        AS request_count,
		       toInt64(countIf(has_error = true OR toUInt16OrZero(response_status_code) >= 400)) AS error_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		  AND mat_http_route != ''
		GROUP BY timestamp, http_route
		ORDER BY timestamp ASC, error_count DESC
	`, bucket),
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetLatencyBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]latencyBreakdownRow, error) {
	var rows []latencyBreakdownRow
	err := r.db.Select(ctx, &rows, `
		SELECT
			service_name,
			avg(duration_nano / 1000000.0) AS total_ms,
			toInt64(count())               AS span_count
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
