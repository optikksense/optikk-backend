package redmetrics

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
)

func (r *ClickHouseRepository) GetTopSlowOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]SlowOperation, error) {
	var rows []SlowOperation
	err := r.db.Select(ctx, &rows, `
		SELECT s.name                                              AS operation_name,
		       s.service_name                                      AS service_name,
		       quantileExact(0.50)(s.duration_nano / 1000000.0)   AS p50_ms,
		       quantileExact(0.95)(s.duration_nano / 1000000.0)   AS p95_ms,
		       quantileExact(0.99)(s.duration_nano / 1000000.0)   AS p99_ms,
		       toInt64(count())                                    AS span_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		GROUP BY s.name, s.service_name
		ORDER BY p99_ms DESC
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

func (r *ClickHouseRepository) GetTopErrorOperations(ctx context.Context, teamID int64, startMs, endMs int64, limit int) ([]ErrorOperation, error) {
	var rows []ErrorOperation
	err := r.db.Select(ctx, &rows, `
		SELECT s.name                                                                                AS operation_name,
		       s.service_name                                                                        AS service_name,
		       s.mat_exception_type                                                                  AS exception_type,
		       toInt64(count())                                                                      AS total_count,
		       toInt64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)) AS error_count,
		       if(count() > 0,
		           countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) * 100.0 / count(),
		           0)                                                                                AS error_rate
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		GROUP BY s.name, s.service_name, exception_type
		ORDER BY error_rate DESC
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

func (r *ClickHouseRepository) GetLatencyBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]latencyBreakdownRow, error) {
	var rows []latencyBreakdownRow
	err := r.db.Select(ctx, &rows, `
		SELECT s.service_name,
		       sum(s.duration_nano) / 1000000.0 AS total_ms,
		       toInt64(count())                  AS span_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end
		GROUP BY s.service_name
		ORDER BY total_ms DESC
	`,
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}
