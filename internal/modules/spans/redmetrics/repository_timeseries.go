package redmetrics

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	rootspan "github.com/observability/observability-backend-go/internal/modules/spans/shared/rootspan"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// bucketSecs returns the bucket width in seconds matching the adaptive strategy.
func bucketSecs(startMs, endMs int64) float64 {
	h := (endMs - startMs) / 3_600_000
	switch {
	case h <= 3:
		return 60.0
	case h <= 12:
		return 300.0
	default:
		return 3600.0
	}
}

func (r *ClickHouseRepository) GetRequestRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceRatePoint, error) {
	bs := bucketSecs(startMs, endMs)
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       s.service_name,
		       count() / @bucketSeconds AS rps
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		  AND `+rootspan.Condition("s")+`
		GROUP BY timestamp, s.service_name
		ORDER BY timestamp ASC, s.service_name ASC
	`, bucket)
	var rows []ServiceRatePoint
	err := r.db.Select(ctx, &rows, query,
		clickhouse.Named("bucketSeconds", bs),
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetErrorRateTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceErrorRatePoint, error) {
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       s.service_name,
		       if(count() > 0,
		          countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400) * 100.0 / count(),
		          0) AS error_pct
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		  AND `+rootspan.Condition("s")+`
		GROUP BY timestamp, s.service_name
		ORDER BY timestamp ASC, s.service_name ASC
	`, bucket)
	var rows []ServiceErrorRatePoint
	err := r.db.Select(ctx, &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetP95LatencyTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceLatencyPoint, error) {
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       s.service_name,
		       quantileExact(0.95)(s.duration_nano / 1000000.0) AS p95_ms
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		  AND `+rootspan.Condition("s")+`
		GROUP BY timestamp, s.service_name
		ORDER BY timestamp ASC, s.service_name ASC
	`, bucket)
	var rows []ServiceLatencyPoint
	err := r.db.Select(ctx, &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetSpanKindBreakdown(ctx context.Context, teamID int64, startMs, endMs int64) ([]SpanKindPoint, error) {
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       s.kind_string,
		       toInt64(count()) AS span_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		  AND s.kind_string != ''
		GROUP BY timestamp, s.kind_string
		ORDER BY timestamp ASC, s.kind_string ASC
	`, bucket)
	var rows []SpanKindPoint
	err := r.db.Select(ctx, &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}

func (r *ClickHouseRepository) GetErrorsByRoute(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorByRoutePoint, error) {
	bucket := timebucket.ExprForColumnTime(startMs, endMs, "s.timestamp")
	query := fmt.Sprintf(`
		SELECT %s AS timestamp,
		       s.mat_http_route AS http_route,
		       toInt64(countIf(s.has_error = true OR toUInt16OrZero(s.response_status_code) >= 400)) AS error_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end
		  AND s.mat_http_route != ''
		GROUP BY timestamp, http_route
		ORDER BY timestamp ASC, error_count DESC
	`, bucket)
	var rows []ErrorByRoutePoint
	err := r.db.Select(ctx, &rows, query,
		clickhouse.Named("teamID", uint32(teamID)),
		clickhouse.Named("bucketStart", timebucket.SpansBucketStart(startMs/1000)),
		clickhouse.Named("bucketEnd", timebucket.SpansBucketStart(endMs/1000)),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	)
	return rows, err
}
