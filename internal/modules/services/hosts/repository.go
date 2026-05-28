package hosts

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/timebucket"
	"github.com/Optikk-Org/optikk-backend/internal/modules/infrastructure/infraconsts"
)

const defaultUnknownHost = "unknown"

type Repository interface {
	QueryHostSpans(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]hostSpansRow, error)
	QueryHostMetric(ctx context.Context, teamID int64, startMs, endMs int64, metricName string) ([]hostMetricRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

// QueryHostSpans returns per-host RED aggregates from spans_1m restricted to
// the requested service.
func (r *ClickHouseRepository) QueryHostSpans(
	ctx context.Context, teamID int64, startMs, endMs int64, serviceName string,
) ([]hostSpansRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT DISTINCT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id   = @teamID
		         AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		         AND service   = @serviceName
		)
		SELECT
		    if(host != '', host, @unknownHost)        AS host,
		    any(environment)                          AS zone,
		    sum(request_count)                        AS request_count,
		    sum(error_count)                          AS error_count,
		    quantileTimingMerge(0.99)(latency_state)  AS p99_ms,
		    max(timestamp)                            AS last_seen
		FROM observability.spans_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND service   = @serviceName
		GROUP BY host
		ORDER BY request_count DESC`
	args := append(spanArgs(teamID, startMs, endMs),
		clickhouse.Named("serviceName", serviceName),
		clickhouse.Named("unknownHost", defaultUnknownHost),
	)
	var rows []hostSpansRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "hosts.QueryHostSpans",
		&rows, query, args...)
}

// QueryHostMetric returns the average value of a single host-keyed metric
// (e.g. system.cpu.utilization) per host across the time window.
func (r *ClickHouseRepository) QueryHostMetric(
	ctx context.Context, teamID int64, startMs, endMs int64, metricName string,
) ([]hostMetricRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id     = @teamID
		         AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		         AND metric_name = @metricName
		)
		SELECT
		    host,
		    avg(val_sum / nullIf(val_count, 0))         AS value
		FROM observability.metrics_1m
		PREWHERE team_id     = @teamID
		     AND ts_bucket   BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint IN active_fps
		WHERE metric_name = @metricName
		  AND timestamp BETWEEN @start AND @end
		  AND host != ''
		GROUP BY host`
	args := append(spanArgs(teamID, startMs, endMs),
		clickhouse.Named("metricName", metricName),
	)
	var rows []hostMetricRow
	return rows, dbutil.SelectCH(dbutil.DashboardCtx(ctx), r.db, "hosts.QueryHostMetric",
		&rows, query, args...)
}

// CPUMetricName returns the OTel metric used for per-host CPU utilization.
func CPUMetricName() string { return infraconsts.MetricSystemCPUUtilization }

// MemoryMetricName returns the OTel metric used for per-host memory utilization.
func MemoryMetricName() string { return infraconsts.MetricSystemMemoryUtilization }

func spanArgs(teamID int64, startMs, endMs int64) []any {
	bucketStart, bucketEnd := spanBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — TeamID fits UInt32
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

func spanBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}
