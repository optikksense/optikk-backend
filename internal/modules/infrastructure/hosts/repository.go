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
	QueryHostUtilization(ctx context.Context, teamID, startMs, endMs int64) ([]hostMetricRow, error)
	QueryHostSpans(ctx context.Context, teamID, startMs, endMs int64, serviceName string) ([]hostSpansRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) Repository {
	return &ClickHouseRepository{db: db}
}

// QueryHostUtilization returns one row per (host, metric) for the CPU, memory and
// disk utilization families across the window. Follows the shared active_fps CTE
// pattern used by the infra metric repositories.
func (r *ClickHouseRepository) QueryHostUtilization(ctx context.Context, teamID, startMs, endMs int64) ([]hostMetricRow, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    WHERE team_id = @teamID
		      AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		      AND metric_name IN @metricNames
		)
		SELECT
		    host                           AS host,
		    metric_name                    AS metric_name,
		    sum(val_sum) / sum(val_count)  AS value
		FROM observability.metrics_1m
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint   IN active_fps
		WHERE metric_name IN @metricNames
		  AND timestamp BETWEEN @start AND @end
		  AND host != ''
		GROUP BY host, metric_name`

	bucketStart, bucketEnd := metricBucketBounds(startMs, endMs)
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
		clickhouse.Named("metricNames", utilizationMetricNames()),
	}
	var rows []hostMetricRow
	return rows, dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "hosts.QueryHostUtilization", &rows, query, args...)
}

// QueryHostSpans returns per-host RED aggregates from spans_1m restricted to
// the requested service.
func (r *ClickHouseRepository) QueryHostSpans(
	ctx context.Context, teamID, startMs, endMs int64, serviceName string,
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

func metricBucketBounds(startMs, endMs int64) (uint32, uint32) {
	return timebucket.BucketStart(startMs / 1000),
		timebucket.BucketStart(endMs/1000) + uint32(timebucket.BucketSeconds)
}

func spanArgs(teamID, startMs, endMs int64) []any {
	bucketStart, bucketEnd := metricBucketBounds(startMs, endMs)
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — TeamID fits UInt32
		clickhouse.Named("bucketStart", bucketStart),
		clickhouse.Named("bucketEnd", bucketEnd),
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// utilizationMetricNames is the CPU + memory + disk metric families in one slice.
func utilizationMetricNames() []string {
	names := make([]string, 0, len(infraconsts.CPUMetrics)+len(infraconsts.MemoryMetrics)+len(infraconsts.DiskMetrics))
	names = append(names, infraconsts.CPUMetrics...)
	names = append(names, infraconsts.MemoryMetrics...)
	names = append(names, infraconsts.DiskMetrics...)
	return names
}
