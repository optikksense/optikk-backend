package summary

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/filter"
)

// Repository runs the four sub-queries that feed the top-level db summary
// panel. Service.go orchestrates parallel execution and assembles
// SummaryStats from the raw rows.
type Repository interface {
	GetMainStats(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) (mainRawRow, error)
	GetActiveConnections(ctx context.Context, teamID, startMs, endMs int64) (int64, error)
	GetCacheStats(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) (cacheRawRow, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

type mainRawRow struct {
	TotalCount uint64   `ch:"total_count"`
	ErrorCount uint64   `ch:"error_count"`
	AvgMs      float64  `ch:"avg_ms"`
	Buckets    []uint64 `ch:"bucket_counts"`
}

type cacheRawRow struct {
	TotalCount   uint64 `ch:"total_count"`
	SuccessCount uint64 `ch:"success_count"`
}

// GetMainStats returns the latency histogram + aggregate counts over all
// db spans in the window. Service interpolates p95/p99.
func (r *ClickHouseRepository) GetMainStats(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) (mainRawRow, error) {
	filterWhere, filterArgs := filter.BuildSpanClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toUInt64(count())                                                              AS total_count,
		       countIf(has_error OR toUInt16OrZero(response_status_code) >= 400)              AS error_count,
		       toFloat64(avg(duration_nano / 1000000.0))                                      AS avg_ms,
		       ` + filter.LatencyBucketCountsSQL() + `                                        AS bucket_counts
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system != ''` + filterWhere

	args := append(filter.SpanArgs(teamID, startMs, endMs), filterArgs...)
	var row mainRawRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "summary.GetMainStats", &row, query, args...)
}

// GetActiveConnections sums the avg(value) of `db.client.connection.count`
// where state='used' across all DB systems in the window. Returns 0 if
// no metric is reporting (driver instrumentation absent).
func (r *ClickHouseRepository) GetActiveConnections(ctx context.Context, teamID, startMs, endMs int64) (int64, error) {
	const query = `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.metrics_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND metric_name = @metricName
		)
		SELECT toFloat64(avg(value)) AS avg_used
		FROM observability.metrics
		PREWHERE team_id        = @teamID
		     AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		     AND fingerprint    IN active_fps
		     AND metric_name    = @metricName
		WHERE timestamp BETWEEN @start AND @end
		  AND attributes.'db.client.connection.state'::String = 'used'`

	args := filter.MetricArgs(teamID, startMs, endMs, filter.MetricDBConnectionCount)
	var row struct {
		AvgUsed float64 `ch:"avg_used"`
	}
	if err := dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "summary.GetActiveConnections", &row, query, args...); err != nil {
		return 0, nil //nolint:nilerr // best-effort: missing instrumentation shouldn't fail the panel
	}
	return int64(row.AvgUsed + 0.5), nil
}

// GetCacheStats returns total / success counts for spans on db_system='redis'
// (or other cache backends in the future). Service computes hit-rate.
func (r *ClickHouseRepository) GetCacheStats(ctx context.Context, teamID, startMs, endMs int64, f filter.Filters) (cacheRawRow, error) {
	filterWhere, filterArgs := filter.BuildSpanClauses(f)
	query := `
		WITH active_fps AS (
		    SELECT fingerprint
		    FROM observability.spans_resource
		    PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd
		)
		SELECT toUInt64(count())                                                                       AS total_count,
		       countIf(NOT has_error AND toUInt16OrZero(response_status_code) < 400)                   AS success_count
		FROM observability.spans
		PREWHERE team_id = @teamID AND ts_bucket BETWEEN @bucketStart AND @bucketEnd AND fingerprint IN active_fps
		WHERE timestamp BETWEEN @start AND @end
		  AND db_system = 'redis'` + filterWhere

	args := append(filter.SpanArgs(teamID, startMs, endMs), filterArgs...)
	var row cacheRawRow
	return row, dbutil.QueryRowCH(dbutil.OverviewCtx(ctx), r.db, "summary.GetCacheStats", &row, query, args...)
}
