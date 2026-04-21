package system

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	"github.com/Optikk-Org/optikk-backend/internal/infra/rollup"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/saturation/database/internal/shared"
)

type Repository interface {
	GetSystemLatency(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string, f shared.Filters) ([]LatencyTimeSeries, error)
	GetSystemOps(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string, f shared.Filters) ([]OpsTimeSeries, error)
	GetSystemTopCollectionsByLatency(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error)
	GetSystemTopCollectionsByVolume(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error)
	GetSystemErrors(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]ErrorTimeSeries, error)
	GetSystemNamespaces(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// systemFilter augments the shared rollup filters with a concrete db_system pin.
func systemFilter(dbSystem string, f shared.Filters) (string, []any) {
	frag, args := shared.RollupFilterClauses(f)
	frag += ` AND db_system = @dbSystem`
	args = append(args, clickhouse.Named("dbSystem", dbSystem))
	return frag, args
}

func (r *ClickHouseRepository) GetSystemLatency(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string, f shared.Filters) ([]LatencyTimeSeries, error) {
	table, tierStep := rollup.TierTableFor(shared.DBHistRollupPrefix, startMs, endMs)
	fc, fargs := systemFilter(dbSystem, f)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                          AS time_bucket,
		    db_operation                                                                AS group_by,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[1]) * 1000  AS p50_ms,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[2]) * 1000  AS p95_ms,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3]) * 1000  AS p99_ms
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`, shared.BucketTimeExpr, table, fc)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)
	args = append(args, fargs...)
	var rows []LatencyTimeSeries
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSystemOps(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string, f shared.Filters) ([]OpsTimeSeries, error) {
	table, tierStep := rollup.TierTableFor(shared.DBHistRollupPrefix, startMs, endMs)
	fc, fargs := systemFilter(dbSystem, f)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                   AS time_bucket,
		    db_operation                         AS group_by,
		    toFloat64(sumMerge(hist_count)) / %f AS ops_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`, shared.BucketTimeExpr, bucketSec, table, fc)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)
	args = append(args, fargs...)
	var rows []OpsTimeSeries
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSystemTopCollectionsByLatency(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	table, tierStep := rollup.TierTableFor(shared.DBHistRollupPrefix, startMs, endMs)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    db_collection                                                               AS collection_name,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3]) * 1000  AS p99_ms,
		    toFloat64(sumMerge(hist_count)) / %f                                        AS ops_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND db_system = @dbSystem
		  AND notEmpty(db_collection)
		GROUP BY collection_name
		ORDER BY p99_ms DESC
		LIMIT 20
	`, bucketSec, table)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
		clickhouse.Named("dbSystem", dbSystem),
	)
	var rows []SystemCollectionRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSystemTopCollectionsByVolume(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	table, tierStep := rollup.TierTableFor(shared.DBHistRollupPrefix, startMs, endMs)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    db_collection                                                               AS collection_name,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3]) * 1000  AS p99_ms,
		    toFloat64(sumMerge(hist_count)) / %f                                        AS ops_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND db_system = @dbSystem
		  AND notEmpty(db_collection)
		GROUP BY collection_name
		ORDER BY ops_per_sec DESC
		LIMIT 20
	`, bucketSec, table)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
		clickhouse.Named("dbSystem", dbSystem),
	)
	var rows []SystemCollectionRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSystemErrors(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]ErrorTimeSeries, error) {
	table, tierStep := rollup.TierTableFor(shared.DBHistRollupPrefix, startMs, endMs)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                   AS time_bucket,
		    db_operation                         AS group_by,
		    toFloat64(sumMerge(hist_count)) / %f AS errors_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND db_system = @dbSystem
		  AND notEmpty(error_type)
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`, shared.BucketTimeExpr, bucketSec, table)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
		clickhouse.Named("dbSystem", dbSystem),
	)
	var rows []ErrorTimeSeries
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSystemNamespaces(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error) {
	table, tierStep := rollup.TierTableFor(shared.DBHistRollupPrefix, startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    db_namespace             AS namespace,
		    toInt64(sumMerge(hist_count)) AS span_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND metric_name = @metricName
		  AND db_system = @dbSystem
		  AND notEmpty(db_namespace)
		GROUP BY namespace
		ORDER BY span_count DESC
	`, table)

	args := append(shared.RollupBaseParams(teamID, startMs, endMs, shared.MetricDBOperationDuration),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
		clickhouse.Named("dbSystem", dbSystem),
	)
	var rows []SystemNamespace
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}
