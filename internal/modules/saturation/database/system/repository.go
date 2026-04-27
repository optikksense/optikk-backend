package system

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
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
	table := "observability.signoz_index_v3"
	tierStep := int64(1)
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
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`, shared.BucketTimeExpr, table, fc)

	args := append(shared.BaseParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)
	args = append(args, fargs...)
	var rows []LatencyTimeSeries
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "system.GetSystemLatency", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSystemOps(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string, f shared.Filters) ([]OpsTimeSeries, error) {
	table := "observability.signoz_index_v3"
	tierStep := int64(1)
	fc, fargs := systemFilter(dbSystem, f)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                   AS time_bucket,
		    db_operation                         AS group_by,
		    toFloat64(sum(hist_count)) / %f AS ops_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`, shared.BucketTimeExpr, bucketSec, table, fc)

	args := append(shared.BaseParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
	)
	args = append(args, fargs...)
	var rows []OpsTimeSeries
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "system.GetSystemOps", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSystemTopCollectionsByLatency(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	table := "observability.metrics"
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    db_collection                                                               AS collection_name,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3]) * 1000  AS p99_ms,
		    toFloat64(sum(hist_count)) / %f                                        AS ops_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND db_system = @dbSystem
		  AND notEmpty(db_collection)
		GROUP BY collection_name
		ORDER BY p99_ms DESC
		LIMIT 20
	`, bucketSec, table)

	args := append(shared.BaseParams(teamID, startMs, endMs),
		clickhouse.Named("dbSystem", dbSystem),
	)
	var rows []SystemCollectionRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "system.GetSystemTopCollectionsByLatency", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSystemTopCollectionsByVolume(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	table := "observability.metrics"
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    db_collection                                                               AS collection_name,
		    toFloat64(quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest)[3]) * 1000  AS p99_ms,
		    toFloat64(sum(hist_count)) / %f                                        AS ops_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND db_system = @dbSystem
		  AND notEmpty(db_collection)
		GROUP BY collection_name
		ORDER BY ops_per_sec DESC
		LIMIT 20
	`, bucketSec, table)

	args := append(shared.BaseParams(teamID, startMs, endMs),
		clickhouse.Named("dbSystem", dbSystem),
	)
	var rows []SystemCollectionRow
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "system.GetSystemTopCollectionsByVolume", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSystemErrors(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]ErrorTimeSeries, error) {
	table := "observability.signoz_index_v3"
	tierStep := int64(1)
	bucketSec := shared.BucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                   AS time_bucket,
		    db_operation                         AS group_by,
		    toFloat64(sum(hist_count)) / %f AS errors_per_sec
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND db_system = @dbSystem
		  AND notEmpty(error_type)
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`, shared.BucketTimeExpr, bucketSec, table)

	args := append(shared.BaseParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", shared.QueryIntervalMinutes(tierStep, startMs, endMs)),
		clickhouse.Named("dbSystem", dbSystem),
	)
	var rows []ErrorTimeSeries
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "system.GetSystemErrors", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

func (r *ClickHouseRepository) GetSystemNamespaces(ctx context.Context, teamID int64, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error) {
	table := "observability.metrics"

	query := fmt.Sprintf(`
		SELECT
		    db_namespace             AS namespace,
		    toInt64(sum(hist_count)) AS span_count
		FROM %s
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end
		  AND db_system = @dbSystem
		  AND notEmpty(db_namespace)
		GROUP BY namespace
		ORDER BY span_count DESC
	`, table)

	args := append(shared.BaseParams(teamID, startMs, endMs),
		clickhouse.Named("dbSystem", dbSystem),
	)
	var rows []SystemNamespace
	if err := dbutil.SelectCH(dbutil.OverviewCtx(ctx), r.db, "system.GetSystemNamespaces", &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}
