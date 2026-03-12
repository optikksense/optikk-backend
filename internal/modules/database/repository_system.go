package database

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

func (r *ClickHouseRepository) GetSystemLatency(teamID int64, startMs, endMs int64, dbSystem string, f Filters) ([]LatencyTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := filterClauses(f)
	systemAttr := attrString(AttrDBSystem)
	opAttr := attrString(AttrDBOperationName)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS time_bucket,
		    %s                                                                              AS group_by,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p50_ms,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p95_ms,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = ?
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, opAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		systemAttr,
		fc,
	)

	args := buildArgs([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbSystem}, fargs)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	out := make([]LatencyTimeSeries, len(rows))
	for i, row := range rows {
		out[i] = LatencyTimeSeries{
			TimeBucket: dbutil.StringFromAny(row["time_bucket"]),
			GroupBy:    dbutil.StringFromAny(row["group_by"]),
			P50Ms:      dbutil.NullableFloat64FromAny(row["p50_ms"]),
			P95Ms:      dbutil.NullableFloat64FromAny(row["p95_ms"]),
			P99Ms:      dbutil.NullableFloat64FromAny(row["p99_ms"]),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetSystemOps(teamID int64, startMs, endMs int64, dbSystem string, f Filters) ([]OpsTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := filterClauses(f)
	systemAttr := attrString(AttrDBSystem)
	opAttr := attrString(AttrDBOperationName)
	bucketSec := bucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                               AS time_bucket,
		    %s                               AS group_by,
		    toFloat64(sum(hist_count)) / %f  AS ops_per_sec
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = ?
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, opAttr,
		bucketSec,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		systemAttr,
		fc,
	)

	args := buildArgs([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbSystem}, fargs)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	out := make([]OpsTimeSeries, len(rows))
	for i, row := range rows {
		out[i] = OpsTimeSeries{
			TimeBucket: dbutil.StringFromAny(row["time_bucket"]),
			GroupBy:    dbutil.StringFromAny(row["group_by"]),
			OpsPerSec:  dbutil.NullableFloat64FromAny(row["ops_per_sec"]),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetSystemTopCollectionsByLatency(teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	collAttr := attrString(AttrDBCollectionName)
	systemAttr := attrString(AttrDBSystem)
	bucketSec := bucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS collection_name,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms,
		    toFloat64(sum(hist_count)) / %f                                                 AS ops_per_sec
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = ?
		  AND notEmpty(%s)
		GROUP BY collection_name
		ORDER BY p99_ms DESC
		LIMIT 20
	`,
		collAttr,
		bucketSec,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		systemAttr,
		collAttr,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbSystem)
	if err != nil {
		return nil, err
	}

	out := make([]SystemCollectionRow, len(rows))
	for i, row := range rows {
		out[i] = SystemCollectionRow{
			CollectionName: dbutil.StringFromAny(row["collection_name"]),
			P99Ms:          dbutil.NullableFloat64FromAny(row["p99_ms"]),
			OpsPerSec:      dbutil.NullableFloat64FromAny(row["ops_per_sec"]),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetSystemTopCollectionsByVolume(teamID int64, startMs, endMs int64, dbSystem string) ([]SystemCollectionRow, error) {
	collAttr := attrString(AttrDBCollectionName)
	systemAttr := attrString(AttrDBSystem)
	bucketSec := bucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS collection_name,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms,
		    toFloat64(sum(hist_count)) / %f                                                 AS ops_per_sec
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = ?
		  AND notEmpty(%s)
		GROUP BY collection_name
		ORDER BY ops_per_sec DESC
		LIMIT 20
	`,
		collAttr,
		bucketSec,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		systemAttr,
		collAttr,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbSystem)
	if err != nil {
		return nil, err
	}

	out := make([]SystemCollectionRow, len(rows))
	for i, row := range rows {
		out[i] = SystemCollectionRow{
			CollectionName: dbutil.StringFromAny(row["collection_name"]),
			P99Ms:          dbutil.NullableFloat64FromAny(row["p99_ms"]),
			OpsPerSec:      dbutil.NullableFloat64FromAny(row["ops_per_sec"]),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetSystemErrors(teamID int64, startMs, endMs int64, dbSystem string) ([]ErrorTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	systemAttr := attrString(AttrDBSystem)
	errorAttr := attrString(AttrErrorType)
	opAttr := attrString(AttrDBOperationName)
	bucketSec := bucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                               AS time_bucket,
		    %s                               AS group_by,
		    toFloat64(sum(hist_count)) / %f  AS errors_per_sec
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = ?
		  AND notEmpty(%s)
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, opAttr,
		bucketSec,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		systemAttr,
		errorAttr,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbSystem)
	if err != nil {
		return nil, err
	}

	out := make([]ErrorTimeSeries, len(rows))
	for i, row := range rows {
		out[i] = ErrorTimeSeries{
			TimeBucket:   dbutil.StringFromAny(row["time_bucket"]),
			GroupBy:      dbutil.StringFromAny(row["group_by"]),
			ErrorsPerSec: dbutil.NullableFloat64FromAny(row["errors_per_sec"]),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetSystemNamespaces(teamID int64, startMs, endMs int64, dbSystem string) ([]SystemNamespace, error) {
	systemAttr := attrString(AttrDBSystem)
	nsAttr := attrString(AttrDBNamespace)

	query := fmt.Sprintf(`
		SELECT
		    %s                               AS namespace,
		    toInt64(sum(hist_count))         AS span_count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = ?
		  AND notEmpty(%s)
		GROUP BY namespace
		ORDER BY span_count DESC
	`,
		nsAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		systemAttr,
		nsAttr,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), dbSystem)
	if err != nil {
		return nil, err
	}

	out := make([]SystemNamespace, len(rows))
	for i, row := range rows {
		out[i] = SystemNamespace{
			Namespace: dbutil.StringFromAny(row["namespace"]),
			SpanCount: dbutil.Int64FromAny(row["span_count"]),
		}
	}
	return out, nil
}
