package database

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

func (r *ClickHouseRepository) GetCollectionLatency(teamID int64, startMs, endMs int64, collection string, f Filters) ([]LatencyTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := filterClauses(f)
	collAttr := attrString(AttrDBCollectionName)
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
		collAttr,
		fc,
	)

	args := buildArgs([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), collection}, fargs)
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

func (r *ClickHouseRepository) GetCollectionOps(teamID int64, startMs, endMs int64, collection string, f Filters) ([]OpsTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := filterClauses(f)
	collAttr := attrString(AttrDBCollectionName)
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
		collAttr,
		fc,
	)

	args := buildArgs([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), collection}, fargs)
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

func (r *ClickHouseRepository) GetCollectionErrors(teamID int64, startMs, endMs int64, collection string, f Filters) ([]ErrorTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := filterClauses(f)
	collAttr := attrString(AttrDBCollectionName)
	errorAttr := attrString(AttrErrorType)
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
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, errorAttr,
		bucketSec,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		collAttr,
		errorAttr,
		fc,
	)

	args := buildArgs([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), collection}, fargs)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
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

func (r *ClickHouseRepository) GetCollectionQueryTexts(teamID int64, startMs, endMs int64, collection string, f Filters, limit int) ([]CollectionTopQuery, error) {
	if limit <= 0 {
		limit = 10
	}
	fc, fargs := filterClauses(f)
	collAttr := attrString(AttrDBCollectionName)
	queryAttr := attrString(AttrDBQueryText)
	errorAttr := attrString(AttrErrorType)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS query_text,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms,
		    toInt64(sum(hist_count))                                                        AS call_count,
		    toInt64(sumIf(hist_count, notEmpty(%s)))                                        AS error_count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = ?
		  AND notEmpty(%s)
		  %s
		GROUP BY query_text
		ORDER BY p99_ms DESC
		LIMIT %d
	`,
		queryAttr,
		errorAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		collAttr,
		queryAttr,
		fc, limit,
	)

	args := buildArgs([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), collection}, fargs)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	out := make([]CollectionTopQuery, len(rows))
	for i, row := range rows {
		out[i] = CollectionTopQuery{
			QueryText:  dbutil.StringFromAny(row["query_text"]),
			P99Ms:      dbutil.NullableFloat64FromAny(row["p99_ms"]),
			CallCount:  dbutil.Int64FromAny(row["call_count"]),
			ErrorCount: dbutil.Int64FromAny(row["error_count"]),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetCollectionReadVsWrite(teamID int64, startMs, endMs int64, collection string) ([]ReadWritePoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	collAttr := attrString(AttrDBCollectionName)
	opAttr := attrString(AttrDBOperationName)
	bucketSec := bucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                                             AS time_bucket,
		    toFloat64(sumIf(hist_count, upper(%s) IN ('SELECT', 'FIND', 'GET'))) / %f                     AS read_ops_per_sec,
		    toFloat64(sumIf(hist_count, upper(%s) IN ('INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'UPSERT', 'SET', 'PUT', 'AGGREGATE'))) / %f  AS write_ops_per_sec
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND %s = ?
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		opAttr, bucketSec,
		opAttr, bucketSec,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		collAttr,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), collection)
	if err != nil {
		return nil, err
	}

	out := make([]ReadWritePoint, len(rows))
	for i, row := range rows {
		out[i] = ReadWritePoint{
			TimeBucket:     dbutil.StringFromAny(row["time_bucket"]),
			ReadOpsPerSec:  dbutil.NullableFloat64FromAny(row["read_ops_per_sec"]),
			WriteOpsPerSec: dbutil.NullableFloat64FromAny(row["write_ops_per_sec"]),
		}
	}
	return out, nil
}
