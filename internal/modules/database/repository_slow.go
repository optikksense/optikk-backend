package database

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

func (r *ClickHouseRepository) GetSlowQueryPatterns(teamID int64, startMs, endMs int64, f Filters, limit int) ([]SlowQueryPattern, error) {
	if limit <= 0 {
		limit = 10
	}
	fc, fargs := filterClauses(f)
	queryAttr := attrString(AttrDBQueryText)
	collAttr := attrString(AttrDBCollectionName)
	errorAttr := attrString(AttrErrorType)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS query_text,
		    %s                                                                              AS collection_name,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p50_ms,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p95_ms,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms,
		    toInt64(sum(hist_count))                                                        AS call_count,
		    toInt64(sumIf(hist_count, notEmpty(%s)))                                        AS error_count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  %s
		GROUP BY query_text, collection_name
		ORDER BY p99_ms DESC
		LIMIT %d
	`,
		queryAttr, collAttr, errorAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		fc, limit,
	)

	args := buildArgs([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, fargs)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	out := make([]SlowQueryPattern, len(rows))
	for i, row := range rows {
		out[i] = SlowQueryPattern{
			QueryText:      dbutil.StringFromAny(row["query_text"]),
			CollectionName: dbutil.StringFromAny(row["collection_name"]),
			P50Ms:          dbutil.NullableFloat64FromAny(row["p50_ms"]),
			P95Ms:          dbutil.NullableFloat64FromAny(row["p95_ms"]),
			P99Ms:          dbutil.NullableFloat64FromAny(row["p99_ms"]),
			CallCount:      dbutil.Int64FromAny(row["call_count"]),
			ErrorCount:     dbutil.Int64FromAny(row["error_count"]),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetSlowestCollections(teamID int64, startMs, endMs int64, f Filters) ([]SlowCollectionRow, error) {
	fc, fargs := filterClauses(f)
	collAttr := attrString(AttrDBCollectionName)
	errorAttr := attrString(AttrErrorType)
	bucketSec := bucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS collection_name,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms,
		    toFloat64(sum(hist_count)) / %f                                                 AS ops_per_sec,
		    toFloat64(sumIf(hist_count, notEmpty(%s))) / nullIf(toFloat64(sum(hist_count)), 0) * 100 AS error_rate
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  %s
		GROUP BY collection_name
		ORDER BY p99_ms DESC
		LIMIT 50
	`,
		collAttr,
		bucketSec,
		errorAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		collAttr,
		fc,
	)

	args := buildArgs([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, fargs)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	out := make([]SlowCollectionRow, len(rows))
	for i, row := range rows {
		out[i] = SlowCollectionRow{
			CollectionName: dbutil.StringFromAny(row["collection_name"]),
			P99Ms:          dbutil.NullableFloat64FromAny(row["p99_ms"]),
			OpsPerSec:      dbutil.NullableFloat64FromAny(row["ops_per_sec"]),
			ErrorRate:      dbutil.NullableFloat64FromAny(row["error_rate"]),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetSlowQueryRate(teamID int64, startMs, endMs int64, f Filters, thresholdMs float64) ([]SlowRatePoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := filterClauses(f)
	bucketSec := bucketWidthSeconds(startMs, endMs)
	thresholdSec := thresholdMs / 1000.0

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                                    AS time_bucket,
		    toFloat64(sumIf(hist_count, (hist_sum / nullIf(hist_count, 0)) > %f)) / %f           AS slow_per_sec
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  %s
		GROUP BY time_bucket
		ORDER BY time_bucket
	`,
		bucket,
		thresholdSec, bucketSec,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		fc,
	)

	args := buildArgs([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, fargs)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	out := make([]SlowRatePoint, len(rows))
	for i, row := range rows {
		out[i] = SlowRatePoint{
			TimeBucket: dbutil.StringFromAny(row["time_bucket"]),
			SlowPerSec: dbutil.NullableFloat64FromAny(row["slow_per_sec"]),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetP99ByQueryText(teamID int64, startMs, endMs int64, f Filters, limit int) ([]P99ByQueryText, error) {
	if limit <= 0 {
		limit = 10
	}
	fc, fargs := filterClauses(f)
	queryAttr := attrString(AttrDBQueryText)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS query_text,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  %s
		GROUP BY query_text
		ORDER BY p99_ms DESC
		LIMIT %d
	`,
		queryAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		queryAttr,
		fc, limit,
	)

	args := buildArgs([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, fargs)
	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	out := make([]P99ByQueryText, len(rows))
	for i, row := range rows {
		out[i] = P99ByQueryText{
			QueryText: dbutil.StringFromAny(row["query_text"]),
			P99Ms:     dbutil.NullableFloat64FromAny(row["p99_ms"]),
		}
	}
	return out, nil
}
