package database

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// errorSeriesByAttr is the shared helper for all "errors by X over time" queries.
func (r *ClickHouseRepository) errorSeriesByAttr(
	teamID int64, startMs, endMs int64,
	groupAttr string,
	f Filters,
) ([]ErrorTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := filterClauses(f)
	groupExpr := attrString(groupAttr)
	errorAttr := attrString(AttrErrorType)
	bucketSec := bucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                        AS time_bucket,
		    %s                                                        AS group_by,
		    toFloat64(sum(hist_count)) / %f                          AS errors_per_sec
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, groupExpr,
		bucketSec,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		errorAttr,
		fc,
	)

	args := buildArgs([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, fargs)
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

func (r *ClickHouseRepository) GetErrorsBySystem(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error) {
	return r.errorSeriesByAttr(teamID, startMs, endMs, AttrDBSystem, f)
}

func (r *ClickHouseRepository) GetErrorsByOperation(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error) {
	return r.errorSeriesByAttr(teamID, startMs, endMs, AttrDBOperationName, f)
}

func (r *ClickHouseRepository) GetErrorsByErrorType(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error) {
	return r.errorSeriesByAttr(teamID, startMs, endMs, AttrErrorType, f)
}

func (r *ClickHouseRepository) GetErrorsByCollection(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error) {
	return r.errorSeriesByAttr(teamID, startMs, endMs, AttrDBCollectionName, f)
}

func (r *ClickHouseRepository) GetErrorsByResponseStatus(teamID int64, startMs, endMs int64, f Filters) ([]ErrorTimeSeries, error) {
	return r.errorSeriesByAttr(teamID, startMs, endMs, AttrDBResponseStatus, f)
}

func (r *ClickHouseRepository) GetErrorRatio(teamID int64, startMs, endMs int64, f Filters) ([]ErrorRatioPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := filterClauses(f)
	errorAttr := attrString(AttrErrorType)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS time_bucket,
		    toFloat64(sumIf(hist_count, notEmpty(%s))) / nullIf(toFloat64(sum(hist_count)), 0) * 100 AS error_ratio_pct
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
		errorAttr,
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

	out := make([]ErrorRatioPoint, len(rows))
	for i, row := range rows {
		out[i] = ErrorRatioPoint{
			TimeBucket:    dbutil.StringFromAny(row["time_bucket"]),
			ErrorRatioPct: dbutil.NullableFloat64FromAny(row["error_ratio_pct"]),
		}
	}
	return out, nil
}
