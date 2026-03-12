package database

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// opsSeriesByAttr is the shared helper for all "ops/sec by X over time" queries.
func (r *ClickHouseRepository) opsSeriesByAttr(
	teamID int64, startMs, endMs int64,
	groupAttr string,
	f Filters,
) ([]OpsTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := filterClauses(f)
	groupExpr := attrString(groupAttr)

	// Calculate interval in seconds for rate computation.
	intervalSec := timebucket.NewAdaptiveStrategy(startMs, endMs)
	_ = intervalSec // we use count() / interval below via window duration

	// We compute rate as total_count / window_seconds per bucket.
	// For true rate, we count within each bucket and divide by its width.
	query := fmt.Sprintf(`
		SELECT
		    %s                        AS time_bucket,
		    %s                        AS group_by,
		    toInt64(sum(hist_count))  AS op_count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  %s
		GROUP BY time_bucket, group_by
		ORDER BY time_bucket, group_by
	`,
		bucket, groupExpr,
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

	// Bucket width in seconds (approximate from overall window).
	bucketSec := bucketWidthSeconds(startMs, endMs)

	out := make([]OpsTimeSeries, len(rows))
	for i, row := range rows {
		opCount := dbutil.Float64FromAny(row["op_count"])
		rate := opCount / bucketSec
		out[i] = OpsTimeSeries{
			TimeBucket: dbutil.StringFromAny(row["time_bucket"]),
			GroupBy:    dbutil.StringFromAny(row["group_by"]),
			OpsPerSec:  &rate,
		}
	}
	return out, nil
}

// bucketWidthSeconds returns the approximate width of each time bucket in seconds.
func bucketWidthSeconds(startMs, endMs int64) float64 {
	hours := float64(endMs-startMs) / 3_600_000.0
	switch {
	case hours <= 3:
		return 60 // 1 minute
	case hours <= 24:
		return 300 // 5 minutes
	case hours <= 168:
		return 3600 // 1 hour
	default:
		return 86400 // 1 day
	}
}

func (r *ClickHouseRepository) GetOpsBySystem(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error) {
	return r.opsSeriesByAttr(teamID, startMs, endMs, AttrDBSystem, f)
}

func (r *ClickHouseRepository) GetOpsByOperation(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error) {
	return r.opsSeriesByAttr(teamID, startMs, endMs, AttrDBOperationName, f)
}

func (r *ClickHouseRepository) GetOpsByCollection(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error) {
	return r.opsSeriesByAttr(teamID, startMs, endMs, AttrDBCollectionName, f)
}

func (r *ClickHouseRepository) GetOpsByNamespace(teamID int64, startMs, endMs int64, f Filters) ([]OpsTimeSeries, error) {
	return r.opsSeriesByAttr(teamID, startMs, endMs, AttrDBNamespace, f)
}

func (r *ClickHouseRepository) GetReadVsWrite(teamID int64, startMs, endMs int64, f Filters) ([]ReadWritePoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := filterClauses(f)
	opAttr := attrString(AttrDBOperationName)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                            AS time_bucket,
		    sumIf(hist_count, upper(%s) IN ('SELECT', 'FIND', 'GET'))                    AS read_count,
		    sumIf(hist_count, upper(%s) IN ('INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'UPSERT', 'SET', 'PUT', 'AGGREGATE')) AS write_count
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
		opAttr, opAttr,
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

	bucketSec := bucketWidthSeconds(startMs, endMs)
	out := make([]ReadWritePoint, len(rows))
	for i, row := range rows {
		readRate := dbutil.Float64FromAny(row["read_count"]) / bucketSec
		writeRate := dbutil.Float64FromAny(row["write_count"]) / bucketSec
		out[i] = ReadWritePoint{
			TimeBucket:     dbutil.StringFromAny(row["time_bucket"]),
			ReadOpsPerSec:  &readRate,
			WriteOpsPerSec: &writeRate,
		}
	}
	return out, nil
}
