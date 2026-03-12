package database

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// -----------------------------------------------------------------------
// Section 7 — Connection Pool Utilization
// -----------------------------------------------------------------------

func (r *ClickHouseRepository) GetConnectionCountSeries(teamID int64, startMs, endMs int64) ([]ConnectionCountPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	poolAttr := attrString(AttrPoolName)
	stateAttr := attrString(AttrConnectionState)

	query := fmt.Sprintf(`
		SELECT
		    %s               AS time_bucket,
		    %s               AS pool_name,
		    %s               AS state,
		    avg(value)       AS count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY time_bucket, pool_name, state
		ORDER BY time_bucket, pool_name, state
	`,
		bucket, poolAttr, stateAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBConnectionCount,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	out := make([]ConnectionCountPoint, len(rows))
	for i, row := range rows {
		out[i] = ConnectionCountPoint{
			TimeBucket: dbutil.StringFromAny(row["time_bucket"]),
			PoolName:   dbutil.StringFromAny(row["pool_name"]),
			State:      dbutil.StringFromAny(row["state"]),
			Count:      dbutil.NullableFloat64FromAny(row["count"]),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetConnectionUtilization(teamID int64, startMs, endMs int64) ([]ConnectionUtilPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	poolAttr := attrString(AttrPoolName)
	stateAttr := attrString(AttrConnectionState)

	// Utilisation = used / max * 100.  We join the two metrics within the time bucket.
	query := fmt.Sprintf(`
		SELECT
		    %s                                                           AS time_bucket,
		    %s                                                           AS pool_name,
		    avgIf(value, %s = 'used')                                   AS used_avg,
		    (
		        SELECT avg(value)
		        FROM %s AS mx
		        WHERE mx.%s = m.%s
		          AND mx.%s BETWEEN ? AND ?
		          AND mx.%s = '%s'
		          AND mx.%s = %s
		    )                                                             AS max_val,
		    if(max_val > 0, used_avg / max_val * 100, NULL)             AS util_pct
		FROM %s AS m
		WHERE m.%s = ?
		  AND m.%s BETWEEN ? AND ?
		  AND m.%s = '%s'
		GROUP BY time_bucket, pool_name
		ORDER BY time_bucket, pool_name
	`,
		bucket, poolAttr, stateAttr,
		// sub-select for max_val
		TableMetrics,
		ColTeamID, ColTeamID,
		ColTimestamp,
		ColMetricName, MetricDBConnectionMax,
		poolAttr, poolAttr,
		// outer table
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBConnectionCount,
	)

	rows, err := dbutil.QueryMaps(r.db, query,
		dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), // sub-select timestamps
		uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs), // outer
	)
	if err != nil {
		return nil, err
	}

	out := make([]ConnectionUtilPoint, len(rows))
	for i, row := range rows {
		out[i] = ConnectionUtilPoint{
			TimeBucket: dbutil.StringFromAny(row["time_bucket"]),
			PoolName:   dbutil.StringFromAny(row["pool_name"]),
			UtilPct:    dbutil.NullableFloat64FromAny(row["util_pct"]),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetConnectionLimits(teamID int64, startMs, endMs int64) ([]ConnectionLimits, error) {
	poolAttr := attrString(AttrPoolName)

	query := fmt.Sprintf(`
		SELECT
		    %s                                               AS pool_name,
		    avgIf(value, metric_name = '%s')                AS max_val,
		    avgIf(value, metric_name = '%s')                AS idle_max,
		    avgIf(value, metric_name = '%s')                AS idle_min
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s IN ('%s', '%s', '%s')
		GROUP BY pool_name
		ORDER BY pool_name
	`,
		poolAttr,
		MetricDBConnectionMax, MetricDBConnectionIdleMax, MetricDBConnectionIdleMin,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName,
		MetricDBConnectionMax, MetricDBConnectionIdleMax, MetricDBConnectionIdleMin,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	out := make([]ConnectionLimits, len(rows))
	for i, row := range rows {
		out[i] = ConnectionLimits{
			PoolName: dbutil.StringFromAny(row["pool_name"]),
			Max:      dbutil.NullableFloat64FromAny(row["max_val"]),
			IdleMax:  dbutil.NullableFloat64FromAny(row["idle_max"]),
			IdleMin:  dbutil.NullableFloat64FromAny(row["idle_min"]),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetPendingRequests(teamID int64, startMs, endMs int64) ([]PendingRequestsPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	poolAttr := attrString(AttrPoolName)

	query := fmt.Sprintf(`
		SELECT
		    %s             AS time_bucket,
		    %s             AS pool_name,
		    avg(value)     AS count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY time_bucket, pool_name
		ORDER BY time_bucket, pool_name
	`,
		bucket, poolAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBConnectionPendReqs,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	out := make([]PendingRequestsPoint, len(rows))
	for i, row := range rows {
		out[i] = PendingRequestsPoint{
			TimeBucket: dbutil.StringFromAny(row["time_bucket"]),
			PoolName:   dbutil.StringFromAny(row["pool_name"]),
			Count:      dbutil.NullableFloat64FromAny(row["count"]),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetConnectionTimeoutRate(teamID int64, startMs, endMs int64) ([]ConnectionTimeoutPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	poolAttr := attrString(AttrPoolName)
	bucketSec := bucketWidthSeconds(startMs, endMs)

	query := fmt.Sprintf(`
		SELECT
		    %s                               AS time_bucket,
		    %s                               AS pool_name,
		    toFloat64(sum(value)) / %f        AS timeout_rate
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		GROUP BY time_bucket, pool_name
		ORDER BY time_bucket, pool_name
	`,
		bucket, poolAttr,
		bucketSec,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBConnectionTimeouts,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	out := make([]ConnectionTimeoutPoint, len(rows))
	for i, row := range rows {
		out[i] = ConnectionTimeoutPoint{
			TimeBucket:  dbutil.StringFromAny(row["time_bucket"]),
			PoolName:    dbutil.StringFromAny(row["pool_name"]),
			TimeoutRate: dbutil.NullableFloat64FromAny(row["timeout_rate"]),
		}
	}
	return out, nil
}

// -----------------------------------------------------------------------
// Section 8 — Connection Pool Latency
// -----------------------------------------------------------------------

func (r *ClickHouseRepository) poolLatency(teamID int64, startMs, endMs int64, metricName string) ([]PoolLatencyPoint, error) {
	bucket := timebucket.Expression(startMs, endMs)
	poolAttr := attrString(AttrPoolName)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                              AS time_bucket,
		    %s                                                                              AS pool_name,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p50_ms,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p95_ms,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		GROUP BY time_bucket, pool_name
		ORDER BY time_bucket, pool_name
	`,
		bucket, poolAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, metricName,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	out := make([]PoolLatencyPoint, len(rows))
	for i, row := range rows {
		out[i] = PoolLatencyPoint{
			TimeBucket: dbutil.StringFromAny(row["time_bucket"]),
			PoolName:   dbutil.StringFromAny(row["pool_name"]),
			P50Ms:      dbutil.NullableFloat64FromAny(row["p50_ms"]),
			P95Ms:      dbutil.NullableFloat64FromAny(row["p95_ms"]),
			P99Ms:      dbutil.NullableFloat64FromAny(row["p99_ms"]),
		}
	}
	return out, nil
}

func (r *ClickHouseRepository) GetConnectionWaitTime(teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error) {
	return r.poolLatency(teamID, startMs, endMs, MetricDBConnectionWaitTime)
}

func (r *ClickHouseRepository) GetConnectionCreateTime(teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error) {
	return r.poolLatency(teamID, startMs, endMs, MetricDBConnectionCreateTime)
}

func (r *ClickHouseRepository) GetConnectionUseTime(teamID int64, startMs, endMs int64) ([]PoolLatencyPoint, error) {
	return r.poolLatency(teamID, startMs, endMs, MetricDBConnectionUseTime)
}
