package database

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

func (r *ClickHouseRepository) GetSummaryStats(teamID int64, startMs, endMs int64, f Filters) (SummaryStats, error) {
	fc, fargs := filterClauses(f)

	durationMs := float64(endMs-startMs) / 1000.0
	if durationMs <= 0 {
		durationMs = 1
	}

	// Main histogram aggregate: avg, p95, span count, error count.
	qMain := fmt.Sprintf(`
		SELECT
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count)  AS p50,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count)  AS p95,
		    sum(hist_count)                                                             AS total_count,
		    sumIf(hist_count, notEmpty(%s))                                             AS error_count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  %s
	`,
		attrString(AttrErrorType),
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		fc,
	)

	mainArgs := buildArgs([]any{uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}, fargs)
	row, err := dbutil.QueryMap(r.db, qMain, mainArgs...)
	if err != nil {
		return SummaryStats{}, err
	}

	p95 := dbutil.NullableFloat64FromAny(row["p95"])
	p50 := dbutil.NullableFloat64FromAny(row["p50"])
	totalCount := dbutil.Int64FromAny(row["total_count"])
	errorCount := dbutil.Int64FromAny(row["error_count"])

	// scale latencies from seconds → ms (OTel histogram stores seconds)
	scaleMs := func(v *float64) *float64 {
		if v == nil {
			return nil
		}
		ms := *v * 1000.0
		return &ms
	}

	errorRate := float64(errorCount) / durationMs
	var errorRatePtr *float64
	if totalCount > 0 {
		errorRatePtr = &errorRate
	}

	// Active connections: sum of "used" state across all pools.
	qConn := fmt.Sprintf(`
		SELECT toInt64(round(sum(value))) AS used_count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND %s = 'used'
	`,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBConnectionCount,
		attrString(AttrConnectionState),
	)
	connRow, err := dbutil.QueryMap(r.db, qConn, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	activeConns := int64(0)
	if err == nil {
		activeConns = dbutil.Int64FromAny(connRow["used_count"])
	}

	// Cache hit rate: redis success / redis total.
	qCache := fmt.Sprintf(`
		SELECT
		    countIf(empty(%s))       AS success_count,
		    count()                   AS total_count
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND %s = 'redis'
		  AND metric_type = 'Histogram'
	`,
		attrString(AttrErrorType),
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		attrString(AttrDBSystem),
	)
	cacheRow, err := dbutil.QueryMap(r.db, qCache, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	var cacheHitRate *float64
	if err == nil {
		redisTotal := dbutil.Int64FromAny(cacheRow["total_count"])
		redisSuccess := dbutil.Int64FromAny(cacheRow["success_count"])
		if redisTotal > 0 {
			rate := float64(redisSuccess) / float64(redisTotal) * 100
			cacheHitRate = &rate
		}
	}

	return SummaryStats{
		AvgLatencyMs:      scaleMs(p50),
		P95LatencyMs:      scaleMs(p95),
		SpanCount:         totalCount,
		ActiveConnections: activeConns,
		ErrorRate:         errorRatePtr,
		CacheHitRate:      cacheHitRate,
	}, nil
}

// latencySeriesByAttr is the shared helper for all "latency by X over time" queries.
func (r *ClickHouseRepository) latencySeriesByAttr(
	teamID int64, startMs, endMs int64,
	groupAttr string,
	f Filters,
) ([]LatencyTimeSeries, error) {
	bucket := timebucket.Expression(startMs, endMs)
	fc, fargs := filterClauses(f)
	groupExpr := attrString(groupAttr)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                             AS time_bucket,
		    %s                                                                             AS group_by,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p50_ms,
		    quantileExactWeighted(0.95)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p95_ms,
		    quantileExactWeighted(0.99)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS p99_ms
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
