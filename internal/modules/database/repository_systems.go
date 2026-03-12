package database

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

func (r *ClickHouseRepository) GetDetectedSystems(teamID int64, startMs, endMs int64) ([]DetectedSystem, error) {
	systemAttr := attrString(AttrDBSystem)
	serverAttr := attrString(AttrServerAddress)
	errorAttr := attrString(AttrErrorType)

	query := fmt.Sprintf(`
		SELECT
		    %s                                                                             AS db_system,
		    sum(hist_count)                                                                AS span_count,
		    sumIf(hist_count, notEmpty(%s))                                                AS error_count,
		    quantileExactWeighted(0.50)(hist_sum / nullIf(hist_count, 0), hist_count) * 1000 AS avg_latency_ms,
		    sum(hist_count)                                                                AS query_count,
		    any(%s)                                                                        AS server_address,
		    max(timestamp)                                                                 AS last_seen
		FROM %s
		WHERE %s = ?
		  AND %s BETWEEN ? AND ?
		  AND %s = '%s'
		  AND metric_type = 'Histogram'
		  AND notEmpty(%s)
		GROUP BY db_system
		ORDER BY span_count DESC
	`,
		systemAttr,
		errorAttr,
		serverAttr,
		TableMetrics,
		ColTeamID, ColTimestamp,
		ColMetricName, MetricDBOperationDuration,
		systemAttr,
	)

	rows, err := dbutil.QueryMaps(r.db, query, uint32(teamID), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	out := make([]DetectedSystem, len(rows))
	for i, row := range rows {
		out[i] = DetectedSystem{
			DBSystem:      dbutil.StringFromAny(row["db_system"]),
			SpanCount:     dbutil.Int64FromAny(row["span_count"]),
			ErrorCount:    dbutil.Int64FromAny(row["error_count"]),
			AvgLatencyMs:  dbutil.Float64FromAny(row["avg_latency_ms"]),
			QueryCount:    dbutil.Int64FromAny(row["query_count"]),
			ServerAddress: dbutil.StringFromAny(row["server_address"]),
			LastSeen:      dbutil.StringFromAny(row["last_seen"]),
		}
	}
	return out, nil
}
