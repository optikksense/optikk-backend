package servicepage

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

func (r *ClickHouseRepository) GetServiceHealth(teamID, startMs, endMs int64) ([]ServiceHealth, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT service_name, request_count, error_count, error_rate, p95_latency
		FROM (
			SELECT s.service_name                                                             AS service_name,
			       count()                                                                    AS request_count,
			       countIf(`+ErrorCondition()+`)                                             AS error_count,
			       if(count() > 0, countIf(`+ErrorCondition()+`)*100.0/count(), 0)           AS error_rate,
			       quantile(`+fmt.Sprintf("%.2f", QuantileP95)+`)(s.duration_nano / 1000000.0) AS p95_latency
			FROM observability.spans s
			WHERE s.team_id = ?
			  AND `+RootSpanCondition()+`
			  AND s.ts_bucket_start BETWEEN ? AND ?
			  AND s.timestamp BETWEEN ? AND ?
			GROUP BY s.service_name
		)
		ORDER BY request_count DESC
	`, teamID, timebucket.SpansBucketStart(startMs/1000), timebucket.SpansBucketStart(endMs/1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	results := make([]ServiceHealth, len(rows))
	for i, row := range rows {
		errorRate := dbutil.Float64FromAny(row["error_rate"])
		results[i] = ServiceHealth{
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			ErrorRate:    errorRate,
			P95LatencyMs: dbutil.Float64FromAny(row["p95_latency"]),
			HealthStatus: deriveHealthStatus(errorRate),
		}
	}
	return results, nil
}

func deriveHealthStatus(errorRate float64) string {
	switch {
	case errorRate <= HealthyMaxErrorRate:
		return "healthy"
	case errorRate <= DegradedMaxErrorRate:
		return "degraded"
	default:
		return "critical"
	}
}
