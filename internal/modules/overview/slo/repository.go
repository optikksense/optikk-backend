package slo

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

func sloBucketExpr(startMs, endMs int64) string {
	return timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
}

type Repository interface {
	GetSummary(teamID int64, startMs, endMs int64, serviceName string) (Summary, error)
	GetTimeSeries(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSlice, error)
	GetBurnDown(teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error)
	GetBurnRate(teamID int64, startMs, endMs int64, serviceName string) (*BurnRate, error)
}

type ClickHouseRepository struct {
	db dbutil.Querier
}

func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetSummary(teamID int64, startMs, endMs int64, serviceName string) (Summary, error) {
	query := `
		SELECT total_requests,
		       error_count,
		       if(total_requests > 0,
		          (total_requests-error_count)*100.0/total_requests,
		          100.0)              AS availability_percent,
		       avg_latency_ms,
		       p95_latency_ms
		FROM (
			SELECT count()                                                                      AS total_requests,
			       countIf(` + ErrorCondition() + `)                                           AS error_count,
			       avg(s.duration_nano / 1000000.0)                                            AS avg_latency_ms,
			       quantile(` + fmt.Sprintf("%.2f", QuantileP95) + `)(s.duration_nano / 1000000.0) AS p95_latency_ms
			FROM observability.spans s
			WHERE s.team_id = ? AND ` + RootSpanCondition() + ` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`
	args := []any{teamID, timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += `
		)`

	row, err := dbutil.QueryMap(r.db, query, args...)
	if err != nil {
		return Summary{}, err
	}

	return Summary{
		TotalRequests:       dbutil.Int64FromAny(row["total_requests"]),
		ErrorCount:          dbutil.Int64FromAny(row["error_count"]),
		AvailabilityPercent: dbutil.Float64FromAny(row["availability_percent"]),
		AvgLatencyMs:        dbutil.Float64FromAny(row["avg_latency_ms"]),
		P95LatencyMs:        dbutil.Float64FromAny(row["p95_latency_ms"]),
	}, nil
}

func (r *ClickHouseRepository) GetTimeSeries(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSlice, error) {
	bucket := sloBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT time_bucket,
		       request_count,
		       error_count,
		       if(request_count > 0,
		          (request_count-error_count)*100.0/request_count,
		          100.0)            AS availability_percent,
		       avg_latency_ms
		FROM (
			SELECT %s                                   AS time_bucket,
			       count()                              AS request_count,
			       countIf(`+ErrorCondition()+`)        AS error_count,
			       avg(s.duration_nano / 1000000.0)     AS avg_latency_ms
			FROM observability.spans s
			WHERE s.team_id = ? AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN ? AND ? AND s.timestamp BETWEEN ? AND ?`, bucket)
	args := []any{teamID, timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY 1
		)
		ORDER BY 1 ASC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	slices := make([]TimeSlice, len(rows))
	for i, row := range rows {
		slices[i] = TimeSlice{
			Timestamp:           dbutil.StringFromAny(row["time_bucket"]),
			RequestCount:        dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:          dbutil.Int64FromAny(row["error_count"]),
			AvailabilityPercent: dbutil.Float64FromAny(row["availability_percent"]),
			AvgLatencyMs:        dbutil.NullableFloat64FromAny(row["avg_latency_ms"]),
		}
	}
	return slices, nil
}
