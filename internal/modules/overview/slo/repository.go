package slo

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
)

// sloBucketExpr returns a ClickHouse time-bucketing expression for adaptive granularity.
func sloBucketExpr(startMs, endMs int64) string {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return "formatDateTime(toStartOfMinute(start_time), '%Y-%m-%d %H:%i:00')"
	case hours <= 24:
		return "formatDateTime(toStartOfFiveMinutes(start_time), '%Y-%m-%d %H:%i:00')"
	case hours <= 168:
		return "formatDateTime(toStartOfHour(start_time), '%Y-%m-%d %H:%i:00')"
	default:
		return "formatDateTime(toStartOfDay(start_time), '%Y-%m-%d %H:%i:00')"
	}
}

// Repository encapsulates data access logic for the SLO dashboard.
type Repository interface {
	GetSummary(teamUUID string, startMs, endMs int64, serviceName string) (Summary, error)
	GetTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSlice, error)
}

// ClickHouseRepository encapsulates overview SLO data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new overview SLO repository.
func NewRepository(db dbutil.Querier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetSummary(teamUUID string, startMs, endMs int64, serviceName string) (Summary, error) {
	query := `
		SELECT total_requests,
		       error_count,
		       if(total_requests > 0,
		          (total_requests-error_count)*100.0/total_requests,
		          100.0)              AS availability_percent,
		       avg_latency_ms,
		       p95_latency_ms
		FROM (
			SELECT count()                         AS total_requests,
			       countIf(status = 'ERROR' OR http_status_code >= 400) AS error_count,
			       avg(duration_ms)                AS avg_latency_ms,
			       quantile(0.95)(duration_ms)     AS p95_latency_ms
			FROM spans
			WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
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

func (r *ClickHouseRepository) GetTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSlice, error) {
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
			SELECT %s                     AS time_bucket,
			       count()                   AS request_count,
			       countIf(status = 'ERROR' OR http_status_code >= 400) AS error_count,
			       avg(duration_ms)          AS avg_latency_ms
			FROM spans
			WHERE team_id = ? AND is_root = 1 AND start_time BETWEEN ? AND ?`, bucket)
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
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
