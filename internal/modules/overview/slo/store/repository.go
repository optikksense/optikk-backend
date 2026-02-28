package store

import (
	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/overview/slo/model"
)

// Repository encapsulates data access logic for the SLO dashboard.
type Repository interface {
	GetSummary(teamUUID string, startMs, endMs int64, serviceName string) (model.Summary, error)
	GetTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSlice, error)
}

// ClickHouseRepository encapsulates overview SLO data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new overview SLO repository.
func NewRepository(db dbutil.Querier) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetSummary(teamUUID string, startMs, endMs int64, serviceName string) (model.Summary, error) {
	query := `
		SELECT sum(count) as total_requests,
		       sum(if(status='ERROR', count, 0)) as error_count,
		       if(sum(count) > 0, (sum(count)-sum(if(status='ERROR', count, 0)))*100.0/sum(count), 100.0) as availability_percent,
		       avg(avg) as avg_latency_ms,
		       avg(p95) as p95_latency_ms
		FROM metrics
		WHERE team_id = ? AND metric_category = 'http' AND timestamp BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}

	row, err := dbutil.QueryMap(r.db, query, args...)
	if err != nil {
		return model.Summary{}, err
	}

	return model.Summary{
		TotalRequests:       dbutil.Int64FromAny(row["total_requests"]),
		ErrorCount:          dbutil.Int64FromAny(row["error_count"]),
		AvailabilityPercent: dbutil.Float64FromAny(row["availability_percent"]),
		AvgLatencyMs:        dbutil.Float64FromAny(row["avg_latency_ms"]),
		P95LatencyMs:        dbutil.Float64FromAny(row["p95_latency_ms"]),
	}, nil
}

func (r *ClickHouseRepository) GetTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]model.TimeSlice, error) {
	query := `
		SELECT formatDateTime(toStartOfMinute(timestamp), '%Y-%m-%d %H:%i:00') as time_bucket,
		       sum(count) as request_count,
		       sum(if(status='ERROR', count, 0)) as error_count,
		       if(sum(count) > 0, (sum(count)-sum(if(status='ERROR', count, 0)))*100.0/sum(count), 100.0) as availability_percent,
		       avg(avg) as avg_latency_ms
		FROM metrics
		WHERE team_id = ? AND metric_category = 'http' AND timestamp BETWEEN ? AND ?`
	args := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY 1 ORDER BY 1 ASC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	slices := make([]model.TimeSlice, len(rows))
	for i, row := range rows {
		slices[i] = model.TimeSlice{
			Timestamp:           dbutil.StringFromAny(row["time_bucket"]),
			RequestCount:        dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:          dbutil.Int64FromAny(row["error_count"]),
			AvailabilityPercent: dbutil.Float64FromAny(row["availability_percent"]),
			AvgLatencyMs:        dbutil.NullableFloat64FromAny(row["avg_latency_ms"]),
		}
	}
	return slices, nil
}
