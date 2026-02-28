package store

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	"github.com/observability/observability-backend-go/internal/modules/services/service/model"
)

// serviceBucketExpr returns a ClickHouse expression for adaptive time bucketing.
func serviceBucketExpr(startMs, endMs int64) string {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return "minute"
	case hours <= 24:
		return "toStartOfInterval(minute, INTERVAL 5 MINUTE)"
	case hours <= 168:
		return "toStartOfInterval(minute, INTERVAL 60 MINUTE)"
	default:
		return "toStartOfInterval(minute, INTERVAL 1440 MINUTE)"
	}
}

const (
	healthyMaxErrorRate  = 1.0
	degradedMaxErrorRate = 5.0
)

// Repository encapsulates data access logic for the services overview page.
type Repository interface {
	GetTotalServices(teamUUID string, startMs, endMs int64) (int64, error)
	GetHealthyServices(teamUUID string, startMs, endMs int64) (int64, error)
	GetDegradedServices(teamUUID string, startMs, endMs int64) (int64, error)
	GetUnhealthyServices(teamUUID string, startMs, endMs int64) (int64, error)
	GetServiceMetrics(teamUUID string, startMs, endMs int64) ([]model.ServiceMetric, error)
	GetServiceTimeSeries(teamUUID string, startMs, endMs int64) ([]model.TimeSeriesPoint, error)
}

// ClickHouseRepository encapsulates services overview data access logic.
type ClickHouseRepository struct {
	db dbutil.Querier
}

// NewRepository creates a new services overview repository.
func NewRepository(db dbutil.Querier) Repository {
	return &ClickHouseRepository{db: db}
}

func (r *ClickHouseRepository) GetTotalServices(teamUUID string, startMs, endMs int64) (int64, error) {
	row, err := dbutil.QueryMap(r.db, `
		SELECT COUNT(*) as count
		FROM (
			SELECT service_name
			FROM observability.spans_service_1m
			WHERE team_id = ? AND minute BETWEEN ? AND ?
			GROUP BY service_name
		)
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return 0, err
	}
	return dbutil.Int64FromAny(row["count"]), nil
}

func (r *ClickHouseRepository) GetHealthyServices(teamUUID string, startMs, endMs int64) (int64, error) {
	return r.countServicesByErrorRate(teamUUID, startMs, endMs, "error_rate <= ?", healthyMaxErrorRate)
}

func (r *ClickHouseRepository) GetDegradedServices(teamUUID string, startMs, endMs int64) (int64, error) {
	return r.countServicesByErrorRate(teamUUID, startMs, endMs, "error_rate > ? AND error_rate <= ?", healthyMaxErrorRate, degradedMaxErrorRate)
}

func (r *ClickHouseRepository) GetUnhealthyServices(teamUUID string, startMs, endMs int64) (int64, error) {
	return r.countServicesByErrorRate(teamUUID, startMs, endMs, "error_rate > ?", degradedMaxErrorRate)
}

func (r *ClickHouseRepository) GetServiceMetrics(teamUUID string, startMs, endMs int64) ([]model.ServiceMetric, error) {
	rows, err := dbutil.QueryMaps(r.db, `
		SELECT service_name,
		       countMerge(request_count)       AS request_count,
		       countMerge(error_count)         AS error_count,
		       avgMerge(avg_state)             AS avg_latency,
		       quantileMerge(0.5)(p50_state)   AS p50_latency,
		       quantileMerge(0.95)(p95_state)  AS p95_latency,
		       quantileMerge(0.99)(p99_state)  AS p99_latency
		FROM observability.spans_service_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?
		GROUP BY service_name
		ORDER BY countMerge(request_count) DESC
	`, teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	metrics := make([]model.ServiceMetric, len(rows))
	for i, row := range rows {
		metrics[i] = model.ServiceMetric{
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
			P50Latency:   dbutil.Float64FromAny(row["p50_latency"]),
			P95Latency:   dbutil.Float64FromAny(row["p95_latency"]),
			P99Latency:   dbutil.Float64FromAny(row["p99_latency"]),
		}
	}
	return metrics, nil
}

func (r *ClickHouseRepository) GetServiceTimeSeries(teamUUID string, startMs, endMs int64) ([]model.TimeSeriesPoint, error) {
	bucket := serviceBucketExpr(startMs, endMs)
	rows, err := dbutil.QueryMaps(r.db, fmt.Sprintf(`
		SELECT service_name,
		       %s                    AS timestamp,
		       countMerge(request_count) AS request_count,
		       countMerge(error_count)   AS error_count,
		       avgMerge(avg_state)       AS avg_latency
		FROM observability.spans_service_1m
		WHERE team_id = ? AND minute BETWEEN ? AND ?
		GROUP BY service_name, %s
		ORDER BY timestamp ASC, countMerge(request_count) DESC
	`, bucket, bucket), teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs))
	if err != nil {
		return nil, err
	}

	points := make([]model.TimeSeriesPoint, len(rows))
	for i, row := range rows {
		points[i] = model.TimeSeriesPoint{
			Timestamp:    dbutil.TimeFromAny(row["timestamp"]),
			ServiceName:  dbutil.StringFromAny(row["service_name"]),
			RequestCount: dbutil.Int64FromAny(row["request_count"]),
			ErrorCount:   dbutil.Int64FromAny(row["error_count"]),
			AvgLatency:   dbutil.Float64FromAny(row["avg_latency"]),
		}
	}
	return points, nil
}

func (r *ClickHouseRepository) countServicesByErrorRate(teamUUID string, startMs, endMs int64, havingClause string, args ...any) (int64, error) {
	queryArgs := []any{teamUUID, dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	queryArgs = append(queryArgs, args...)

	row, err := dbutil.QueryMap(r.db, `
		SELECT COUNT(*) as count
		FROM (
			SELECT service_name,
			       if(countMerge(request_count) > 0,
			          countMerge(error_count)*100.0/countMerge(request_count), 0) as error_rate
			FROM observability.spans_service_1m
			WHERE team_id = ? AND minute BETWEEN ? AND ?
			GROUP BY service_name
			HAVING `+havingClause+`
		)
	`, queryArgs...)
	if err != nil {
		return 0, err
	}
	return dbutil.Int64FromAny(row["count"]), nil
}
