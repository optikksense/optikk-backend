package slo

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

const serviceNameFilter = " AND s.service_name = @serviceName"

func sloBucketExpr(startMs, endMs int64) string {
	return timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
}

type Repository interface {
	GetSummary(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (summaryDTO, error)
	GetTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]timeSliceDTO, error)
	GetBurnDown(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error)
	GetBurnRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*BurnRate, error)
}

type ClickHouseRepository struct {
	db *dbutil.NativeQuerier
}

func NewRepository(db *dbutil.NativeQuerier) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// summaryRow is the DTO for GetSummary.
type summaryRow struct {
	TotalRequests       int64   `ch:"total_requests"`
	ErrorCount          int64   `ch:"error_count"`
	AvailabilityPercent float64 `ch:"availability_percent"`
	AvgLatencyMs        float64 `ch:"avg_latency_ms"`
	P95LatencyMs        float64 `ch:"p95_latency_ms"`
}

func (r *ClickHouseRepository) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (Summary, error) {
	query := `
		SELECT total_requests,
		       error_count,
		       if(total_requests > 0,
		          (total_requests-error_count)*100.0/total_requests,
		          100.0)              AS availability_percent,
		       avg_latency_ms,
		       p95_latency_ms
		FROM (
			SELECT toInt64(count())                                                             AS total_requests,
			       toInt64(countIf(` + ErrorCondition() + `))                                  AS error_count,
			       avg(s.duration_nano / 1000000.0)                                            AS avg_latency_ms,
			       quantile(` + fmt.Sprintf("%.2f", QuantileP95) + `)(s.duration_nano / 1000000.0) AS p95_latency_ms
			FROM observability.spans s
			WHERE s.team_id = @teamID AND ` + RootSpanCondition() + ` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`
	args := dbutil.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
		)`

	var row summaryRow
	if err := r.db.QueryRow(ctx, &row, query, args...); err != nil {
		return Summary{}, err
	}

	return Summary(row), nil
}

// timeSliceRow is the DTO for GetTimeSeries (AvgLatencyMs is float64 in CH, converted to *float64 in model).
type timeSliceRow struct {
	TimeBucket          string  `ch:"time_bucket"`
	RequestCount        int64   `ch:"request_count"`
	ErrorCount          int64   `ch:"error_count"`
	AvailabilityPercent float64 `ch:"availability_percent"`
	AvgLatencyMs        float64 `ch:"avg_latency_ms"`
}

func (r *ClickHouseRepository) GetTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]TimeSlice, error) {
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
			       toInt64(count())                     AS request_count,
			       toInt64(countIf(`+ErrorCondition()+`)) AS error_count,
			       avg(s.duration_nano / 1000000.0)     AS avg_latency_ms
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := dbutil.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += ` GROUP BY 1
		)
		ORDER BY 1 ASC`

	var rows []timeSliceRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
		return nil, err
	}

	slices := make([]TimeSlice, len(rows))
	for i, row := range rows {
		avg := row.AvgLatencyMs
		slices[i] = TimeSlice{
			Timestamp:           row.TimeBucket,
			RequestCount:        row.RequestCount,
			ErrorCount:          row.ErrorCount,
			AvailabilityPercent: row.AvailabilityPercent,
			AvgLatencyMs:        &avg,
		}
	}
	return slices, nil
}
