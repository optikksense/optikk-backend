package slo

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
)

const serviceNameFilter = " AND s.service_name = @serviceName"

func sloBucketExpr(startMs, endMs int64) string {
	return timebucket.ExprForColumn(startMs, endMs, "s.timestamp")
}

type Repository interface {
	GetSummary(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (summaryRow, error)
	GetTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]timeSliceRow, error)
	GetBurnDown(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error)
	ErrorRateForWindow(ctx context.Context, teamID int64, minutes int, serviceName string) (float64, error)
}

type ClickHouseRepository struct {
	db clickhouse.Conn
}

func NewRepository(db clickhouse.Conn) *ClickHouseRepository {
	return &ClickHouseRepository{db: db}
}

// BurnDownPoint represents a single point on the error budget burn-down chart.
type BurnDownPoint struct {
	Timestamp               string  `json:"timestamp"`
	ErrorBudgetRemainingPct float64 `json:"error_budget_remaining_pct"`
	CumulativeErrorCount    int64   `json:"cumulative_error_count"`
	CumulativeRequestCount  int64   `json:"cumulative_request_count"`
}

// BurnRate holds the current fast and slow burn rates.
type BurnRate struct {
	FastBurnRate    float64 `json:"fast_burn_rate"`
	SlowBurnRate    float64 `json:"slow_burn_rate"`
	FastWindow      string  `json:"fast_window"`
	SlowWindow      string  `json:"slow_window"`
	BudgetRemaining float64 `json:"budget_remaining_pct"`
}

// summaryRow is the DTO for GetSummary. LatencySumMs/LatencyCount power the
// Go-side avg; P95 is filled by the service from sketches (sketch.SpanLatencyService).
type summaryRow struct {
	TotalRequests  int64   `ch:"total_requests"`
	ErrorCount     int64   `ch:"error_count"`
	LatencySumMs   float64 `ch:"latency_sum_ms"`
	LatencyCount   int64   `ch:"latency_count"`
	P95LatencyMs   float64 `ch:"p95_latency_ms"`
}

func (r *ClickHouseRepository) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (summaryRow, error) {
	// p95 comes from sketch.Querier (SpanLatencyService) — see service.go.
	// Avg is computed in Go from latency_sum_ms / latency_count.
	query := `
		SELECT toInt64(count())                                         AS total_requests,
		       toInt64(countIf(` + ErrorCondition() + `))              AS error_count,
		       sum(s.duration_nano / 1000000.0)                        AS latency_sum_ms,
		       toInt64(count())                                         AS latency_count,
		       0 AS p95_latency_ms
		FROM observability.spans s
		WHERE s.team_id = @teamID AND ` + RootSpanCondition() + ` AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd AND s.timestamp BETWEEN @start AND @end`
	args := dbutil.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}

	var row summaryRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, args...).ScanStruct(&row); err != nil {
		return summaryRow{}, err
	}

	return row, nil
}

// timeSliceRow is the DTO for GetTimeSeries. LatencySumMs/LatencyCount feed
// the Go-side avg; no percentiles in the time series path.
type timeSliceRow struct {
	TimeBucket          string  `ch:"time_bucket"`
	RequestCount        int64   `ch:"request_count"`
	ErrorCount          int64   `ch:"error_count"`
	AvailabilityPercent float64 `ch:"availability_percent"`
	LatencySumMs        float64 `ch:"latency_sum_ms"`
	LatencyCount        int64   `ch:"latency_count"`
}

func (r *ClickHouseRepository) GetTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]timeSliceRow, error) {
	bucket := sloBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT time_bucket,
		       request_count,
		       error_count,
		       if(request_count > 0,
		          (request_count-error_count)*100.0/request_count,
		          100.0)            AS availability_percent,
		       latency_sum_ms,
		       latency_count
		FROM (
			SELECT %s                                   AS time_bucket,
			       toInt64(count())                     AS request_count,
			       toInt64(countIf(`+ErrorCondition()+`)) AS error_count,
			       sum(s.duration_nano / 1000000.0)     AS latency_sum_ms,
			       toInt64(count())                     AS latency_count
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
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}
	return rows, nil
}

// burnDownRow is the DTO for the raw per-bucket row from ClickHouse.
type burnDownRow struct {
	TimeBucket   string `ch:"time_bucket"`
	RequestCount int64  `ch:"request_count"`
	ErrorCount   int64  `ch:"error_count"`
}

func (r *ClickHouseRepository) GetBurnDown(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error) {
	bucket := sloBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       toInt64(count())              AS request_count,
		       toInt64(countIf(`+ErrorCondition()+`)) AS error_count
		FROM observability.spans s
		WHERE s.team_id = @teamID AND `+RootSpanCondition()+`
		  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
		  AND s.timestamp BETWEEN @start AND @end`, bucket)
	args := dbutil.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += ` GROUP BY time_bucket ORDER BY time_bucket ASC`

	var rows []burnDownRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}

	totalBudget := 100.0 - availabilityTarget
	var cumErrors, cumRequests int64
	points := make([]BurnDownPoint, len(rows))
	for i, row := range rows {
		cumErrors += row.ErrorCount
		cumRequests += row.RequestCount

		var remaining float64
		if cumRequests > 0 && totalBudget > 0 {
			burned := float64(cumErrors) * 100.0 / float64(cumRequests)
			remaining = (totalBudget - burned) * 100.0 / totalBudget
			if remaining < 0 {
				remaining = 0
			}
			if remaining > 100 {
				remaining = 100
			}
		} else {
			remaining = 100
		}

		points[i] = BurnDownPoint{
			Timestamp:               row.TimeBucket,
			ErrorBudgetRemainingPct: remaining,
			CumulativeErrorCount:    cumErrors,
			CumulativeRequestCount:  cumRequests,
		}
	}
	return points, nil
}

// errorRateRow is the DTO for scalar error rate queries.
type errorRateRow struct {
	ErrorRate float64 `ch:"error_rate"`
}

func (r *ClickHouseRepository) ErrorRateForWindow(ctx context.Context, teamID int64, minutes int, serviceName string) (float64, error) {
	query := fmt.Sprintf(`
		SELECT countIf(`+ErrorCondition()+`) * 100.0 / count() AS error_rate
		FROM observability.spans s
		WHERE s.team_id = @teamID AND `+RootSpanCondition()+`
		  AND s.timestamp >= now() - INTERVAL %d MINUTE`, minutes)
	args := []any{clickhouse.Named("teamID", uint32(teamID))} //nolint:gosec // G115
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}

	var row errorRateRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, args...).ScanStruct(&row); err != nil {
		return 0, err
	}
	return row.ErrorRate, nil
}
