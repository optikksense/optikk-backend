package slo

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/observability/observability-backend-go/internal/database"
)

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
	args := database.SpanBaseParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += ` AND s.service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += ` GROUP BY time_bucket ORDER BY time_bucket ASC`

	var rows []burnDownRow
	if err := r.db.Select(ctx, &rows, query, args...); err != nil {
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

func (r *ClickHouseRepository) GetBurnRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*BurnRate, error) {
	// Fast burn: last 5 minutes error rate
	fastRate, err := r.errorRateForWindow(ctx, teamID, 5, serviceName)
	if err != nil {
		return nil, err
	}
	// Slow burn: last 60 minutes error rate
	slowRate, err := r.errorRateForWindow(ctx, teamID, 60, serviceName)
	if err != nil {
		return nil, err
	}

	// Overall budget remaining
	summary, err := r.GetSummary(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}

	return &BurnRate{
		FastBurnRate:    fastRate,
		SlowBurnRate:    slowRate,
		FastWindow:      "5m",
		SlowWindow:      "1h",
		BudgetRemaining: remainingErrorBudgetPercent(summary.AvailabilityPercent),
	}, nil
}

// errorRateRow is the DTO for scalar error rate queries.
type errorRateRow struct {
	ErrorRate float64 `ch:"error_rate"`
}

func (r *ClickHouseRepository) errorRateForWindow(ctx context.Context, teamID int64, minutes int, serviceName string) (float64, error) {
	query := fmt.Sprintf(`
		SELECT countIf(`+ErrorCondition()+`) * 100.0 / count() AS error_rate
		FROM observability.spans s
		WHERE s.team_id = @teamID AND `+RootSpanCondition()+`
		  AND s.timestamp >= now() - INTERVAL %d MINUTE`, minutes)
	args := []any{clickhouse.Named("teamID", uint32(teamID))} //nolint:gosec // G115
	if serviceName != "" {
		query += ` AND s.service_name = @serviceName`
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}

	var row errorRateRow
	if err := r.db.QueryRow(ctx, &row, query, args...); err != nil {
		return 0, err
	}
	return row.ErrorRate, nil
}
