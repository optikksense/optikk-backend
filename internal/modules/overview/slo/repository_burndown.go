package slo

import (
	"fmt"

	dbutil "github.com/observability/observability-backend-go/internal/database"
	timebucket "github.com/observability/observability-backend-go/internal/platform/timebucket"
)

// BurnDownPoint represents a single point on the error budget burn-down chart.
type BurnDownPoint struct {
	Timestamp              string  `json:"timestamp"`
	ErrorBudgetRemainingPct float64 `json:"error_budget_remaining_pct"`
	CumulativeErrorCount   int64   `json:"cumulative_error_count"`
	CumulativeRequestCount int64   `json:"cumulative_request_count"`
}

// BurnRate holds the current fast and slow burn rates.
type BurnRate struct {
	FastBurnRate  float64 `json:"fast_burn_rate"`
	SlowBurnRate  float64 `json:"slow_burn_rate"`
	FastWindow    string  `json:"fast_window"`
	SlowWindow    string  `json:"slow_window"`
	BudgetRemaining float64 `json:"budget_remaining_pct"`
}

func (r *ClickHouseRepository) GetBurnDown(teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error) {
	bucket := sloBucketExpr(startMs, endMs)
	query := fmt.Sprintf(`
		SELECT %s AS time_bucket,
		       count()                       AS request_count,
		       countIf(`+ErrorCondition()+`) AS error_count
		FROM observability.spans s
		WHERE s.team_id = ? AND `+RootSpanCondition()+`
		  AND s.ts_bucket_start BETWEEN ? AND ?
		  AND s.timestamp BETWEEN ? AND ?`, bucket)
	args := []any{teamID, timebucket.SpansBucketStart(startMs / 1000), timebucket.SpansBucketStart(endMs / 1000), dbutil.SqlTime(startMs), dbutil.SqlTime(endMs)}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}
	query += ` GROUP BY time_bucket ORDER BY time_bucket ASC`

	rows, err := dbutil.QueryMaps(r.db, query, args...)
	if err != nil {
		return nil, err
	}

	totalBudget := 100.0 - availabilityTarget
	var cumErrors, cumRequests int64
	points := make([]BurnDownPoint, len(rows))
	for i, row := range rows {
		errors := dbutil.Int64FromAny(row["error_count"])
		requests := dbutil.Int64FromAny(row["request_count"])
		cumErrors += errors
		cumRequests += requests

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
			Timestamp:              dbutil.StringFromAny(row["time_bucket"]),
			ErrorBudgetRemainingPct: remaining,
			CumulativeErrorCount:   cumErrors,
			CumulativeRequestCount: cumRequests,
		}
	}
	return points, nil
}

func (r *ClickHouseRepository) GetBurnRate(teamID int64, startMs, endMs int64, serviceName string) (*BurnRate, error) {
	// Fast burn: last 5 minutes error rate
	fastRate, err := r.errorRateForWindow(teamID, 5, serviceName)
	if err != nil {
		return nil, err
	}
	// Slow burn: last 60 minutes error rate
	slowRate, err := r.errorRateForWindow(teamID, 60, serviceName)
	if err != nil {
		return nil, err
	}

	// Overall budget remaining
	summary, err := r.GetSummary(teamID, startMs, endMs, serviceName)
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

func (r *ClickHouseRepository) errorRateForWindow(teamID int64, minutes int, serviceName string) (float64, error) {
	query := fmt.Sprintf(`
		SELECT countIf(`+ErrorCondition()+`) * 100.0 / count() AS error_rate
		FROM observability.spans s
		WHERE s.team_id = ? AND `+RootSpanCondition()+`
		  AND s.timestamp >= now() - INTERVAL %d MINUTE`, minutes)
	args := []any{teamID}
	if serviceName != "" {
		query += ` AND s.service_name = ?`
		args = append(args, serviceName)
	}

	row, err := dbutil.QueryMap(r.db, query, args...)
	if err != nil {
		return 0, err
	}
	return dbutil.Float64FromAny(row["error_rate"]), nil
}
