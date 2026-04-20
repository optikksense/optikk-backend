package slo

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
)

// Reads target `observability.spans_rollup_1m` — see overview/repository.go
// for the SQL discipline. SLO queries need count + error_count + duration_sum
// + p95; every one of those is available as a state column + merge op, so the
// query reduces to `sumMerge` and `quantilesTDigestWeightedMerge` calls.

const serviceNameFilter = " AND service_name = @serviceName"

type Repository interface {
	GetSummary(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (Summary, error)
	GetTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]TimeSlice, error)
	GetBurnDown(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error)
	GetBurnRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*BurnRate, error)
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

func intervalMinutesFor(startMs, endMs int64) int64 {
	hours := (endMs - startMs) / 3_600_000
	switch {
	case hours <= 3:
		return 1
	case hours <= 24:
		return 5
	case hours <= 168:
		return 60
	default:
		return 1440
	}
}

func rollupParams(teamID int64, startMs, endMs int64) []any {
	return []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115 — tenant ID fits uint32
		clickhouse.Named("start", time.UnixMilli(startMs)),
		clickhouse.Named("end", time.UnixMilli(endMs)),
	}
}

// summaryRawRow scans the rollup-merge output; Summary's derived fields
// (availability, avg) are computed in Go to avoid SQL-side conditionals.
type summaryRawRow struct {
	RequestCount  uint64  `ch:"request_count"`
	ErrorCount    uint64  `ch:"error_count"`
	DurationMsSum float64 `ch:"duration_ms_sum"`
	P95LatencyMs  float64 `ch:"p95_latency_ms"`
}

func (r *ClickHouseRepository) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (Summary, error) {
	query := `
		SELECT sumMerge(request_count)                                            AS request_count,
		       sumMerge(error_count)                                              AS error_count,
		       sumMerge(duration_ms_sum)                                          AS duration_ms_sum,
		       quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(latency_ms_digest).2 AS p95_latency_ms
		FROM observability.spans_rollup_1m
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`
	args := rollupParams(teamID, startMs, endMs)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}

	var row summaryRawRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, args...).ScanStruct(&row); err != nil {
		return Summary{}, err
	}

	total := int64(row.RequestCount) //nolint:gosec // domain-bounded
	errs := int64(row.ErrorCount)    //nolint:gosec // domain-bounded
	availability := 100.0
	if total > 0 {
		availability = float64(total-errs) * 100.0 / float64(total)
	}
	avg := 0.0
	if row.RequestCount > 0 {
		avg = row.DurationMsSum / float64(row.RequestCount)
	}

	return Summary{
		TotalRequests:       total,
		ErrorCount:          errs,
		AvailabilityPercent: availability,
		AvgLatencyMs:        avg,
		P95LatencyMs:        row.P95LatencyMs,
	}, nil
}

// timeSliceRawRow is the per-bucket rollup merge output.
type timeSliceRawRow struct {
	TimeBucket    time.Time `ch:"time_bucket"`
	RequestCount  uint64    `ch:"request_count"`
	ErrorCount    uint64    `ch:"error_count"`
	DurationMsSum float64   `ch:"duration_ms_sum"`
}

func (r *ClickHouseRepository) GetTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]TimeSlice, error) {
	query := `
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       sumMerge(request_count)                                      AS request_count,
		       sumMerge(error_count)                                        AS error_count,
		       sumMerge(duration_ms_sum)                                    AS duration_ms_sum
		FROM observability.spans_rollup_1m
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", intervalMinutesFor(startMs, endMs)),
	)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
		GROUP BY time_bucket
		ORDER BY time_bucket ASC`

	var rows []timeSliceRawRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}

	slices := make([]TimeSlice, len(rows))
	for i, row := range rows {
		total := int64(row.RequestCount) //nolint:gosec // domain-bounded
		errs := int64(row.ErrorCount)    //nolint:gosec // domain-bounded
		availability := 100.0
		if total > 0 {
			availability = float64(total-errs) * 100.0 / float64(total)
		}
		var avgPtr *float64
		if row.RequestCount > 0 {
			avg := row.DurationMsSum / float64(row.RequestCount)
			avgPtr = &avg
		}
		slices[i] = TimeSlice{
			Timestamp:           row.TimeBucket.UTC().Format(time.RFC3339),
			RequestCount:        total,
			ErrorCount:          errs,
			AvailabilityPercent: availability,
			AvgLatencyMs:        avgPtr,
		}
	}
	return slices, nil
}

// burnDownRawRow is the per-bucket request/error count from the rollup.
type burnDownRawRow struct {
	TimeBucket   time.Time `ch:"time_bucket"`
	RequestCount uint64    `ch:"request_count"`
	ErrorCount   uint64    `ch:"error_count"`
}

func (r *ClickHouseRepository) GetBurnDown(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error) {
	query := `
		SELECT toStartOfInterval(bucket_ts, toIntervalMinute(@intervalMin)) AS time_bucket,
		       sumMerge(request_count)                                      AS request_count,
		       sumMerge(error_count)                                        AS error_count
		FROM observability.spans_rollup_1m
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @start AND @end`
	args := append(rollupParams(teamID, startMs, endMs),
		clickhouse.Named("intervalMin", intervalMinutesFor(startMs, endMs)),
	)
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}
	query += `
		GROUP BY time_bucket
		ORDER BY time_bucket ASC`

	var rows []burnDownRawRow
	if err := r.db.Select(dbutil.OverviewCtx(ctx), &rows, query, args...); err != nil {
		return nil, err
	}

	totalBudget := 100.0 - availabilityTarget
	var cumErrors, cumRequests int64
	points := make([]BurnDownPoint, len(rows))
	for i, row := range rows {
		cumErrors += int64(row.ErrorCount)     //nolint:gosec // domain-bounded
		cumRequests += int64(row.RequestCount) //nolint:gosec // domain-bounded

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
			Timestamp:               row.TimeBucket.UTC().Format(time.RFC3339),
			ErrorBudgetRemainingPct: remaining,
			CumulativeErrorCount:    cumErrors,
			CumulativeRequestCount:  cumRequests,
		}
	}
	return points, nil
}

func (r *ClickHouseRepository) GetBurnRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*BurnRate, error) {
	fastRate, err := r.errorRateForWindow(ctx, teamID, 5, serviceName)
	if err != nil {
		return nil, err
	}
	slowRate, err := r.errorRateForWindow(ctx, teamID, 60, serviceName)
	if err != nil {
		return nil, err
	}

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

type errorRateRawRow struct {
	RequestCount uint64 `ch:"request_count"`
	ErrorCount   uint64 `ch:"error_count"`
}

func (r *ClickHouseRepository) errorRateForWindow(ctx context.Context, teamID int64, minutes int, serviceName string) (float64, error) {
	now := time.Now()
	since := now.Add(-time.Duration(minutes) * time.Minute)
	query := `
		SELECT sumMerge(request_count) AS request_count,
		       sumMerge(error_count)   AS error_count
		FROM observability.spans_rollup_1m
		WHERE team_id = @teamID
		  AND bucket_ts BETWEEN @since AND @nowTs`
	args := []any{
		clickhouse.Named("teamID", uint32(teamID)), //nolint:gosec // G115
		clickhouse.Named("since", since),
		clickhouse.Named("nowTs", now),
	}
	if serviceName != "" {
		query += serviceNameFilter
		args = append(args, clickhouse.Named("serviceName", serviceName))
	}

	var row errorRateRawRow
	if err := r.db.QueryRow(dbutil.OverviewCtx(ctx), query, args...).ScanStruct(&row); err != nil {
		return 0, err
	}
	if row.RequestCount == 0 {
		return 0, nil
	}
	return float64(row.ErrorCount) * 100.0 / float64(row.RequestCount), nil
}
