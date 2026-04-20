package slo

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	dbutil "github.com/Optikk-Org/optikk-backend/internal/infra/database"
	timebucket "github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"golang.org/x/sync/errgroup"
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

// summaryRow is the DTO for GetSummary. TotalRequests/LatencySumMs come from a
// totals-only scan and ErrorCount from a parallel error-only scan; the service
// merges them. P95 is filled from sketches (sketch.SpanLatencyService).
type summaryRow struct {
	TotalRequests int64
	ErrorCount    int64
	LatencySumMs  float64
	LatencyCount  int64
	P95LatencyMs  float64
}

// summaryTotalsRow is the CH scan target for the totals leg: full volume +
// latency sum. LatencyCount is intentionally redundant with TotalRequests so
// avg = LatencySumMs / LatencyCount stays computable when we later split the
// numerator across different row sets.
type summaryTotalsRow struct {
	TotalRequests uint64  `ch:"total_requests"`
	LatencySumMs  float64 `ch:"latency_sum_ms"`
}

// summaryErrorsRow is the CH scan target for the error-only leg.
type summaryErrorsRow struct {
	ErrorCount uint64 `ch:"error_count"`
}

func (r *ClickHouseRepository) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (summaryRow, error) {
	var (
		totals summaryTotalsRow
		errs   summaryErrorsRow
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		query := `
			SELECT count()                            AS total_requests,
			       sum(s.duration_nano / 1000000.0)   AS latency_sum_ms
			FROM observability.spans s
			WHERE s.team_id = @teamID AND ` + RootSpanCondition() + `
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end`
		args := dbutil.SpanBaseParams(teamID, startMs, endMs)
		if serviceName != "" {
			query += serviceNameFilter
			args = append(args, clickhouse.Named("serviceName", serviceName))
		}
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&totals)
	})

	g.Go(func() error {
		query := `
			SELECT count() AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND ` + RootSpanCondition() + ` AND (` + ErrorCondition() + `)
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end`
		args := dbutil.SpanBaseParams(teamID, startMs, endMs)
		if serviceName != "" {
			query += serviceNameFilter
			args = append(args, clickhouse.Named("serviceName", serviceName))
		}
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&errs)
	})

	if err := g.Wait(); err != nil {
		return summaryRow{}, err
	}

	return summaryRow{
		TotalRequests: int64(totals.TotalRequests), //nolint:gosec // domain-bounded
		ErrorCount:    int64(errs.ErrorCount),      //nolint:gosec // domain-bounded
		LatencySumMs:  totals.LatencySumMs,
		LatencyCount:  int64(totals.TotalRequests), //nolint:gosec // domain-bounded
		P95LatencyMs:  0,
	}, nil
}

// timeSliceRow is the DTO returned to the service layer. AvailabilityPercent
// is computed in Go from request_count / error_count.
type timeSliceRow struct {
	TimeBucket          string
	RequestCount        int64
	ErrorCount          int64
	AvailabilityPercent float64
	LatencySumMs        float64
	LatencyCount        int64
}

// timeSliceTotalsRow is the CH scan target for the totals leg of the
// timeseries query: per-bucket volume + latency sum.
type timeSliceTotalsRow struct {
	TimeBucket   string  `ch:"time_bucket"`
	RequestCount uint64  `ch:"request_count"`
	LatencySumMs float64 `ch:"latency_sum_ms"`
}

// timeSliceErrorsRow is the CH scan target for the error-only leg of the
// timeseries query.
type timeSliceErrorsRow struct {
	TimeBucket string `ch:"time_bucket"`
	ErrorCount uint64 `ch:"error_count"`
}

func (r *ClickHouseRepository) GetTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]timeSliceRow, error) {
	bucket := sloBucketExpr(startMs, endMs)

	var (
		totals []timeSliceTotalsRow
		errs   []timeSliceErrorsRow
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT %s                                 AS time_bucket,
			       count()                            AS request_count,
			       sum(s.duration_nano / 1000000.0)   AS latency_sum_ms
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
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, query, args...)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT %s      AS time_bucket,
			       count() AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND (`+ErrorCondition()+`)
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end`, bucket)
		args := dbutil.SpanBaseParams(teamID, startMs, endMs)
		if serviceName != "" {
			query += serviceNameFilter
			args = append(args, clickhouse.Named("serviceName", serviceName))
		}
		query += ` GROUP BY time_bucket`
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, query, args...)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[e.TimeBucket] = e.ErrorCount
	}
	out := make([]timeSliceRow, 0, len(totals))
	for _, t := range totals {
		reqCount := int64(t.RequestCount)         //nolint:gosec // domain-bounded
		errCount := int64(errIdx[t.TimeBucket])   //nolint:gosec // domain-bounded
		availability := 100.0
		if reqCount > 0 {
			availability = float64(reqCount-errCount) * 100.0 / float64(reqCount)
		}
		out = append(out, timeSliceRow{
			TimeBucket:          t.TimeBucket,
			RequestCount:        reqCount,
			ErrorCount:          errCount,
			AvailabilityPercent: availability,
			LatencySumMs:        t.LatencySumMs,
			LatencyCount:        reqCount,
		})
	}
	return out, nil
}

// burnDownTotalsRow is the CH scan target for the totals leg of the burn-down
// aggregation.
type burnDownTotalsRow struct {
	TimeBucket   string `ch:"time_bucket"`
	RequestCount uint64 `ch:"request_count"`
}

// burnDownErrorsRow is the CH scan target for the error-only leg.
type burnDownErrorsRow struct {
	TimeBucket string `ch:"time_bucket"`
	ErrorCount uint64 `ch:"error_count"`
}

func (r *ClickHouseRepository) GetBurnDown(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error) {
	bucket := sloBucketExpr(startMs, endMs)

	var (
		totals []burnDownTotalsRow
		errs   []burnDownErrorsRow
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT %s      AS time_bucket,
			       count() AS request_count
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
		return r.db.Select(dbutil.OverviewCtx(gctx), &totals, query, args...)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT %s      AS time_bucket,
			       count() AS error_count
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND (`+ErrorCondition()+`)
			  AND s.ts_bucket_start BETWEEN @bucketStart AND @bucketEnd
			  AND s.timestamp BETWEEN @start AND @end`, bucket)
		args := dbutil.SpanBaseParams(teamID, startMs, endMs)
		if serviceName != "" {
			query += serviceNameFilter
			args = append(args, clickhouse.Named("serviceName", serviceName))
		}
		query += ` GROUP BY time_bucket`
		return r.db.Select(dbutil.OverviewCtx(gctx), &errs, query, args...)
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	errIdx := make(map[string]uint64, len(errs))
	for _, e := range errs {
		errIdx[e.TimeBucket] = e.ErrorCount
	}

	totalBudget := 100.0 - availabilityTarget
	var cumErrors, cumRequests int64
	points := make([]BurnDownPoint, len(totals))
	for i, t := range totals {
		cumErrors += int64(errIdx[t.TimeBucket]) //nolint:gosec // domain-bounded
		cumRequests += int64(t.RequestCount)     //nolint:gosec // domain-bounded

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
			Timestamp:               t.TimeBucket,
			ErrorBudgetRemainingPct: remaining,
			CumulativeErrorCount:    cumErrors,
			CumulativeRequestCount:  cumRequests,
		}
	}
	return points, nil
}

// scalarCountRow is a trivial CH scan target for count()-only scalar queries.
type scalarCountRow struct {
	Count uint64 `ch:"cnt"`
}

func (r *ClickHouseRepository) ErrorRateForWindow(ctx context.Context, teamID int64, minutes int, serviceName string) (float64, error) {
	var (
		total scalarCountRow
		errs  scalarCountRow
	)
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT count() AS cnt
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+`
			  AND s.timestamp >= now() - INTERVAL %d MINUTE`, minutes)
		args := []any{clickhouse.Named("teamID", uint32(teamID))} //nolint:gosec // G115
		if serviceName != "" {
			query += serviceNameFilter
			args = append(args, clickhouse.Named("serviceName", serviceName))
		}
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&total)
	})

	g.Go(func() error {
		query := fmt.Sprintf(`
			SELECT count() AS cnt
			FROM observability.spans s
			WHERE s.team_id = @teamID AND `+RootSpanCondition()+` AND (`+ErrorCondition()+`)
			  AND s.timestamp >= now() - INTERVAL %d MINUTE`, minutes)
		args := []any{clickhouse.Named("teamID", uint32(teamID))} //nolint:gosec // G115
		if serviceName != "" {
			query += serviceNameFilter
			args = append(args, clickhouse.Named("serviceName", serviceName))
		}
		return r.db.QueryRow(dbutil.OverviewCtx(gctx), query, args...).ScanStruct(&errs)
	})

	if err := g.Wait(); err != nil {
		return 0, err
	}
	if total.Count == 0 {
		return 0, nil
	}
	return float64(errs.Count) * 100.0 / float64(total.Count), nil
}
