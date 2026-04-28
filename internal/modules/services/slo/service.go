package slo

import (
	"context"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/utils"
	"golang.org/x/sync/errgroup"
)

const (
	availabilityTarget = 99.9
	p95LatencyTargetMs = 300.0
)

type Service interface {
	GetSloSli(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*Response, error)
	GetBurnDown(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error)
	GetBurnRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*BurnRate, error)
}

type SLOService struct {
	repo Repository
	now  func() time.Time
}

func NewService(repo Repository) Service {
	return &SLOService{repo: repo, now: time.Now}
}

func (s *SLOService) GetSloSli(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*Response, error) {
	g, gctx := errgroup.WithContext(ctx)

	var summaryRow SummaryRow
	var sliceRows []TimeSliceRow

	g.Go(func() error {
		var err error
		if serviceName != "" {
			summaryRow, err = s.repo.GetSummaryByService(gctx, teamID, startMs, endMs, serviceName)
		} else {
			summaryRow, err = s.repo.GetSummary(gctx, teamID, startMs, endMs)
		}
		return err
	})
	g.Go(func() error {
		var err error
		if serviceName != "" {
			sliceRows, err = s.repo.GetTimeSeriesByService(gctx, teamID, startMs, endMs, serviceName)
		} else {
			sliceRows, err = s.repo.GetTimeSeries(gctx, teamID, startMs, endMs)
		}
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	summary := buildSummary(summaryRow)
	slices := buildTimeSlices(sliceRows)
	errorBudgetRemaining := remainingErrorBudgetPercent(summary.AvailabilityPercent)

	return &Response{
		Objectives: Objectives{
			AvailabilityTarget: availabilityTarget,
			P95LatencyTargetMs: p95LatencyTargetMs,
		},
		Status: Status{
			AvailabilityPercent:         summary.AvailabilityPercent,
			P95LatencyMs:                summary.P95LatencyMs,
			ErrorBudgetRemainingPercent: errorBudgetRemaining,
			Compliant:                   summary.AvailabilityPercent >= availabilityTarget && summary.P95LatencyMs <= p95LatencyTargetMs,
		},
		Summary:    summary,
		Timeseries: slices,
	}, nil
}

func (s *SLOService) GetBurnDown(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error) {
	var rows []BurnDownRow
	var err error
	if serviceName != "" {
		rows, err = s.repo.GetBurnDownByService(ctx, teamID, startMs, endMs, serviceName)
	} else {
		rows, err = s.repo.GetBurnDown(ctx, teamID, startMs, endMs)
	}
	if err != nil {
		return nil, err
	}

	totalBudget := 100.0 - availabilityTarget
	var cumErrors, cumRequests int64
	points := make([]BurnDownPoint, len(rows))
	for i, row := range rows {
		cumErrors += int64(row.ErrorCount)     //nolint:gosec // domain-bounded
		cumRequests += int64(row.RequestCount) //nolint:gosec // domain-bounded

		remaining := 100.0
		if cumRequests > 0 && totalBudget > 0 {
			burned := float64(cumErrors) * 100.0 / float64(cumRequests)
			remaining = (totalBudget - burned) * 100.0 / totalBudget
			if remaining < 0 {
				remaining = 0
			}
			if remaining > 100 {
				remaining = 100
			}
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

func (s *SLOService) GetBurnRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*BurnRate, error) {
	now := s.now()
	fastUntilMs := now.UnixMilli()
	fastSinceMs := now.Add(-5 * time.Minute).UnixMilli()
	slowSinceMs := now.Add(-time.Hour).UnixMilli()

	g, gctx := errgroup.WithContext(ctx)
	var fastRow, slowRow WindowCountsRow
	var summaryRow SummaryRow

	g.Go(func() error {
		var err error
		if serviceName != "" {
			fastRow, err = s.repo.ErrorRateForWindowByService(gctx, teamID, fastSinceMs, fastUntilMs, serviceName)
		} else {
			fastRow, err = s.repo.ErrorRateForWindow(gctx, teamID, fastSinceMs, fastUntilMs)
		}
		return err
	})
	g.Go(func() error {
		var err error
		if serviceName != "" {
			slowRow, err = s.repo.ErrorRateForWindowByService(gctx, teamID, slowSinceMs, fastUntilMs, serviceName)
		} else {
			slowRow, err = s.repo.ErrorRateForWindow(gctx, teamID, slowSinceMs, fastUntilMs)
		}
		return err
	})
	g.Go(func() error {
		var err error
		if serviceName != "" {
			summaryRow, err = s.repo.GetSummaryByService(gctx, teamID, startMs, endMs, serviceName)
		} else {
			summaryRow, err = s.repo.GetSummary(gctx, teamID, startMs, endMs)
		}
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	summary := buildSummary(summaryRow)
	return &BurnRate{
		FastBurnRate:    errorPct(fastRow),
		SlowBurnRate:    errorPct(slowRow),
		FastWindow:      "5m",
		SlowWindow:      "1h",
		BudgetRemaining: remainingErrorBudgetPercent(summary.AvailabilityPercent),
	}, nil
}

func buildSummary(row SummaryRow) Summary {
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
		P95LatencyMs:        utils.SanitizeFloat(float64(row.P95LatencyMs)),
	}
}

func buildTimeSlices(rows []TimeSliceRow) []TimeSlice {
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
	return slices
}

func errorPct(row WindowCountsRow) float64 {
	if row.RequestCount == 0 {
		return 0
	}
	return float64(row.ErrorCount) * 100.0 / float64(row.RequestCount)
}

func remainingErrorBudgetPercent(availabilityPercent float64) float64 {
	totalBudget := 100.0 - availabilityTarget
	if totalBudget <= 0 {
		return 100.0
	}

	burned := 100.0 - availabilityPercent
	remaining := (totalBudget - burned) * 100.0 / totalBudget
	switch {
	case remaining < 0:
		return 0
	case remaining > 100:
		return 100
	default:
		return remaining
	}
}
