package slo

import (
	"context"

	"golang.org/x/sync/errgroup"
)

const (
	availabilityTarget	= 99.9
	p95LatencyTargetMs	= 300.0
)

type Service interface {
	GetSloSli(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*Response, error)
	GetBurnDown(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error)
	GetBurnRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*BurnRate, error)
}

type SLOService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &SLOService{repo: repo}
}

func (s *SLOService) GetSloSli(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*Response, error) {
	g, gctx := errgroup.WithContext(ctx)

	var summary Summary
	var timeseries []TimeSlice

	g.Go(func() error {
		var err error
		summary, err = s.repo.GetSummary(gctx, teamID, startMs, endMs, serviceName)
		return err
	})
	g.Go(func() error {
		var err error
		timeseries, err = s.repo.GetTimeSeries(gctx, teamID, startMs, endMs, serviceName)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	errorBudgetRemaining := remainingErrorBudgetPercent(summary.AvailabilityPercent)

	return &Response{
		Objectives: Objectives{
			AvailabilityTarget:	availabilityTarget,
			P95LatencyTargetMs:	p95LatencyTargetMs,
		},
		Status: Status{
			AvailabilityPercent:		summary.AvailabilityPercent,
			P95LatencyMs:			summary.P95LatencyMs,
			ErrorBudgetRemainingPercent:	errorBudgetRemaining,
			Compliant:			summary.AvailabilityPercent >= availabilityTarget && summary.P95LatencyMs <= p95LatencyTargetMs,
		},
		Summary:	summary,
		Timeseries:	timeseries,
	}, nil
}

func (s *SLOService) GetBurnDown(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error) {
	return s.repo.GetBurnDown(ctx, teamID, startMs, endMs, serviceName)
}

func (s *SLOService) GetBurnRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) (*BurnRate, error) {
	return s.repo.GetBurnRate(ctx, teamID, startMs, endMs, serviceName)
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
