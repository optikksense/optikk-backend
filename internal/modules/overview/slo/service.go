package slo

import "context"

const (
	availabilityTarget = 99.9
	p95LatencyTargetMs = 300.0
)

type Service interface {
	GetSloSli(teamID int64, startMs, endMs int64, serviceName string) (*Response, error)
	GetBurnDown(teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error)
	GetBurnRate(teamID int64, startMs, endMs int64, serviceName string) (*BurnRate, error)
}

type SLOService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &SLOService{repo: repo}
}

func (s *SLOService) GetSloSli(teamID int64, startMs, endMs int64, serviceName string) (*Response, error) {
	ctx := context.Background()

	summary, err := s.repo.GetSummary(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}

	timeseries, err := s.repo.GetTimeSeries(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}

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
		Timeseries: timeseries,
	}, nil
}

func (s *SLOService) GetBurnDown(teamID int64, startMs, endMs int64, serviceName string) ([]BurnDownPoint, error) {
	return s.repo.GetBurnDown(context.Background(), teamID, startMs, endMs, serviceName)
}

func (s *SLOService) GetBurnRate(teamID int64, startMs, endMs int64, serviceName string) (*BurnRate, error) {
	return s.repo.GetBurnRate(context.Background(), teamID, startMs, endMs, serviceName)
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
