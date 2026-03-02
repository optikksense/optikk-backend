package slo

import ()

const (
	availabilityTarget = 99.9
	p95LatencyTargetMs = 300.0
)

// Service encapsulates the business logic for the overview SLO module.
type Service interface {
	GetSloSli(teamUUID string, startMs, endMs int64, serviceName string) (*Response, error)
}

// SLOService provides business logic orchestration for overview SLO dashboards.
type SLOService struct {
	repo Repository
}

// NewService creates a new SLOService.
func NewService(repo Repository) Service {
	return &SLOService{repo: repo}
}

func (s *SLOService) GetSloSli(teamUUID string, startMs, endMs int64, serviceName string) (*Response, error) {
	summary, err := s.repo.GetSummary(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}

	timeseries, err := s.repo.GetTimeSeries(teamUUID, startMs, endMs, serviceName)
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
