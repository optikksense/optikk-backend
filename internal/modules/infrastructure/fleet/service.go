package fleet

import (
	"context"
	"time"
)

type Service interface {
	GetFleetPods(ctx context.Context, teamID int64, startMs, endMs int64) ([]FleetPod, error)
}

type fleetService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &fleetService{repo: repo}
}

func (s *fleetService) GetFleetPods(ctx context.Context, teamID int64, startMs, endMs int64) ([]FleetPod, error) {
	rows, err := s.repo.QueryFleetPods(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]FleetPod, len(rows))
	for i, r := range rows {
		errorRate, avgLatency := redDerivations(r.RequestCount, r.ErrorCount, r.DurationMsSum)
		services := r.Services
		if services == nil {
			services = []string{}
		}
		out[i] = FleetPod{
			PodName:      r.Pod,
			Host:         r.Host,
			Services:     services,
			RequestCount: int64(r.RequestCount), //nolint:gosec
			ErrorCount:   int64(r.ErrorCount),
			ErrorRate:    errorRate,
			AvgLatencyMs: avgLatency,
			P95LatencyMs: float64(r.P95LatencyMs),
			LastSeen:     r.LastSeen.Format(time.RFC3339),
		}
	}
	return out, nil
}

func redDerivations(reqCount, errCount uint64, durationMsSum float64) (errorRate, avgLatency float64) {
	if reqCount == 0 {
		return 0, 0
	}
	rc := float64(reqCount)
	return float64(errCount) * 100.0 / rc, durationMsSum / rc
}
