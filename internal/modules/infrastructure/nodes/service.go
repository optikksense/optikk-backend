package nodes

import (
	"context"
	"log/slog"
	"time"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetInfrastructureNodes(ctx context.Context, teamID int64, startMs, endMs int64) ([]InfrastructureNode, error) {
	rows, err := s.repo.QueryInfrastructureNodes(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]InfrastructureNode, len(rows))
	for i, r := range rows {
		errorRate, avgLatency := redDerivations(r.RequestCount, r.ErrorCount, r.DurationMsSum)
		out[i] = InfrastructureNode{
			Host:     r.Host,
			PodCount: int64(r.PodCount),
			// Container count is not derived from spans.
			ContainerCount: 0,
			// Services are omitted from fleet view for performance.
			Services:     []string{},
			RequestCount: int64(r.RequestCount),
			ErrorCount:   int64(r.ErrorCount),
			ErrorRate:    errorRate,
			AvgLatencyMs: avgLatency,
			P95LatencyMs: float64(r.P95LatencyMs),
			LastSeen:     r.LastSeen.Format(time.RFC3339),
		}
	}
	return out, nil
}

func (s *Service) GetInfrastructureNodeSummary(ctx context.Context, teamID int64, startMs, endMs int64) (InfrastructureNodeSummary, error) {
	row, err := s.repo.QueryInfrastructureNodeSummary(ctx, teamID, startMs, endMs)
	if err != nil {
		slog.ErrorContext(ctx, "nodes: GetInfrastructureNodeSummary failed", slog.Any("error", err), slog.Int64("team_id", teamID))
		return InfrastructureNodeSummary{}, err
	}
	var totalPods int64
	if row.TotalPods != nil {
		totalPods = int64(*row.TotalPods)
	}
	return InfrastructureNodeSummary{
		HealthyNodes:   int64(row.HealthyNodes),
		DegradedNodes:  int64(row.DegradedNodes),
		UnhealthyNodes: int64(row.UnhealthyNodes),
		TotalPods:      totalPods,
	}, nil
}

func (s *Service) GetInfrastructureNodeServices(ctx context.Context, teamID int64, host string, startMs, endMs int64) ([]InfrastructureNodeService, error) {
	rows, err := s.repo.QueryInfrastructureNodeServices(ctx, teamID, host, startMs, endMs)
	if err != nil {
		return nil, err
	}
	out := make([]InfrastructureNodeService, len(rows))
	for i, r := range rows {
		errorRate, avgLatency := redDerivations(r.RequestCount, r.ErrorCount, r.DurationMsSum)
		out[i] = InfrastructureNodeService{
			ServiceName:  r.Service,
			RequestCount: int64(r.RequestCount),
			ErrorCount:   int64(r.ErrorCount),
			ErrorRate:    errorRate,
			AvgLatencyMs: avgLatency,
			P95LatencyMs: float64(r.P95LatencyMs),
			PodCount:     int64(r.PodCount),
		}
	}
	return out, nil
}

// redDerivations computes (error_rate %, avg_latency_ms) from raw aggregates.
func redDerivations(reqCount, errCount uint64, durationMsSum float64) (errorRate, avgLatency float64) {
	if reqCount == 0 {
		return 0, 0
	}
	rc := float64(reqCount)
	return float64(errCount) * 100.0 / rc, durationMsSum / rc
}
