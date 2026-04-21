package overview

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]RequestRatePoint, error) {
	rows, err := s.repo.GetRequestRate(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return mapRequestRateRows(rows), nil
}

func (s *Service) GetErrorRate(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]ErrorRatePoint, error) {
	rows, err := s.repo.GetErrorRate(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return mapErrorRateRows(rows), nil
}

func (s *Service) GetP95Latency(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]P95LatencyPoint, error) {
	rows, err := s.repo.GetP95Latency(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return mapP95LatencyRows(rows), nil
}

func (s *Service) GetChartMetrics(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]ChartMetricsPoint, error) {
	rows, err := s.repo.GetChartMetrics(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return mapChartMetricsRows(rows), nil
}

func (s *Service) GetServices(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceMetric, error) {
	rows, err := s.repo.GetServices(ctx, teamID, startMs, endMs)
	if err != nil {
		return nil, err
	}
	return mapServiceMetricRows(rows), nil
}

func (s *Service) GetTopEndpoints(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]EndpointMetric, error) {
	rows, err := s.repo.GetTopEndpoints(ctx, teamID, startMs, endMs, serviceName)
	if err != nil {
		return nil, err
	}
	return mapEndpointMetricRows(rows), nil
}

func (s *Service) GetSummary(ctx context.Context, teamID int64, startMs, endMs int64) (GlobalSummary, error) {
	row, err := s.repo.GetSummary(ctx, teamID, startMs, endMs)
	if err != nil {
		return GlobalSummary{}, err
	}
	return mapGlobalSummaryRow(row), nil
}

// GetBatchSummary runs the above-fold Summary-tab queries (global summary +
// per-service metrics) concurrently and returns them in one payload. Collapses
// 2 HTTP round-trips into 1; server-side fan-out via errgroup means response
// time is bounded by max(child) instead of sum(child). The timeseries queries
// stay separate so the frontend can defer them until charts scroll into view.
func (s *Service) GetBatchSummary(ctx context.Context, teamID int64, startMs, endMs int64) (BatchSummaryResponse, error) {
	g, gctx := errgroup.WithContext(ctx)

	var (
		summary  GlobalSummary
		services []ServiceMetric
	)

	g.Go(func() error {
		v, err := s.GetSummary(gctx, teamID, startMs, endMs)
		summary = v
		return err
	})
	g.Go(func() error {
		v, err := s.GetServices(gctx, teamID, startMs, endMs)
		services = v
		return err
	})

	if err := g.Wait(); err != nil {
		return BatchSummaryResponse{}, err
	}

	return BatchSummaryResponse{
		Summary:  summary,
		Services: services,
	}, nil
}
