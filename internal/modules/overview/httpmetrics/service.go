package httpmetrics

import "context"

type Service interface {
	GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error)
	GetRequestDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetActiveRequests(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error)
	GetRequestBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetResponseBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetClientDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetDNSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetTLSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetTopRoutesByVolume(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetTopRoutesByLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetRouteErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetRouteErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteTimeseriesPoint, error)
	GetStatusDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusGroupBucket, error)
	GetErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorTimeseriesPoint, error)
	GetTopExternalHosts(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
	GetExternalHostLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
	GetExternalHostErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
}

type HTTPMetricsService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &HTTPMetricsService{repo: repo}
}

func (s *HTTPMetricsService) GetRequestRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error) {
	return s.repo.GetRequestRate(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetRequestDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetRequestDuration(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetActiveRequests(ctx context.Context, teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	return s.repo.GetActiveRequests(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetRequestBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetRequestBodySize(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetResponseBodySize(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetResponseBodySize(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetClientDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetClientDuration(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetDNSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetDNSDuration(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetTLSDuration(ctx context.Context, teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetTLSDuration(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetTopRoutesByVolume(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	return s.repo.GetTopRoutesByVolume(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetTopRoutesByLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	return s.repo.GetTopRoutesByLatency(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetRouteErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	return s.repo.GetRouteErrorRate(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetRouteErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]RouteTimeseriesPoint, error) {
	return s.repo.GetRouteErrorTimeseries(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetStatusDistribution(ctx context.Context, teamID int64, startMs, endMs int64) ([]StatusGroupBucket, error) {
	return s.repo.GetStatusDistribution(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetErrorTimeseries(ctx context.Context, teamID int64, startMs, endMs int64) ([]ErrorTimeseriesPoint, error) {
	return s.repo.GetErrorTimeseries(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetTopExternalHosts(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	return s.repo.GetTopExternalHosts(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetExternalHostLatency(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	return s.repo.GetExternalHostLatency(ctx, teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetExternalHostErrorRate(ctx context.Context, teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	return s.repo.GetExternalHostErrorRate(ctx, teamID, startMs, endMs)
}
