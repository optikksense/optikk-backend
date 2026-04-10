package httpmetrics

type Service interface {
	GetRequestRate(teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error)
	GetRequestDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetActiveRequests(teamID int64, startMs, endMs int64) ([]TimeBucket, error)
	GetRequestBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetResponseBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetClientDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetDNSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetTLSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error)
	GetTopRoutesByVolume(teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetTopRoutesByLatency(teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetRouteErrorRate(teamID int64, startMs, endMs int64) ([]RouteMetric, error)
	GetRouteErrorTimeseries(teamID int64, startMs, endMs int64) ([]RouteTimeseriesPoint, error)
	GetStatusDistribution(teamID int64, startMs, endMs int64) ([]StatusGroupBucket, error)
	GetErrorTimeseries(teamID int64, startMs, endMs int64) ([]ErrorTimeseriesPoint, error)
	GetTopExternalHosts(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
	GetExternalHostLatency(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
	GetExternalHostErrorRate(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error)
}

type HTTPMetricsService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &HTTPMetricsService{repo: repo}
}

func (s *HTTPMetricsService) GetRequestRate(teamID int64, startMs, endMs int64) ([]StatusCodeBucket, error) {
	return s.repo.GetRequestRate(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetRequestDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetRequestDuration(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetActiveRequests(teamID int64, startMs, endMs int64) ([]TimeBucket, error) {
	return s.repo.GetActiveRequests(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetRequestBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetRequestBodySize(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetResponseBodySize(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetResponseBodySize(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetClientDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetClientDuration(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetDNSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetDNSDuration(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetTLSDuration(teamID int64, startMs, endMs int64) (HistogramSummary, error) {
	return s.repo.GetTLSDuration(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetTopRoutesByVolume(teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	return s.repo.GetTopRoutesByVolume(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetTopRoutesByLatency(teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	return s.repo.GetTopRoutesByLatency(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetRouteErrorRate(teamID int64, startMs, endMs int64) ([]RouteMetric, error) {
	return s.repo.GetRouteErrorRate(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetRouteErrorTimeseries(teamID int64, startMs, endMs int64) ([]RouteTimeseriesPoint, error) {
	return s.repo.GetRouteErrorTimeseries(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetStatusDistribution(teamID int64, startMs, endMs int64) ([]StatusGroupBucket, error) {
	return s.repo.GetStatusDistribution(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetErrorTimeseries(teamID int64, startMs, endMs int64) ([]ErrorTimeseriesPoint, error) {
	return s.repo.GetErrorTimeseries(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetTopExternalHosts(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	return s.repo.GetTopExternalHosts(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetExternalHostLatency(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	return s.repo.GetExternalHostLatency(teamID, startMs, endMs)
}

func (s *HTTPMetricsService) GetExternalHostErrorRate(teamID int64, startMs, endMs int64) ([]ExternalHostMetric, error) {
	return s.repo.GetExternalHostErrorRate(teamID, startMs, endMs)
}
