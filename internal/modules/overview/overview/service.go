package overview

type Service interface {
	GetRequestRate(teamID int64, startMs, endMs int64, serviceName string) ([]RequestRatePoint, error)
	GetErrorRate(teamID int64, startMs, endMs int64, serviceName string) ([]ErrorRatePoint, error)
	GetP95Latency(teamID int64, startMs, endMs int64, serviceName string) ([]P95LatencyPoint, error)
	GetServices(teamID int64, startMs, endMs int64) ([]ServiceMetric, error)
	GetTopEndpoints(teamID int64, startMs, endMs int64, serviceName string) ([]EndpointMetric, error)
	GetEndpointTimeSeries(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
}

type OverviewService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &OverviewService{repo: repo}
}

func (s *OverviewService) GetRequestRate(teamID int64, startMs, endMs int64, serviceName string) ([]RequestRatePoint, error) {
	return s.repo.GetRequestRate(teamID, startMs, endMs, serviceName)
}

func (s *OverviewService) GetErrorRate(teamID int64, startMs, endMs int64, serviceName string) ([]ErrorRatePoint, error) {
	return s.repo.GetErrorRate(teamID, startMs, endMs, serviceName)
}

func (s *OverviewService) GetP95Latency(teamID int64, startMs, endMs int64, serviceName string) ([]P95LatencyPoint, error) {
	return s.repo.GetP95Latency(teamID, startMs, endMs, serviceName)
}

func (s *OverviewService) GetServices(teamID int64, startMs, endMs int64) ([]ServiceMetric, error) {
	return s.repo.GetServices(teamID, startMs, endMs)
}

func (s *OverviewService) GetTopEndpoints(teamID int64, startMs, endMs int64, serviceName string) ([]EndpointMetric, error) {
	return s.repo.GetTopEndpoints(teamID, startMs, endMs, serviceName)
}

func (s *OverviewService) GetEndpointTimeSeries(teamID int64, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	return s.repo.GetEndpointTimeSeries(teamID, startMs, endMs, serviceName)
}
