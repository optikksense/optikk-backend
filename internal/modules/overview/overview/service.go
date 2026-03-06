package overview

// Service encapsulates the business logic for the overview module.
type Service interface {
	GetRequestRate(teamUUID string, startMs, endMs int64, serviceName string) ([]RequestRatePoint, error)
	GetErrorRate(teamUUID string, startMs, endMs int64, serviceName string) ([]ErrorRatePoint, error)
	GetP95Latency(teamUUID string, startMs, endMs int64, serviceName string) ([]P95LatencyPoint, error)
	GetServices(teamUUID string, startMs, endMs int64) ([]ServiceMetric, error)
	GetTopEndpoints(teamUUID string, startMs, endMs int64, serviceName string) ([]EndpointMetric, error)
	GetEndpointTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error)
}

// OverviewService provides business logic orchestration for overview dashboards.
type OverviewService struct {
	repo Repository
}

// NewService creates a new OverviewService.
func NewService(repo Repository) Service {
	return &OverviewService{repo: repo}
}

func (s *OverviewService) GetRequestRate(teamUUID string, startMs, endMs int64, serviceName string) ([]RequestRatePoint, error) {
	return s.repo.GetRequestRate(teamUUID, startMs, endMs, serviceName)
}

func (s *OverviewService) GetErrorRate(teamUUID string, startMs, endMs int64, serviceName string) ([]ErrorRatePoint, error) {
	return s.repo.GetErrorRate(teamUUID, startMs, endMs, serviceName)
}

func (s *OverviewService) GetP95Latency(teamUUID string, startMs, endMs int64, serviceName string) ([]P95LatencyPoint, error) {
	return s.repo.GetP95Latency(teamUUID, startMs, endMs, serviceName)
}

func (s *OverviewService) GetServices(teamUUID string, startMs, endMs int64) ([]ServiceMetric, error) {
	return s.repo.GetServices(teamUUID, startMs, endMs)
}

func (s *OverviewService) GetTopEndpoints(teamUUID string, startMs, endMs int64, serviceName string) ([]EndpointMetric, error) {
	return s.repo.GetTopEndpoints(teamUUID, startMs, endMs, serviceName)
}

func (s *OverviewService) GetEndpointTimeSeries(teamUUID string, startMs, endMs int64, serviceName string) ([]TimeSeriesPoint, error) {
	return s.repo.GetEndpointTimeSeries(teamUUID, startMs, endMs, serviceName)
}
