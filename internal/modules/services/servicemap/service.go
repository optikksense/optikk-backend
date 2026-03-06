package servicemap

// Service encapsulates business logic for service map endpoints.
type Service interface {
	GetUpstreamDownstream(teamUUID, serviceName string, startMs, endMs int64) ([]ServiceDependencyDetail, error)
	GetExternalDependencies(teamUUID string, startMs, endMs int64) ([]ExternalDependency, error)
	GetClientServerLatency(teamUUID string, startMs, endMs int64, operationName string) ([]ClientServerLatencyPoint, error)
}

// ServiceMapService implements Service.
type ServiceMapService struct {
	repo Repository
}

// NewService creates a new service map service.
func NewService(repo Repository) Service {
	return &ServiceMapService{repo: repo}
}

func (s *ServiceMapService) GetUpstreamDownstream(teamUUID, serviceName string, startMs, endMs int64) ([]ServiceDependencyDetail, error) {
	return s.repo.GetUpstreamDownstream(teamUUID, serviceName, startMs, endMs)
}

func (s *ServiceMapService) GetExternalDependencies(teamUUID string, startMs, endMs int64) ([]ExternalDependency, error) {
	return s.repo.GetExternalDependencies(teamUUID, startMs, endMs)
}

func (s *ServiceMapService) GetClientServerLatency(teamUUID string, startMs, endMs int64, operationName string) ([]ClientServerLatencyPoint, error) {
	return s.repo.GetClientServerLatency(teamUUID, startMs, endMs, operationName)
}
