package servicemap

type Service interface {
	GetUpstreamDownstream(teamID int64, serviceName string, startMs, endMs int64) ([]ServiceDependencyDetail, error)
	GetExternalDependencies(teamID int64, startMs, endMs int64) ([]ExternalDependency, error)
	GetClientServerLatency(teamID int64, startMs, endMs int64, operationName string) ([]ClientServerLatencyPoint, error)
}

type ServiceMapService struct {
	repo Repository
}

func NewService(repo Repository) Service {
	return &ServiceMapService{repo: repo}
}

func (s *ServiceMapService) GetUpstreamDownstream(teamID int64, serviceName string, startMs, endMs int64) ([]ServiceDependencyDetail, error) {
	return s.repo.GetUpstreamDownstream(teamID, serviceName, startMs, endMs)
}

func (s *ServiceMapService) GetExternalDependencies(teamID int64, startMs, endMs int64) ([]ExternalDependency, error) {
	return s.repo.GetExternalDependencies(teamID, startMs, endMs)
}

func (s *ServiceMapService) GetClientServerLatency(teamID int64, startMs, endMs int64, operationName string) ([]ClientServerLatencyPoint, error) {
	return s.repo.GetClientServerLatency(teamID, startMs, endMs, operationName)
}
