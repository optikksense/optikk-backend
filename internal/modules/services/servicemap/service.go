package servicemap

import "context"

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
	rows, err := s.repo.GetUpstreamDownstream(context.Background(), teamID, serviceName, startMs, endMs)
	if err != nil {
		return nil, err
	}
	result := make([]ServiceDependencyDetail, len(rows))
	for i, r := range rows {
		direction := "downstream"
		if r.Target == serviceName {
			direction = "upstream"
		}
		result[i] = ServiceDependencyDetail{
			Source:       r.Source,
			Target:       r.Target,
			CallCount:    r.CallCount,
			P95LatencyMs: r.P95LatencyMs,
			ErrorRate:    r.ErrorRate,
			Direction:    direction,
		}
	}
	return result, nil
}

func (s *ServiceMapService) GetExternalDependencies(teamID int64, startMs, endMs int64) ([]ExternalDependency, error) {
	return s.repo.GetExternalDependencies(context.Background(), teamID, startMs, endMs)
}

func (s *ServiceMapService) GetClientServerLatency(teamID int64, startMs, endMs int64, operationName string) ([]ClientServerLatencyPoint, error) {
	rows, err := s.repo.GetClientServerLatency(context.Background(), teamID, startMs, endMs, operationName)
	if err != nil {
		return nil, err
	}
	result := make([]ClientServerLatencyPoint, len(rows))
	for i, r := range rows {
		result[i] = ClientServerLatencyPoint{
			Timestamp:     r.Timestamp,
			OperationName: r.OperationName,
			ClientP95Ms:   r.ClientP95Ms,
			ServerP95Ms:   r.ServerP95Ms,
			NetworkGapMs:  r.ClientP95Ms - r.ServerP95Ms,
		}
	}
	return result, nil
}
