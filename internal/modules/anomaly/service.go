package anomaly

import "fmt"

// Service defines the business-logic contract for anomaly detection.
type Service interface {
	GetBaseline(teamID, startMs, endMs int64, req BaselineRequest) ([]BaselinePoint, error)
}

// AnomalyService implements Service.
type AnomalyService struct {
	repo Repository
}

// NewService returns a production AnomalyService.
func NewService(repo Repository) Service {
	return &AnomalyService{repo: repo}
}

var validMetrics = map[string]bool{
	"error_rate":   true,
	"latency_p95":  true,
	"request_rate": true,
}

func (s *AnomalyService) GetBaseline(teamID, startMs, endMs int64, req BaselineRequest) ([]BaselinePoint, error) {
	if !validMetrics[req.Metric] {
		return nil, fmt.Errorf("unsupported metric: %s", req.Metric)
	}
	if req.Sensitivity <= 0 {
		req.Sensitivity = 2.0
	}
	return s.repo.GetBaseline(teamID, startMs, endMs, req)
}
