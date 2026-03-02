package deployments

import (
	"github.com/observability/observability-backend-go/internal/contracts"
)

// Service encapsulates the business logic for the deployments module.
type Service interface {
	GetDeployments(teamUUID string, startMs, endMs int64, serviceName, environment string, limit, offset int) ([]Deployment, int64, error)
	GetDeploymentEvents(teamUUID string, startMs, endMs int64, serviceName string) ([]DeploymentEvent, error)
	GetDeploymentDiff(teamUUID string, deployID string, windowMinutes int) (*DeploymentDiff, error)
	CreateDeployment(teamUUID string, deployID string, req contracts.DeploymentCreateRequest) error
}

// DeploymentService provides business logic orchestration for deployment tracking.
type DeploymentService struct {
	repo Repository
}

// NewService creates a new DeploymentService.
func NewService(repo Repository) Service {
	return &DeploymentService{repo: repo}
}

func (s *DeploymentService) GetDeployments(teamUUID string, startMs, endMs int64, serviceName, environment string, limit, offset int) ([]Deployment, int64, error) {
	return s.repo.GetDeployments(teamUUID, startMs, endMs, serviceName, environment, limit, offset)
}

func (s *DeploymentService) GetDeploymentEvents(teamUUID string, startMs, endMs int64, serviceName string) ([]DeploymentEvent, error) {
	return s.repo.GetDeploymentEvents(teamUUID, startMs, endMs, serviceName)
}

func (s *DeploymentService) GetDeploymentDiff(teamUUID string, deployID string, windowMinutes int) (*DeploymentDiff, error) {
	return s.repo.GetDeploymentDiff(teamUUID, deployID, windowMinutes)
}

func (s *DeploymentService) CreateDeployment(teamUUID string, deployID string, req contracts.DeploymentCreateRequest) error {
	return s.repo.CreateDeployment(teamUUID, deployID, req)
}
