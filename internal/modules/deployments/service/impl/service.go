package impl

import (
	"github.com/observability/observability-backend-go/internal/contracts"
	"github.com/observability/observability-backend-go/internal/modules/deployments/model"
	"github.com/observability/observability-backend-go/internal/modules/deployments/store"
)

// DeploymentService provides business logic orchestration for deployment tracking.
type DeploymentService struct {
	repo store.Repository
}

// NewService creates a new DeploymentService.
func NewService(repo store.Repository) *DeploymentService {
	return &DeploymentService{repo: repo}
}

func (s *DeploymentService) GetDeployments(teamUUID string, startMs, endMs int64, serviceName, environment string, limit, offset int) ([]model.Deployment, int64, error) {
	return s.repo.GetDeployments(teamUUID, startMs, endMs, serviceName, environment, limit, offset)
}

func (s *DeploymentService) GetDeploymentEvents(teamUUID string, startMs, endMs int64, serviceName string) ([]model.DeploymentEvent, error) {
	return s.repo.GetDeploymentEvents(teamUUID, startMs, endMs, serviceName)
}

func (s *DeploymentService) GetDeploymentDiff(teamUUID string, deployID string, windowMinutes int) (*model.DeploymentDiff, error) {
	return s.repo.GetDeploymentDiff(teamUUID, deployID, windowMinutes)
}

func (s *DeploymentService) CreateDeployment(teamUUID string, deployID string, req contracts.DeploymentCreateRequest) error {
	return s.repo.CreateDeployment(teamUUID, deployID, req)
}
