package service

import (
	"github.com/observability/observability-backend-go/internal/contracts"
	"github.com/observability/observability-backend-go/internal/modules/deployments/model"
)

// Service encapsulates the business logic for the deployments module.
type Service interface {
	GetDeployments(teamUUID string, startMs, endMs int64, serviceName, environment string, limit, offset int) ([]model.Deployment, int64, error)
	GetDeploymentEvents(teamUUID string, startMs, endMs int64, serviceName string) ([]model.DeploymentEvent, error)
	GetDeploymentDiff(teamUUID string, deployID string, windowMinutes int) (*model.DeploymentDiff, error)
	CreateDeployment(teamUUID string, deployID string, req contracts.DeploymentCreateRequest) error
}
