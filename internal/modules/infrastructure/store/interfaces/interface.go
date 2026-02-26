package interfaces

import "github.com/observability/observability-backend-go/internal/modules/infrastructure/model"

// Repository encapsulates data access logic for infrastructure tracking.
type Repository interface {
	GetInfrastructure(teamUUID string, startMs, endMs int64) ([]model.InfrastructureSummary, error)
	GetInfrastructureNodes(teamUUID string, startMs, endMs int64) ([]model.InfrastructureNode, error)
	GetInfrastructureNodeServices(teamUUID, host string, startMs, endMs int64) ([]model.InfrastructureNodeService, error)
}
