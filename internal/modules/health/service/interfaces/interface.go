package interfaces

import (
	"github.com/observability/observability-backend-go/internal/contracts"
	"github.com/observability/observability-backend-go/internal/modules/health/model"
)

// Service encapsulates the business logic for the health module.
type Service interface {
	GetHealthChecks(teamID int64) ([]model.HealthCheck, error)
	GetHealthCheck(id int64) (model.HealthCheck, error)
	CreateHealthCheck(teamID, orgID int64, req contracts.HealthCheckRequest) (model.HealthCheck, error)
	UpdateHealthCheck(id int64, req contracts.HealthCheckRequest) (model.HealthCheck, error)
	DeleteHealthCheck(id int64) error
	ToggleHealthCheck(id int64) (model.HealthCheck, error)
	GetHealthCheckStatus(teamUUID string, startMs, endMs int64) ([]model.HealthCheckStatus, error)
	GetHealthCheckResults(teamUUID string, checkID string, startMs, endMs int64, limit, offset int) ([]model.HealthCheckResult, error)
	GetHealthCheckTrend(teamUUID string, checkID string, startMs, endMs int64) ([]model.HealthCheckTrend, error)
}
