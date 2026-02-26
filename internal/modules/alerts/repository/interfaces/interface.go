package interfaces

import (
	"github.com/observability/observability-backend-go/internal/contracts"
)

// Repository describes alerts data-access operations.
type Repository interface {
	GetAlerts(teamID int64, status string) ([]map[string]any, error)
	GetAlertsPaged(teamID int64, limit, offset int) ([]map[string]any, int64, error)
	GetAlertByID(id int64) (map[string]any, error)
	CreateAlert(orgID, teamID int64, req contracts.AlertRequest) (int64, error)
	MuteAlertWithReason(id int64, minutes int, reason string) error
	BulkMuteAlerts(ids []int64, minutes int, reason string) ([]map[string]any, error)
	BulkResolveAlerts(ids []int64, resolvedBy string) ([]map[string]any, error)
	GetAlertsForIncident(teamID int64, policyID string) ([]map[string]any, error)
	CountActiveAlerts(teamID int64) int64
	GetIncidents(teamID int64, startMs, endMs int64, statuses, severities, services []string, limit, offset int) ([]map[string]any, int64, map[string]int64, map[string]int64, error)
	UpdateAlertState(id int64, status string, acknowledge, resolve, mute bool, user string, muteMinutes int) error
}
