package store

import (
	"github.com/observability/observability-backend-go/internal/contracts"
	"github.com/observability/observability-backend-go/internal/modules/alerts/model"
)

// Repository encapsulates data access logic for alerts.
type Repository interface {
	GetAlerts(teamID int64, status string) ([]model.Alert, error)
	GetAlertsPaged(teamID int64, limit, offset int) ([]model.Alert, int64, error)
	GetAlertByID(id int64) (*model.Alert, error)
	CreateAlert(orgID, teamID int64, req contracts.AlertRequest) (int64, error)
	MuteAlertWithReason(id int64, minutes int, reason string) error
	BulkMuteAlerts(ids []int64, minutes int, reason string) ([]model.Alert, error)
	BulkResolveAlerts(ids []int64, resolvedBy string) ([]model.Alert, error)
	GetAlertsForIncident(teamID int64, policyID string) ([]model.Alert, error)
	CountActiveAlerts(teamID int64) int64
	GetIncidents(teamID int64, startMs, endMs int64, statuses, severities, services []string, limit, offset int) ([]model.Incident, int64, map[string]int64, map[string]int64, error)
	UpdateAlertState(id int64, status string, acknowledge, resolve, mute bool, user string, muteMinutes int) error
}
