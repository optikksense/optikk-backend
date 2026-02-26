package service

import (
	"github.com/observability/observability-backend-go/internal/contracts"
	"github.com/observability/observability-backend-go/internal/modules/alerts/model"
)

// Service encapsulates the business logic for the alerts module.
type Service interface {
	GetAlerts(teamID int64, status string) ([]model.Alert, error)
	GetAlertsPaged(teamID int64, page, size int) (*model.AlertPage, error)
	GetAlertByID(id int64) (*model.Alert, error)
	CreateAlert(orgID, teamID int64, req contracts.AlertRequest) (*model.Alert, error)
	AcknowledgeAlert(id int64, user string) (*model.Alert, error)
	ResolveAlert(id int64, user string) (*model.Alert, error)
	MuteAlert(id int64, minutes int, user string) (*model.Alert, error)
	MuteAlertWithReason(id int64, minutes int, reason string, user string) (*model.Alert, error)
	BulkMuteAlerts(ids []int64, minutes int, reason string) ([]model.Alert, error)
	BulkResolveAlerts(ids []int64, resolvedBy string) ([]model.Alert, error)
	GetAlertsForIncident(teamID int64, policyID string) ([]model.Alert, error)
	CountActiveAlerts(teamID int64) int64
	GetIncidents(teamID int64, startMs, endMs int64, statuses, severities, services []string, limit, offset int) (*model.IncidentsResponse, error)
}
