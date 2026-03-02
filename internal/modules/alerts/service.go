package alerts

import (
	"strings"

	"github.com/observability/observability-backend-go/internal/contracts"
)

// Service encapsulates the business logic for the alerts module.
type Service interface {
	GetAlerts(teamID int64, status string) ([]Alert, error)
	GetAlertsPaged(teamID int64, page, size int) (*AlertPage, error)
	GetAlertByID(id int64) (*Alert, error)
	CreateAlert(orgID, teamID int64, req contracts.AlertRequest) (*Alert, error)
	AcknowledgeAlert(id int64, user string) (*Alert, error)
	ResolveAlert(id int64, user string) (*Alert, error)
	MuteAlert(id int64, minutes int, user string) (*Alert, error)
	MuteAlertWithReason(id int64, minutes int, reason string, user string) (*Alert, error)
	BulkMuteAlerts(ids []int64, minutes int, reason string) ([]Alert, error)
	BulkResolveAlerts(ids []int64, resolvedBy string) ([]Alert, error)
	GetAlertsForIncident(teamID int64, policyID string) ([]Alert, error)
	CountActiveAlerts(teamID int64) int64
	GetIncidents(teamID int64, startMs, endMs int64, statuses, severities, services []string, limit, offset int) (*IncidentsResponse, error)
}

// AlertService provides business logic orchestration for alerts.
type AlertService struct {
	repo Repository
}

// NewService creates a new AlertService.
func NewService(repo Repository) *AlertService {
	return &AlertService{repo: repo}
}

func (s *AlertService) GetAlerts(teamID int64, status string) ([]Alert, error) {
	return s.repo.GetAlerts(teamID, status)
}

func (s *AlertService) GetAlertsPaged(teamID int64, page, size int) (*AlertPage, error) {
	if size <= 0 {
		size = 20
	}
	offset := page * size
	rows, total, err := s.repo.GetAlertsPaged(teamID, size, offset)
	if err != nil {
		return nil, err
	}

	totalPages := int((total + int64(size) - 1) / int64(size))
	return &AlertPage{
		Content: rows,
		Pageable: Pageable{
			PageNumber: page,
			PageSize:   size,
			Offset:     offset,
		},
		TotalElements:    total,
		TotalPages:       totalPages,
		Size:             size,
		Number:           page,
		First:            page == 0,
		Last:             page+1 >= totalPages,
		NumberOfElements: len(rows),
	}, nil
}

func (s *AlertService) GetAlertByID(id int64) (*Alert, error) {
	return s.repo.GetAlertByID(id)
}

func (s *AlertService) CreateAlert(orgID, teamID int64, req contracts.AlertRequest) (*Alert, error) {
	id, err := s.repo.CreateAlert(orgID, teamID, req)
	if err != nil {
		return nil, err
	}
	return s.repo.GetAlertByID(id)
}

func (s *AlertService) AcknowledgeAlert(id int64, user string) (*Alert, error) {
	if err := s.repo.UpdateAlertState(id, "acknowledged", true, false, false, user, 0); err != nil {
		return nil, err
	}
	return s.repo.GetAlertByID(id)
}

func (s *AlertService) ResolveAlert(id int64, user string) (*Alert, error) {
	if err := s.repo.UpdateAlertState(id, "resolved", false, true, false, user, 0); err != nil {
		return nil, err
	}
	return s.repo.GetAlertByID(id)
}

func (s *AlertService) MuteAlert(id int64, minutes int, user string) (*Alert, error) {
	if err := s.repo.UpdateAlertState(id, "muted", false, false, true, user, minutes); err != nil {
		return nil, err
	}
	return s.repo.GetAlertByID(id)
}

func (s *AlertService) MuteAlertWithReason(id int64, minutes int, reason string, user string) (*Alert, error) {
	if err := s.repo.MuteAlertWithReason(id, minutes, reason); err != nil {
		return nil, err
	}
	return s.repo.GetAlertByID(id)
}

func (s *AlertService) BulkMuteAlerts(ids []int64, minutes int, reason string) ([]Alert, error) {
	return s.repo.BulkMuteAlerts(ids, minutes, reason)
}

func (s *AlertService) BulkResolveAlerts(ids []int64, resolvedBy string) ([]Alert, error) {
	return s.repo.BulkResolveAlerts(ids, resolvedBy)
}

func (s *AlertService) GetAlertsForIncident(teamID int64, policyID string) ([]Alert, error) {
	return s.repo.GetAlertsForIncident(teamID, policyID)
}

func (s *AlertService) CountActiveAlerts(teamID int64) int64 {
	return s.repo.CountActiveAlerts(teamID)
}

func alertStatusesFromIncidentStatuses(statuses []string) []string {
	if len(statuses) == 0 {
		return nil
	}
	set := map[string]struct{}{}
	for _, raw := range statuses {
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "open":
			set["active"] = struct{}{}
		case "investigating", "identified":
			set["acknowledged"] = struct{}{}
		case "monitoring":
			set["muted"] = struct{}{}
		case "resolved":
			set["resolved"] = struct{}{}
		case "active", "acknowledged", "muted":
			set[strings.ToLower(strings.TrimSpace(raw))] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	return out
}

func (s *AlertService) GetIncidents(teamID int64, startMs, endMs int64, statuses, severities, services []string, limit, offset int) (*IncidentsResponse, error) {
	alertStatuses := alertStatusesFromIncidentStatuses(statuses)

	incidents, total, statusCounts, severityCounts, err := s.repo.GetIncidents(teamID, startMs, endMs, alertStatuses, severities, services, limit, offset)
	if err != nil {
		return nil, err
	}

	return &IncidentsResponse{
		Incidents: incidents,
		HasMore:   len(incidents) >= limit,
		Offset:    offset,
		Limit:     limit,
		Total:     total,
		Counts: IncidentCounts{
			ByStatus:   statusCounts,
			BySeverity: severityCounts,
		},
	}, nil
}
