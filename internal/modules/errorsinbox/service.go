package errorsinbox

import (
	"fmt"
	"time"
)

var validStatuses = map[string]bool{
	"acknowledged": true,
	"resolved":     true,
	"ignored":      true,
	"snoozed":      true,
}

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetStatus(teamID int64, groupID string) (*ErrorGroupStatus, error) {
	return s.repo.GetByGroupID(teamID, groupID)
}

func (s *Service) ListByStatus(teamID int64, status string, limit int) ([]ErrorGroupStatus, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	return s.repo.ListByStatus(teamID, status, limit)
}

func (s *Service) UpdateStatus(teamID, userID int64, groupID string, req UpdateStatusRequest) (*ErrorGroupStatus, error) {
	if !validStatuses[req.Status] {
		return nil, fmt.Errorf("invalid status %q: must be one of acknowledged, resolved, ignored, snoozed", req.Status)
	}

	var snoozedUntil *time.Time
	if req.Status == "snoozed" {
		if req.SnoozedUntil == nil {
			return nil, fmt.Errorf("snoozedUntil is required when status is snoozed")
		}
		t := time.UnixMilli(*req.SnoozedUntil)
		if t.Before(time.Now()) {
			return nil, fmt.Errorf("snoozedUntil must be in the future")
		}
		snoozedUntil = &t
	}

	note := ""
	if req.Note != nil {
		note = *req.Note
	}

	return s.repo.Upsert(teamID, groupID, req.Status, req.AssignedTo, snoozedUntil, note, userID)
}

func (s *Service) BulkUpdate(teamID, userID int64, req BulkUpdateRequest) error {
	if !validStatuses[req.Status] {
		return fmt.Errorf("invalid status %q: must be one of acknowledged, resolved, ignored, snoozed", req.Status)
	}
	if len(req.GroupIDs) == 0 {
		return fmt.Errorf("groupIds must not be empty")
	}
	if len(req.GroupIDs) > 200 {
		return fmt.Errorf("bulk update limited to 200 groups at a time")
	}

	note := ""
	if req.Note != nil {
		note = *req.Note
	}

	return s.repo.BulkUpsert(teamID, req.GroupIDs, req.Status, req.AssignedTo, note, userID)
}

func (s *Service) GetSummary(teamID int64) (map[string]int64, error) {
	return s.repo.CountByStatus(teamID)
}
