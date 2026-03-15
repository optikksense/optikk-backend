package errorsinbox

import "time"

type ErrorGroupStatus struct {
	ID           int64      `json:"id"`
	TeamID       int64      `json:"teamId"`
	GroupID      string     `json:"groupId"`
	Status       string     `json:"status"`
	AssignedTo   *int64     `json:"assignedTo,omitempty"`
	SnoozedUntil *time.Time `json:"snoozedUntil,omitempty"`
	Note         string     `json:"note"`
	UpdatedBy    int64      `json:"updatedBy"`
	CreatedAt    time.Time  `json:"createdAt"`
	UpdatedAt    time.Time  `json:"updatedAt"`
}

type UpdateStatusRequest struct {
	Status       string  `json:"status" binding:"required"`
	AssignedTo   *int64  `json:"assignedTo"`
	SnoozedUntil *int64  `json:"snoozedUntil"` // ms timestamp
	Note         *string `json:"note"`
}

type BulkUpdateRequest struct {
	GroupIDs   []string `json:"groupIds" binding:"required"`
	Status     string   `json:"status" binding:"required"`
	AssignedTo *int64   `json:"assignedTo"`
	Note       *string  `json:"note"`
}
