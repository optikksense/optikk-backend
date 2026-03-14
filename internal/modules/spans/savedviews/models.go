package savedviews

import "time"

type SavedView struct {
	ID          int64     `json:"id"`
	TeamID      int64     `json:"teamId"`
	CreatedBy   int64     `json:"createdBy"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	ViewType    string    `json:"viewType"` // "trace_search" or "analytics"
	QueryJSON   string    `json:"queryJson"`
	IsShared    bool      `json:"isShared"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type CreateViewRequest struct {
	Name        string `json:"name" binding:"required"`
	Description string `json:"description"`
	ViewType    string `json:"viewType" binding:"required"`
	QueryJSON   string `json:"queryJson" binding:"required"`
	IsShared    bool   `json:"isShared"`
}

type UpdateViewRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	QueryJSON   *string `json:"queryJson"`
	IsShared    *bool   `json:"isShared"`
}
