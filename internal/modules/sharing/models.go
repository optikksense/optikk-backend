package sharing

import "time"

type SharedLink struct {
	ID           int64      `json:"id"`
	TeamID       int64      `json:"teamId"`
	CreatedBy    int64      `json:"createdBy"`
	Token        string     `json:"token"`
	ResourceType string     `json:"resourceType"`
	ResourceID   string     `json:"resourceId"`
	ExpiresAt    *time.Time `json:"expiresAt,omitempty"`
	CreatedAt    time.Time  `json:"createdAt"`
}

type CreateShareRequest struct {
	ResourceType string `json:"resourceType" binding:"required"`
	ResourceID   string `json:"resourceId" binding:"required"`
	ExpiresInHours *int `json:"expiresInHours,omitempty"`
}

type ExportRequest struct {
	Columns []string         `json:"columns" binding:"required"`
	Data    []map[string]any `json:"data" binding:"required"`
}
