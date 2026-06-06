// Package monitors handles CRUD, state actions, and history for monitors.
package monitors

import (
	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// CreateMonitorRequest is the wizard's save payload.
type CreateMonitorRequest struct {
	Name             string               `json:"name" binding:"required"`
	Type             string               `json:"type" binding:"required"`
	Priority         string               `json:"priority"`
	Scope            models.Scope         `json:"scope"`
	Query            models.MonitorQuery  `json:"query"`
	Conditions       models.Conditions    `json:"conditions"`
	Notify           models.NotifyTargets `json:"notify"`
	MessageBody      string               `json:"message_body,omitempty"`
	RunbookURL       string               `json:"runbook_url,omitempty"`
	Tags             []string             `json:"tags,omitempty"`
	EvalEverySec     int                  `json:"eval_every_sec"`
	RenotifyEverySec *int                 `json:"renotify_every_sec,omitempty"`
}

// UpdateMonitorRequest mirrors Create but is applied to an existing id.
type UpdateMonitorRequest = CreateMonitorRequest

// MuteRequest specifies the mute duration in seconds.
type MuteRequest struct {
	DurationSec int `json:"duration_sec" binding:"required"`
}

// ListQuery encapsulates the GET /monitors filters parsed from query string.
type ListQuery struct {
	// Status values: alert, warn, ok, no_data, or empty (all).
	Status   string
	// Type values: metric, apm, log, or empty (all).
	Type     string
	Priority string
	// Muted: nil for any, true for only muted, false for only not-muted.
	Muted    *bool
	Search   string
	Limit    int
	Offset   int
}
