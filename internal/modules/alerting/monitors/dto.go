// Package monitors owns CRUD + state actions + history/series readers for the
// alerting platform's monitor objects. The 6-file pattern is per-responsibility:
// each *.go file has one job. See the alerting module README in CODEBASE_INDEX.md.
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

// MuteRequest takes a duration in seconds (0 = until manually unmuted is
// not supported; pass a long duration instead).
type MuteRequest struct {
	DurationSec int `json:"duration_sec" binding:"required"`
}

// ListQuery encapsulates the GET /monitors filters parsed from the query string.
type ListQuery struct {
	Status   string // alert | warn | ok | no_data | "" (all)
	Type     string // metric | apm | log | "" (all)
	Priority string
	Muted    *bool // nil = any; true = only muted; false = only not-muted
	Search   string
	Limit    int
	Offset   int
}
