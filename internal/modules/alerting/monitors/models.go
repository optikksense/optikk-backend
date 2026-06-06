package monitors

import (
	"encoding/json"
	"time"

	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// MonitorResponse is the wire shape returned by GET / list / create / update,
// reflecting the most recent evaluator decision.
type MonitorResponse struct {
	ID               int64                `json:"id"`
	Name             string               `json:"name"`
	Type             string               `json:"type"`
	Priority         string               `json:"priority"`
	Status           string               `json:"status"`
	CurrentValue     *float64             `json:"current_value,omitempty"`
	Scope            models.Scope         `json:"scope"`
	Query            models.MonitorQuery  `json:"query"`
	Conditions       models.Conditions    `json:"conditions"`
	Notify           models.NotifyTargets `json:"notify"`
	MessageBody      string               `json:"message_body,omitempty"`
	RunbookURL       string               `json:"runbook_url,omitempty"`
	Tags             []string             `json:"tags"`
	EvalEverySec     int                  `json:"eval_every_sec"`
	RenotifyEverySec *int                 `json:"renotify_every_sec,omitempty"`
	MutedUntil       *time.Time           `json:"muted_until,omitempty"`
	Active           bool                 `json:"active"`
	LastEvaluatedAt  *time.Time           `json:"last_evaluated_at,omitempty"`
	TriggeredAt      *time.Time           `json:"triggered_at,omitempty"`
	CreatedAt        time.Time            `json:"created_at"`
	UpdatedAt        *time.Time           `json:"updated_at,omitempty"`
}

// MonitorListResponse pairs a page of monitors with counts by status.
type MonitorListResponse struct {
	Items  []MonitorResponse `json:"items"`
	Counts StatusCounts      `json:"counts"`
}

// StatusCounts powers the list-page KPI strip.
type StatusCounts struct {
	Alert  int `json:"alert"`
	Warn   int `json:"warn"`
	OK     int `json:"ok"`
	NoData int `json:"no_data"`
	Muted  int `json:"muted"`
	Total  int `json:"total"`
}

// toResponse merges a monitors row + state row into the wire shape.
// State may be zero-value if no evaluation has happened yet.
func toResponse(row models.MonitorRow, state models.MonitorStateRow) MonitorResponse {
	out := MonitorResponse{
		ID:           row.ID,
		Name:         row.Name,
		Type:         row.Type,
		Priority:     row.Priority,
		EvalEverySec: row.EvalEverySec,
		Active:       row.Active,
		CreatedAt:    row.CreatedAt,
		Status:       "no_data",
		Tags:         []string{},
	}
	_ = json.Unmarshal(row.ScopeJSON, &out.Scope)
	_ = json.Unmarshal(row.QueryJSON, &out.Query)
	_ = json.Unmarshal(row.ConditionsJSON, &out.Conditions)
	_ = json.Unmarshal(row.NotifyJSON, &out.Notify)
	if len(row.TagsJSON) > 0 {
		_ = json.Unmarshal(row.TagsJSON, &out.Tags)
	}
	if row.MessageBody.Valid {
		out.MessageBody = row.MessageBody.String
	}
	if row.RunbookURL.Valid {
		out.RunbookURL = row.RunbookURL.String
	}
	if row.RenotifyEverySec.Valid {
		v := int(row.RenotifyEverySec.Int64)
		out.RenotifyEverySec = &v
	}
	if row.MutedUntil.Valid {
		t := row.MutedUntil.Time
		out.MutedUntil = &t
	}
	if row.UpdatedAt.Valid {
		t := row.UpdatedAt.Time
		out.UpdatedAt = &t
	}
	if state.MonitorID != 0 {
		out.Status = state.Status
		if state.CurrentValue.Valid {
			v := state.CurrentValue.Float64
			out.CurrentValue = &v
		}
		if state.LastEvaluatedAt.Valid {
			t := state.LastEvaluatedAt.Time
			out.LastEvaluatedAt = &t
		}
		if state.TriggeredAt.Valid {
			t := state.TriggeredAt.Time
			out.TriggeredAt = &t
		}
	}
	return out
}
