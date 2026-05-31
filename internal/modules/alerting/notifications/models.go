package notifications

import (
	"encoding/json"
	"time"

	models "github.com/Optikk-Org/optikk-backend/internal/modules/alerting/shared/models"
)

// ChannelResponse is the wire shape returned by channels endpoints.
type ChannelResponse struct {
	ID             int64           `json:"id"`
	Type           string          `json:"type"`
	Name           string          `json:"name"`
	Config         json.RawMessage `json:"config"`
	Status         string          `json:"status"`
	UsedByCount    int             `json:"used_by_count"`
	LastUsedAt     *time.Time      `json:"last_used_at,omitempty"`
	LastDeliveryAt *time.Time      `json:"last_delivery_at,omitempty"`
	LastErrorText  string          `json:"last_error_text,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

// PolicyResponse is the wire shape for policies.
type PolicyResponse struct {
	ID         int64           `json:"id"`
	Name       string          `json:"name"`
	MatchDSL   string          `json:"match_dsl"`
	Actions    json.RawMessage `json:"actions"`
	Hits30d    int             `json:"hits_30d"`
	LastUsedAt *time.Time      `json:"last_used_at,omitempty"`
	Enabled    bool            `json:"enabled"`
	Position   int             `json:"position"`
	CreatedAt  time.Time       `json:"created_at"`
}

// TemplateResponse is the wire shape for templates.
type TemplateResponse struct {
	ID          int64     `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Body        string    `json:"body"`
	UsedCount   int       `json:"used_count"`
	CreatedAt   time.Time `json:"created_at"`
}

// IntegrationCatalogEntry powers the integrations grid in the Notifications UI.
type IntegrationCatalogEntry struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Desc   string `json:"desc"`
	Status string `json:"status"` // connected | not_connected
	Count  int    `json:"count"`
	Color  string `json:"color"`
}

func toChannelResponse(row models.ChannelRow, usedBy int) ChannelResponse {
	out := ChannelResponse{
		ID:          row.ID,
		Type:        row.Type,
		Name:        row.Name,
		Config:      row.ConfigJSON,
		Status:      row.Status,
		UsedByCount: usedBy,
		CreatedAt:   row.CreatedAt,
	}
	if row.LastUsedAt.Valid {
		t := row.LastUsedAt.Time
		out.LastUsedAt = &t
	}
	if row.LastDeliveryAt.Valid {
		t := row.LastDeliveryAt.Time
		out.LastDeliveryAt = &t
	}
	if row.LastErrorText.Valid {
		out.LastErrorText = row.LastErrorText.String
	}
	return out
}

func toPolicyResponse(row models.PolicyRow) PolicyResponse {
	out := PolicyResponse{
		ID:        row.ID,
		Name:      row.Name,
		MatchDSL:  row.MatchDSL,
		Actions:   row.ActionsJSON,
		Hits30d:   row.Hits30d,
		Enabled:   row.Enabled,
		Position:  row.Position,
		CreatedAt: row.CreatedAt,
	}
	if row.LastUsedAt.Valid {
		t := row.LastUsedAt.Time
		out.LastUsedAt = &t
	}
	return out
}

func toTemplateResponse(row models.TemplateRow) TemplateResponse {
	out := TemplateResponse{
		ID:        row.ID,
		Name:      row.Name,
		Body:      row.Body,
		UsedCount: row.UsedCount,
		CreatedAt: row.CreatedAt,
	}
	if row.Description.Valid {
		out.Description = row.Description.String
	}
	return out
}
