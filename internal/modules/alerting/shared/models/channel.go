package models

import (
	"database/sql"
	"time"
)

// ChannelRow is the raw row for observability.notification_channels.
type ChannelRow struct {
	ID             int64          `db:"id"`
	TeamID         int64          `db:"team_id"`
	Type           string         `db:"type"`
	Name           string         `db:"name"`
	ConfigJSON     []byte         `db:"config_json"`
	Status         string         `db:"status"`
	LastUsedAt     sql.NullTime   `db:"last_used_at"`
	LastDeliveryAt sql.NullTime   `db:"last_delivery_at"`
	LastErrorText  sql.NullString `db:"last_error_text"`
	CreatedAt      time.Time      `db:"created_at"`
	UpdatedAt      sql.NullTime   `db:"updated_at"`
}

// PolicyRow is the raw row for observability.notification_policies.
type PolicyRow struct {
	ID          int64        `db:"id"`
	TeamID      int64        `db:"team_id"`
	Name        string       `db:"name"`
	MatchDSL    string       `db:"match_dsl"`
	ActionsJSON []byte       `db:"actions_json"`
	Hits30d     int          `db:"hits_30d"`
	LastUsedAt  sql.NullTime `db:"last_used_at"`
	Enabled     bool         `db:"enabled"`
	Position    int          `db:"position"`
	CreatedAt   time.Time    `db:"created_at"`
	UpdatedAt   sql.NullTime `db:"updated_at"`
}

// TemplateRow is the raw row for observability.notification_templates.
type TemplateRow struct {
	ID          int64          `db:"id"`
	TeamID      int64          `db:"team_id"`
	Name        string         `db:"name"`
	Description sql.NullString `db:"description"`
	Body        string         `db:"body"`
	UsedCount   int            `db:"used_count"`
	CreatedAt   time.Time      `db:"created_at"`
	UpdatedAt   sql.NullTime   `db:"updated_at"`
}

// SlackWebhookConfig is the config_json shape for slack-typed channels.
type SlackWebhookConfig struct {
	WebhookURL string `json:"webhook_url"`
}

// ChannelTypes lists the v1 channel types in display order. Only "slack" is
// wired end-to-end; the rest ship as Install stubs (no transport).
var ChannelTypes = []string{"slack", "pagerduty", "opsgenie", "teams", "email", "webhook", "jira"}

// IsValidChannelType reports whether t is one of the channel types we persist.
func IsValidChannelType(t string) bool {
	for _, v := range ChannelTypes {
		if v == t {
			return true
		}
	}
	return false
}
