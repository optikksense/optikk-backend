// Package models holds shared row and wire types for the alerting platform.
package models

import (
	"database/sql"
	"time"
)

// MonitorRow is the raw MySQL row for observability.monitors.
type MonitorRow struct {
	ID                int64          `db:"id"`
	TeamID            int64          `db:"team_id"`
	Name              string         `db:"name"`
	Type              string         `db:"type"`
	Priority          string         `db:"priority"`
	ScopeJSON         []byte         `db:"scope_json"`
	QueryJSON         []byte         `db:"query_json"`
	ConditionsJSON    []byte         `db:"conditions_json"`
	NotifyJSON        []byte         `db:"notify_json"`
	MessageTemplateID sql.NullInt64  `db:"message_template_id"`
	MessageBody       sql.NullString `db:"message_body"`
	RunbookURL        sql.NullString `db:"runbook_url"`
	TagsJSON          []byte         `db:"tags_json"`
	EvalEverySec      int            `db:"eval_every_sec"`
	RenotifyEverySec  sql.NullInt64  `db:"renotify_every_sec"`
	MutedUntil        sql.NullTime   `db:"muted_until"`
	Active            bool           `db:"active"`
	CreatedAt         time.Time      `db:"created_at"`
	UpdatedAt         sql.NullTime   `db:"updated_at"`
	CreatedByUserID   sql.NullInt64  `db:"created_by_user_id"`
}

// MonitorStateRow is the raw row for observability.monitor_state.
type MonitorStateRow struct {
	MonitorID        int64           `db:"monitor_id"`
	Status           string          `db:"status"`
	CurrentValue     sql.NullFloat64 `db:"current_value"`
	LastEvaluatedAt  sql.NullTime    `db:"last_evaluated_at"`
	NextEvaluationAt time.Time       `db:"next_evaluation_at"`
	TriggeredAt      sql.NullTime    `db:"triggered_at"`
	LastNotifiedAt   sql.NullTime    `db:"last_notified_at"`
	EvaluationCount  int64           `db:"evaluation_count"`
	AckedByUserID    sql.NullInt64   `db:"acked_by_user_id"`
	AckedAt          sql.NullTime    `db:"acked_at"`
}

// MonitorEventRow is the raw row for observability.monitor_events.
type MonitorEventRow struct {
	ID         int64           `db:"id"`
	MonitorID  int64           `db:"monitor_id"`
	TeamID     int64           `db:"team_id"`
	Kind       string          `db:"kind"`
	Value      sql.NullFloat64 `db:"value"`
	Threshold  sql.NullFloat64 `db:"threshold"`
	StartedAt  time.Time       `db:"started_at"`
	EndedAt    sql.NullTime    `db:"ended_at"`
	ResolvedBy sql.NullString  `db:"resolved_by"`
	PeakValue  sql.NullFloat64 `db:"peak_value"`
	Note       sql.NullString  `db:"note"`
}

// Scope describes the resource filter the monitor evaluates against.
type Scope struct {
	Tags []ScopeTag `json:"tags,omitempty"`
}

// ScopeTag is one resource constraint, e.g. service:payment-svc.
type ScopeTag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Conditions are common to all monitor types.
type Conditions struct {
	// Comparator specifies the operator: above, below, or equal.
	Comparator        string   `json:"comparator"`
	AlertThreshold    *float64 `json:"alert_threshold,omitempty"`
	WarnThreshold     *float64 `json:"warn_threshold,omitempty"`
	RecoveryThreshold *float64 `json:"recovery_threshold,omitempty"`
	NoDataAfterSec    int      `json:"no_data_after_sec"`
	// NoDataAs is the state if data is missing: no_data, alert, or ok.
	NoDataAs  string `json:"no_data_as"`
	MinSample *int   `json:"min_sample,omitempty"`
}


