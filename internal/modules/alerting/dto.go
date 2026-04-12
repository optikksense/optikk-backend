package alerting

import "time"

// CreateRuleRequest is the POST /alerts/rules body.
type CreateRuleRequest struct {
	Name        string             `json:"name" binding:"required"`
	Description string             `json:"description"`
	PresetKind  string             `json:"preset_kind" binding:"required"`
	Scope       AlertRuleScope     `json:"scope" binding:"required"`
	Condition   AlertRuleCondition `json:"condition" binding:"required"`
	Delivery    AlertRuleDelivery  `json:"delivery" binding:"required"`
	Enabled     bool               `json:"enabled"`
}

// UpdateRuleRequest is the PATCH body — top-level pointers so we can detect presence.
type UpdateRuleRequest struct {
	Name        *string             `json:"name,omitempty"`
	Description *string             `json:"description,omitempty"`
	PresetKind  *string             `json:"preset_kind,omitempty"`
	Scope       *AlertRuleScope     `json:"scope,omitempty"`
	Condition   *AlertRuleCondition `json:"condition,omitempty"`
	Delivery    *AlertRuleDelivery  `json:"delivery,omitempty"`
	Enabled     *bool               `json:"enabled,omitempty"`
}

// RuleResponse is the canonical wire shape for a rule.
type RuleResponse struct {
	ID          string             `json:"id"`
	TeamID      int64              `json:"team_id"`
	Name        string             `json:"name"`
	Description string             `json:"description,omitempty"`
	PresetKind  string             `json:"preset_kind"`
	Scope       AlertRuleScope     `json:"scope"`
	Condition   AlertRuleCondition `json:"condition"`
	Delivery    AlertRuleDelivery  `json:"delivery"`
	Summary     string             `json:"summary"`
	RuleState   string             `json:"rule_state"`
	LastEvalAt  *time.Time         `json:"last_eval_at,omitempty"`
	Instances   []*Instance        `json:"instances"`
	MuteUntil   *time.Time         `json:"mute_until,omitempty"`
	Silences    []Silence          `json:"silences,omitempty"`
	Enabled     bool               `json:"enabled"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
}

// PreviewRuleResponse is the preview response for unsaved or edited rules.
type PreviewRuleResponse struct {
	Summary      string              `json:"summary"`
	Engine       RuleEnginePreview   `json:"engine"`
	Notification NotificationPreview `json:"notification"`
}

// IncidentResponse is one firing instance in GET /alerts/incidents.
type IncidentResponse struct {
	AlertID     string             `json:"alert_id"`
	RuleName    string             `json:"rule_name"`
	PresetKind  string             `json:"preset_kind"`
	Summary     string             `json:"summary"`
	Severity    string             `json:"severity"`
	InstanceKey string             `json:"instance_key"`
	GroupValues map[string]string  `json:"group_values,omitempty"`
	State       string             `json:"state"`
	FiredAt     *time.Time         `json:"fired_at,omitempty"`
	Values      map[string]float64 `json:"values,omitempty"`
}

// ActivityEntry is one recent alerting event enriched with rule metadata.
type ActivityEntry struct {
	Ts          time.Time          `json:"ts"`
	AlertID     string             `json:"alert_id"`
	RuleName    string             `json:"rule_name"`
	PresetKind  string             `json:"preset_kind"`
	Summary     string             `json:"summary"`
	Kind        string             `json:"kind"`
	FromState   string             `json:"from_state,omitempty"`
	ToState     string             `json:"to_state,omitempty"`
	InstanceKey string             `json:"instance_key,omitempty"`
	ActorUserID int64              `json:"actor_user_id,omitempty"`
	Message     string             `json:"message,omitempty"`
	Values      map[string]float64 `json:"values,omitempty"`
}

// SlackTestRequest / SlackTestResponse power POST /alerts/slack/test.
type SlackTestRequest struct {
	Rule AlertRuleDefinition `json:"rule" binding:"required"`
}

type SlackTestResponse struct {
	Delivered    bool                `json:"delivered"`
	Error        string              `json:"error,omitempty"`
	Notification NotificationPreview `json:"notification"`
}

// MuteRuleRequest is POST /alerts/rules/:id/mute.
type MuteRuleRequest struct {
	Until  time.Time `json:"until"`
	Reason string    `json:"reason,omitempty"`
}

// AckInstanceRequest is POST /alerts/instances/:id/ack.
type AckInstanceRequest struct {
	InstanceKey string     `json:"instance_key"`
	Until       *time.Time `json:"until,omitempty"`
	Comment     string     `json:"comment,omitempty"`
}

// SnoozeInstanceRequest is POST /alerts/instances/:id/snooze.
type SnoozeInstanceRequest struct {
	InstanceKey string `json:"instance_key"`
	Minutes     int    `json:"minutes"`
}

// TestRuleResponse is POST /alerts/rules/:id/test — dry-run current state.
type TestRuleResponse struct {
	Results     []TestInstanceResult `json:"results"`
	WouldFire   bool                 `json:"would_fire"`
	EvaluatedAt time.Time            `json:"evaluated_at"`
}

type TestInstanceResult struct {
	InstanceKey string             `json:"instance_key"`
	GroupValues map[string]string  `json:"group_values,omitempty"`
	Values      map[string]float64 `json:"values"`
	NoData      bool               `json:"no_data"`
	WouldFire   bool               `json:"would_fire"`
}

// BacktestRequest is POST /alerts/rules/:id/backtest body.
type BacktestRequest struct {
	FromMs int64 `json:"from_ms"`
	ToMs   int64 `json:"to_ms"`
	StepMs int64 `json:"step_ms"`
}

// BacktestResponse is the synthetic timeline returned by backtest.
type BacktestResponse struct {
	Transitions []BacktestTransition `json:"transitions"`
	Ticks       int                  `json:"ticks"`
}

type BacktestTransition struct {
	Ts          time.Time          `json:"ts"`
	InstanceKey string             `json:"instance_key"`
	FromState   string             `json:"from_state"`
	ToState     string             `json:"to_state"`
	Values      map[string]float64 `json:"values,omitempty"`
}

// CreateSilenceRequest / UpdateSilenceRequest are the silence CRUD bodies.
type CreateSilenceRequest struct {
	StartsAt   time.Time         `json:"starts_at"`
	EndsAt     time.Time         `json:"ends_at"`
	Recurrence string            `json:"recurrence,omitempty"`
	MatchTags  map[string]string `json:"match_tags,omitempty"`
	Reason     string            `json:"reason,omitempty"`
	AlertID    int64             `json:"alert_id"`
}

type UpdateSilenceRequest struct {
	StartsAt   *time.Time         `json:"starts_at,omitempty"`
	EndsAt     *time.Time         `json:"ends_at,omitempty"`
	Recurrence *string            `json:"recurrence,omitempty"`
	MatchTags  *map[string]string `json:"match_tags,omitempty"`
	Reason     *string            `json:"reason,omitempty"`
}

// AuditEntry is one row in GET /alerts/rules/:id/audit.
type AuditEntry struct {
	Ts          time.Time `json:"ts"`
	Kind        string    `json:"kind"`
	FromState   string    `json:"from_state,omitempty"`
	ToState     string    `json:"to_state,omitempty"`
	InstanceKey string    `json:"instance_key,omitempty"`
	ActorUserID int64     `json:"actor_user_id,omitempty"`
	Message     string    `json:"message,omitempty"`
}
