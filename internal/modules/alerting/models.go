// Package alerting implements Optik's alert rule engine.
//
// Strict 6-file module pattern. Subpackages:
//   - evaluators/ : condition evaluator interface + slo_burn_rate + error_rate
//   - channels/   : delivery channel interface + slack
//
// The evaluator loop and dispatcher are BackgroundRunners registered alongside
// the HTTP module in modules_manifest.go.
package alerting

import (
	"encoding/json"
	"time"
)

// Condition types. The engine is generic — adding a new type is a new Evaluator
// in the registry, no schema change.
const (
	ConditionSLOBurnRate = "slo_burn_rate"
	ConditionErrorRate   = "error_rate"
)

// Operators used to compare windowed values against thresholds.
const (
	OpGT  = "gt"
	OpGTE = "gte"
	OpLT  = "lt"
	OpLTE = "lte"
	OpEQ  = "eq"
)

// Rule states (rolled up from instance states).
const (
	StateOK     = "ok"
	StateNoData = "no_data"
	StateWarn   = "warn"
	StateAlert  = "alert"
	StateMuted  = "muted"
)

// Severities mirror typical on-call grades.
const (
	SeverityP1 = "p1"
	SeverityP2 = "p2"
	SeverityP3 = "p3"
	SeverityP4 = "p4"
	SeverityP5 = "p5"
)

// Event kinds recorded in ClickHouse alert_events.
const (
	EventKindTransition     = "transition"
	EventKindEdit           = "edit"
	EventKindMute           = "mute"
	EventKindAck            = "ack"
	EventKindSilence        = "silence"
	EventKindDispatch       = "dispatch"
	EventKindDispatchFailed = "dispatch_failed"
)

// Window is one evaluator lookback window. Multi-window rules evaluate all
// windows per tick and compare each against the threshold; transitions require
// all configured windows to agree (AND semantics).
type Window struct {
	Name string `json:"name"`
	Secs int64  `json:"secs"`
}

// Silence is a tag-scoped maintenance window stored inline on the rule.
type Silence struct {
	ID         string            `json:"id"`
	StartsAt   time.Time         `json:"starts_at"`
	EndsAt     time.Time         `json:"ends_at"`
	Recurrence string            `json:"recurrence,omitempty"` // "", "daily", "weekly"
	MatchTags  map[string]string `json:"match_tags,omitempty"`
	Reason     string            `json:"reason,omitempty"`
	CreatedBy  int64             `json:"created_by,omitempty"`
}

// Instance is per-group runtime state for a rule. Stored inline in the alerts
// row as a JSON map keyed by InstanceKey.
type Instance struct {
	InstanceKey          string             `json:"instance_key"`
	GroupValues          map[string]string  `json:"group_values,omitempty"`
	State                string             `json:"state"`
	Values               map[string]float64 `json:"values,omitempty"`
	PendingSince         *time.Time         `json:"pending_since,omitempty"`
	FiredAt              *time.Time         `json:"fired_at,omitempty"`
	ResolvedAt           *time.Time         `json:"resolved_at,omitempty"`
	LastNotifiedAt       *time.Time         `json:"last_notified_at,omitempty"`
	LastTransitionSeq    int64              `json:"last_transition_seq"`
	LastNotifiedSeq      int64              `json:"last_notified_seq"`
	AckedBy              int64              `json:"acked_by,omitempty"`
	AckedUntil           *time.Time         `json:"acked_until,omitempty"`
	SnoozedUntil         *time.Time         `json:"snoozed_until,omitempty"`
	NoData               bool               `json:"no_data,omitempty"`
	NoDataSince          *time.Time         `json:"no_data_since,omitempty"`
	NotifsThisHourCount  int                `json:"notifs_this_hour_count,omitempty"`
	NotifsThisHourAnchor *time.Time         `json:"notifs_this_hour_anchor,omitempty"`
}

// InstancesMap is the serialised shape on the rule row.
type InstancesMap map[string]*Instance

// Rule is the canonical in-memory rule object (decoded from MySQL).
type Rule struct {
	ID                int64
	TeamID            int64
	Name              string
	Description       string
	ConditionType     string
	TargetRef         json.RawMessage
	GroupBy           []string
	Windows           []Window
	Operator          string
	WarnThreshold     *float64
	CriticalThreshold float64
	RecoveryThreshold *float64
	ForSecs           int64
	RecoverForSecs    int64
	KeepAliveSecs     int64
	NoDataSecs        int64
	Severity          string
	NotifyTemplate    string
	MaxNotifsPerHour  int
	SlackWebhookURL   string

	RuleState  string
	LastEvalAt *time.Time
	Instances  InstancesMap
	MuteUntil  *time.Time
	Silences   []Silence
	Enabled    bool
	ParentID   *int64
	CreatedAt  time.Time
	UpdatedAt  time.Time
	CreatedBy  int64
	UpdatedBy  int64
}

// AlertEvent is the append-only ClickHouse row.
type AlertEvent struct {
	Ts           time.Time `ch:"ts"`
	TeamID       uint32    `ch:"team_id"`
	AlertID      int64     `ch:"alert_id"`
	InstanceKey  string    `ch:"instance_key"`
	Kind         string    `ch:"kind"`
	FromState    string    `ch:"from_state"`
	ToState      string    `ch:"to_state"`
	Values       string    `ch:"values"`
	ActorUserID  int64     `ch:"actor_user_id"`
	Message      string    `ch:"message"`
	DeployRefs   string    `ch:"deploy_refs"`
	TransitionID int64     `ch:"transition_id"`
}

// ensure json import is used
var _ = json.RawMessage(nil)
