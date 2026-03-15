package alerting

import "time"

// AlertRule defines a user-created alert condition.
type AlertRule struct {
	ID              int64     `json:"id"`
	TeamID          int64     `json:"teamId"`
	CreatedBy       int64     `json:"createdBy"`
	Name            string    `json:"name"`
	Description     string    `json:"description,omitempty"`
	Enabled         bool      `json:"enabled"`
	Severity        string    `json:"severity"`        // critical, warning, info
	ConditionType   string    `json:"conditionType"`   // threshold, absence
	SignalType      string    `json:"signalType"`      // metric, log, span
	Query           string    `json:"query"`           // OptiQL or simplified query
	Operator        string    `json:"operator"`        // gt, gte, lt, lte, eq
	Threshold       float64   `json:"threshold"`
	DurationMinutes int       `json:"durationMinutes"` // evaluation window
	ServiceName     string    `json:"serviceName,omitempty"`
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
}

type CreateRuleRequest struct {
	Name            string  `json:"name" binding:"required"`
	Description     string  `json:"description"`
	Severity        string  `json:"severity" binding:"required"`
	ConditionType   string  `json:"conditionType" binding:"required"`
	SignalType      string  `json:"signalType" binding:"required"`
	Query           string  `json:"query" binding:"required"`
	Operator        string  `json:"operator" binding:"required"`
	Threshold       float64 `json:"threshold"`
	DurationMinutes int     `json:"durationMinutes" binding:"required"`
	ServiceName     string  `json:"serviceName"`
}

type UpdateRuleRequest struct {
	Name            *string  `json:"name"`
	Description     *string  `json:"description"`
	Enabled         *bool    `json:"enabled"`
	Severity        *string  `json:"severity"`
	Query           *string  `json:"query"`
	Operator        *string  `json:"operator"`
	Threshold       *float64 `json:"threshold"`
	DurationMinutes *int     `json:"durationMinutes"`
	ServiceName     *string  `json:"serviceName"`
}

// NotificationChannel defines where alerts are sent.
type NotificationChannel struct {
	ID          int64     `json:"id"`
	TeamID      int64     `json:"teamId"`
	CreatedBy   int64     `json:"createdBy"`
	Name        string    `json:"name"`
	ChannelType string    `json:"channelType"` // slack, webhook, email
	Config      string    `json:"config"`      // JSON config (url, email address, etc.)
	Enabled     bool      `json:"enabled"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type CreateChannelRequest struct {
	Name        string `json:"name" binding:"required"`
	ChannelType string `json:"channelType" binding:"required"`
	Config      string `json:"config" binding:"required"`
}

type UpdateChannelRequest struct {
	Name    *string `json:"name"`
	Config  *string `json:"config"`
	Enabled *bool   `json:"enabled"`
}

// Incident represents a triggered alert occurrence.
type Incident struct {
	ID             int64      `json:"id"`
	TeamID         int64      `json:"teamId"`
	RuleID         int64      `json:"ruleId"`
	RuleName       string     `json:"ruleName"`
	Severity       string     `json:"severity"`
	Status         string     `json:"status"` // open, acknowledged, resolved
	TriggerValue   float64    `json:"triggerValue"`
	Threshold      float64    `json:"threshold"`
	Message        string     `json:"message,omitempty"`
	AcknowledgedBy *int64     `json:"acknowledgedBy,omitempty"`
	ResolvedBy     *int64     `json:"resolvedBy,omitempty"`
	TriggeredAt    time.Time  `json:"triggeredAt"`
	AcknowledgedAt *time.Time `json:"acknowledgedAt,omitempty"`
	ResolvedAt     *time.Time `json:"resolvedAt,omitempty"`
	CreatedAt      time.Time  `json:"createdAt"`
}

type IncidentAction struct {
	Status string `json:"status" binding:"required"` // acknowledged, resolved
}
