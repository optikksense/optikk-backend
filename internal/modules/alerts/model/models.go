package model

import "time"

// Alert represents a row in the alerts table or a returned structured object.
type Alert struct {
	ID              int64      `json:"id"`
	OrganizationID  int64      `json:"organizationId"`
	TeamID          int64      `json:"teamId"`
	Name            string     `json:"name"`
	Description     *string    `json:"description,omitempty"`
	Type            string     `json:"type"`
	Severity        string     `json:"severity"`
	Status          string     `json:"status"`
	ServiceName     *string    `json:"serviceName,omitempty"`
	Condition       *string    `json:"condition,omitempty"`
	Metric          *string    `json:"metric,omitempty"`
	Operator        *string    `json:"operator,omitempty"`
	Threshold       float64    `json:"threshold"`
	DurationMinutes int        `json:"durationMinutes"`
	CurrentValue    *float64   `json:"currentValue,omitempty"`
	TriggeredBy     *string    `json:"triggeredBy,omitempty"`
	TriggeredAt     *time.Time `json:"triggeredAt,omitempty"`
	AcknowledgedAt  *time.Time `json:"acknowledgedAt,omitempty"`
	AcknowledgedBy  *string    `json:"acknowledgedBy,omitempty"`
	ResolvedAt      *time.Time `json:"resolvedAt,omitempty"`
	ResolvedBy      *string    `json:"resolvedBy,omitempty"`
	MutedUntil      *time.Time `json:"mutedUntil,omitempty"`
	MuteReason      *string    `json:"muteReason,omitempty"`
	RunbookURL      *string    `json:"runbookUrl,omitempty"`
	CreatedAt       time.Time  `json:"createdAt"`
	UpdatedAt       *time.Time `json:"updatedAt,omitempty"`
}

// Incident represents an incident built from aggregate alerts table data.
type Incident struct {
	IncidentID     int64      `json:"incident_id"`
	AlertPolicyID  *string    `json:"alert_policy_id"`
	TriggeredBy    *string    `json:"triggered_by"`
	Title          string     `json:"title"`
	Description    *string    `json:"description"`
	Severity       string     `json:"severity"`
	Priority       *string    `json:"priority"`
	Status         string     `json:"status"`
	Source         string     `json:"source"`
	ServiceName    *string    `json:"service_name"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      *time.Time `json:"updated_at"`
	ResolvedAt     *time.Time `json:"resolved_at"`
	AcknowledgedAt *time.Time `json:"acknowledged_at"`
	AcknowledgedBy *string    `json:"acknowledged_by"`
	Pod            *string    `json:"pod"`
	Container      *string    `json:"container"`
}

// IncidentCounts holds aggregated counts by status and severity.
type IncidentCounts struct {
	ByStatus   map[string]int64 `json:"byStatus"`
	BySeverity map[string]int64 `json:"bySeverity"`
}

// IncidentsResponse represents the paginated response for incidents.
type IncidentsResponse struct {
	Incidents []Incident     `json:"incidents"`
	HasMore   bool           `json:"hasMore"`
	Offset    int            `json:"offset"`
	Limit     int            `json:"limit"`
	Total     int64          `json:"total"`
	Counts    IncidentCounts `json:"counts"`
}

// AlertPage represents a paginated response for alerts.
type AlertPage struct {
	Content          []Alert  `json:"content"`
	Pageable         Pageable `json:"pageable"`
	TotalElements    int64    `json:"totalElements"`
	TotalPages       int      `json:"totalPages"`
	Size             int      `json:"size"`
	Number           int      `json:"number"`
	First            bool     `json:"first"`
	Last             bool     `json:"last"`
	NumberOfElements int      `json:"numberOfElements"`
}

// Pageable holds pagination metadata.
type Pageable struct {
	PageNumber int `json:"pageNumber"`
	PageSize   int `json:"pageSize"`
	Offset     int `json:"offset"`
}
