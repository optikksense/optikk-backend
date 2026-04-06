package search

import (
	"time"

	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
)

const (
	sioHeartbeatInterval = 15 * time.Second
)

// SubscribeLogsPayload is the JSON payload for live tail subscribe (WebSocket or legacy).
type SubscribeLogsPayload struct {
	TeamID int64 `json:"teamId"`

	StartMs      int64    `json:"startMs"`
	EndMs        int64    `json:"endMs"`
	Severities   []string `json:"severities,omitempty"`
	Services     []string `json:"services,omitempty"`
	Hosts        []string `json:"hosts,omitempty"`
	Pods         []string `json:"pods,omitempty"`
	Containers   []string `json:"containers,omitempty"`
	Environments []string `json:"environments,omitempty"`
	TraceID      string   `json:"traceId,omitempty"`
	SpanID       string   `json:"spanId,omitempty"`
	Search       string   `json:"search,omitempty"`
	SearchMode   string   `json:"searchMode,omitempty"`

	ExcludeSeverities []string `json:"excludeSeverities,omitempty"`
	ExcludeServices   []string `json:"excludeServices,omitempty"`
	ExcludeHosts      []string `json:"excludeHosts,omitempty"`

	AttributeFilters []shared.LogAttributeFilter `json:"attributeFilters,omitempty"`
}

type socketErrorPayload struct {
	Message string `json:"message"`
}

type socketHeartbeatPayload struct {
	LagMs        int64 `json:"lagMs"`
	DroppedCount int64 `json:"droppedCount"`
}

type socketLogEventPayload struct {
	Item         shared.Log `json:"item"`
	LagMs        int64      `json:"lagMs"`
	DroppedCount int64      `json:"droppedCount"`
}
