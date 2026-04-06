package livetail

import "time"

const (
	sioPollInterval   = 2 * time.Second
	sioMaxSessionTime = 5 * time.Minute
)

// SubscribeSpansPayload is the JSON payload for live tail subscribe (WebSocket or legacy).
type SubscribeSpansPayload struct {
	TeamID     int64    `json:"teamId"`
	Services   []string `json:"services,omitempty"`
	Status     string   `json:"status,omitempty"`
	SpanKind   string   `json:"spanKind,omitempty"`
	SearchText string   `json:"search,omitempty"`
	Operation  string   `json:"operationName,omitempty"`
	HTTPMethod string   `json:"httpMethod,omitempty"`
}

type socketErrorPayload struct {
	Message string `json:"message"`
}

type socketDonePayload struct {
	Reason string `json:"reason"`
}

type socketHeartbeatPayload struct {
	LagMs        int64 `json:"lagMs"`
	DroppedCount int64 `json:"droppedCount"`
}

type socketSpanEventPayload struct {
	Item         LiveSpan `json:"item"`
	LagMs        int64    `json:"lagMs"`
	DroppedCount int64    `json:"droppedCount"`
}
