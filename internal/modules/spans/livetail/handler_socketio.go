package livetail

import (
	"encoding/json"
	"log"
	"time"

	sio "github.com/observability/observability-backend-go/internal/platform/socketio"
)

const (
	sioPollInterval   = 2 * time.Second
	sioMaxSessionTime = 5 * time.Minute
)

// SubscribeSpansPayload is the JSON payload sent by the client when
// emitting the "subscribe:spans" event on the /live namespace.
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

type socketSpanEventPayload struct {
	Item  LiveSpan `json:"item"`
	LagMs int64    `json:"lagMs"`
}

// SocketIOHandler creates a SubscriptionHandler that streams live spans
// via Socket.IO instead of SSE.
func SocketIOHandler(service *Service) sio.SubscriptionHandler {
	return sio.SubscriptionHandler{
		Event: "subscribe:spans",
		Handle: func(payload json.RawMessage, emit sio.EmitFunc, done <-chan struct{}) {
			var p SubscribeSpansPayload
			if err := json.Unmarshal(payload, &p); err != nil {
				log.Printf("Socket.IO [subscribe:spans] bad payload: %v", err)
				emit("error", socketErrorPayload{Message: "invalid payload"})
				return
			}

			if p.TeamID == 0 {
				emit("error", socketErrorPayload{Message: "teamId is required"})
				return
			}

			filters := LiveTailFilters{
				Services:   p.Services,
				Status:     p.Status,
				SpanKind:   p.SpanKind,
				SearchText: p.SearchText,
				Operation:  p.Operation,
				HTTPMethod: p.HTTPMethod,
			}

			since := time.Now().Add(-sioPollInterval)
			deadline := time.Now().Add(sioMaxSessionTime)
			ticker := time.NewTicker(sioPollInterval)
			defer ticker.Stop()

			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					if time.Now().After(deadline) {
						emit("done", socketDonePayload{Reason: "session_timeout"})
						return
					}

					spans, err := service.Poll(p.TeamID, since, filters)
					if err != nil {
						log.Printf("Socket.IO [subscribe:spans] poll error: %v", err)
						emit("error", socketErrorPayload{Message: "poll error"})
						continue
					}

					for _, s := range spans {
						since = s.Timestamp
						emit("span", socketSpanEventPayload{
							Item:  s,
							LagMs: time.Since(s.Timestamp).Milliseconds(),
						})
					}
				}
			}
		},
	}
}
