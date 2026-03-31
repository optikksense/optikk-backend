package livetail

import (
	"encoding/json"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	sio "github.com/Optikk-Org/optikk-backend/internal/infra/socketio"
	"go.uber.org/zap"
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

type socketHeartbeatPayload struct {
	LagMs        int64 `json:"lagMs"`
	DroppedCount int64 `json:"droppedCount"`
}

type socketSpanEventPayload struct {
	Item         LiveSpan `json:"item"`
	LagMs        int64    `json:"lagMs"`
	DroppedCount int64    `json:"droppedCount"`
}

// SocketIOHandler creates a SubscriptionHandler that streams live spans via Socket.IO.
func SocketIOHandler(service *Service) sio.SubscriptionHandler {
	return sio.SubscriptionHandler{
		Event: "subscribe:spans",
		Handle: func(payload json.RawMessage, emit sio.EmitFunc, done <-chan struct{}) {
			var p SubscribeSpansPayload
			if err := json.Unmarshal(payload, &p); err != nil {
				logger.L().Warn("Socket.IO [subscribe:spans] bad payload", zap.Error(err))
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
			heartbeat := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			defer heartbeat.Stop()
			lastLagMs := int64(0)
			lastDroppedCount := int64(0)

			for {
				select {
				case <-done:
					return
				case <-heartbeat.C:
					emit("heartbeat", socketHeartbeatPayload{
						LagMs:        lastLagMs,
						DroppedCount: lastDroppedCount,
					})
				case <-ticker.C:
					if time.Now().After(deadline) {
						emit("done", socketDonePayload{Reason: "session_timeout"})
						return
					}

					result, err := service.Poll(p.TeamID, since, filters)
					if err != nil {
						logger.L().Warn("Socket.IO [subscribe:spans] poll error", zap.Error(err))
						emit("error", socketErrorPayload{Message: "poll error"})
						continue
					}

					lastDroppedCount = result.DroppedCount
					for _, s := range result.Spans {
						since = s.Timestamp
						lagMs := time.Since(s.Timestamp).Milliseconds()
						if lagMs < 0 {
							lagMs = 0
						}
						emit("span", socketSpanEventPayload{
							Item:         s,
							LagMs:        lagMs,
							DroppedCount: lastDroppedCount,
						})
						lastLagMs = lagMs
					}
				}
			}
		},
	}
}
