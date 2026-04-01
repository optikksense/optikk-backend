package search

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	sio "github.com/Optikk-Org/optikk-backend/internal/infra/socketio"
	shared "github.com/Optikk-Org/optikk-backend/internal/modules/logs/internal/shared"
)

const (
	sioPollInterval      = 2 * time.Second
	sioHeartbeatInterval = 15 * time.Second
	sioMaxLogsPerPoll    = 50
)

// SubscribeLogsPayload is the JSON payload sent by the client when
// emitting the "subscribe:logs" event on the /live namespace.
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

// SocketIOHandler creates a SubscriptionHandler that streams logs via Socket.IO.
func SocketIOHandler(service *Service) sio.SubscriptionHandler {
	return sio.SubscriptionHandler{
		Event: "subscribe:logs",
		Handle: func(payload json.RawMessage, emit sio.EmitFunc, done <-chan struct{}) {
			var p SubscribeLogsPayload
			if err := json.Unmarshal(payload, &p); err != nil {
				logger.L().Warn("Socket.IO [subscribe:logs] bad payload", slog.Any("error", err))
				emit("subscribeError", socketErrorPayload{Message: "invalid payload"})
				return
			}

			if p.TeamID == 0 {
				emit("subscribeError", socketErrorPayload{Message: "teamId is required"})
				return
			}

			filters := shared.LogFilters{
				TeamID:            p.TeamID,
				StartMs:           p.StartMs,
				EndMs:             p.EndMs,
				Severities:        p.Severities,
				Services:          p.Services,
				Hosts:             p.Hosts,
				Pods:              p.Pods,
				Containers:        p.Containers,
				Environments:      p.Environments,
				TraceID:           p.TraceID,
				SpanID:            p.SpanID,
				Search:            p.Search,
				SearchMode:        p.SearchMode,
				ExcludeSeverities: p.ExcludeSeverities,
				ExcludeServices:   p.ExcludeServices,
				ExcludeHosts:      p.ExcludeHosts,
				AttributeFilters:  p.AttributeFilters,
			}

			latestNs := uint64(filters.EndMs) * 1_000_000 //nolint:gosec // G115 - domain-constrained value
			lastLagMs := int64(0)
			lastDroppedCount := int64(0)

			ticker := time.NewTicker(sioPollInterval)
			heartbeat := time.NewTicker(sioHeartbeatInterval)
			defer ticker.Stop()
			defer heartbeat.Stop()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Cancel context when done channel closes.
			go func() {
				<-done
				cancel()
			}()

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
					nowMs := time.Now().UnixMilli()
					pollFilters := filters
					pollFilters.StartMs = int64(latestNs/1_000_000) + 1 //nolint:gosec // G115
					pollFilters.EndMs = nowMs

					resp, err := service.GetLogs(ctx, pollFilters, sioMaxLogsPerPoll, "asc", shared.LogCursor{})
					if err != nil {
						logger.L().Warn("Socket.IO [subscribe:logs] poll error", slog.Any("error", err))
						continue
					}

					lastDroppedCount = 0
					logCount := int64(len(resp.Logs))
					if resp.Total > logCount {
						lastDroppedCount = resp.Total - logCount
					}

					for _, entry := range resp.Logs {
						entryLagMs := nowMs - int64(entry.Timestamp/1_000_000) //nolint:gosec // G115 - timestamp is bounded by domain
						if entryLagMs < 0 {
							entryLagMs = 0
						}

						emit("log", socketLogEventPayload{
							Item:         entry,
							LagMs:        entryLagMs,
							DroppedCount: lastDroppedCount,
						})
						if entry.Timestamp > latestNs {
							latestNs = entry.Timestamp
						}
						lastLagMs = entryLagMs
					}
				}
			}
		},
	}
}
