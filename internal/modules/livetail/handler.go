package livetail

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	otlplogs "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/logs"
	otlpspans "github.com/Optikk-Org/optikk-backend/internal/ingestion/otlp/spans"
	"github.com/Optikk-Org/optikk-backend/internal/infra/session"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// ClientMessage is the first (and only) subscribe message from the client.
type ClientMessage struct {
	Op      string          `json:"op"`
	Payload json.RawMessage `json:"payload"`
}

// outbound is written to the WebSocket.
type outbound struct {
	Event string `json:"event"`
	Data  any    `json:"data"`
}

// Config wires live tail hub and session validation.
type Config struct {
	Hub            Hub
	AllowedOrigins []string
	Sessions       session.Manager
}

// NewHandler returns a Gin handler for GET /api/v1/ws/live (WebSocket upgrade).
func NewHandler(cfg Config) gin.HandlerFunc {
	up := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 4096,
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			if origin == "" || len(cfg.AllowedOrigins) == 0 {
				return true
			}
			for _, allowed := range cfg.AllowedOrigins {
				if allowed == "*" || allowed == origin {
					return true
				}
				if strings.HasPrefix(allowed, "*.") {
					suffix := allowed[1:]
					if strings.HasSuffix(origin, suffix) {
						return true
					}
				}
			}
			return false
		},
	}

	return func(c *gin.Context) {
		authState, ok := cfg.Sessions.GetAuthState(c.Request.Context())
		if !ok || authState.UserID == 0 {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		conn, err := up.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			slog.Warn("live tail WebSocket upgrade failed", slog.Any("error", err))
			return
		}
		defer conn.Close()

		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var msg ClientMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			_ = conn.WriteJSON(outbound{Event: "subscribeError", Data: map[string]string{"message": "invalid JSON"}})
			return
		}

		teamID, ok := extractTeamID(msg.Op, msg.Payload)
		if !ok || teamID == 0 {
			_ = conn.WriteJSON(outbound{Event: "subscribeError", Data: map[string]string{"message": "teamId is required"}})
			return
		}
		if !teamAllowed(authState, teamID) {
			_ = conn.WriteJSON(outbound{Event: "subscribeError", Data: map[string]string{"message": "forbidden team"}})
			return
		}

		eventChan := make(chan any, 500)
		cfg.Hub.Subscribe(teamID, eventChan, nil)
		defer cfg.Hub.Unsubscribe(teamID, eventChan)

		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()

		eventName := "log"
		if msg.Op == "subscribe:spans" {
			eventName = "spans"
		}

		for {
			select {
			case <-done:
				return
			case ev := <-eventChan:
				now := time.Now().UnixMilli()
				var lag int64
				var item any
				switch msg.Op {
				case "subscribe:logs":
					wl, ok := ev.(otlplogs.WireLog)
					if !ok {
						continue
					}
					lag = now - wl.EmitMs
					item = wl
				case "subscribe:spans":
					ws, ok := ev.(otlpspans.WireSpan)
					if !ok {
						continue
					}
					lag = now - ws.EmitMs
					item = ws
				default:
					continue
				}
				if lag < 0 {
					lag = 0
				}
				payload := map[string]any{
					"item":  item,
					"lagMs": lag,
				}
				if err := conn.WriteJSON(outbound{Event: eventName, Data: payload}); err != nil {
					return
				}
			}
		}
	}
}

func extractTeamID(op string, payload json.RawMessage) (int64, bool) {
	if len(payload) == 0 {
		return 0, false
	}
	switch op {
	case "subscribe:logs", "subscribe:spans":
		var p struct {
			TeamID int64 `json:"teamId"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return 0, false
		}
		return p.TeamID, true
	default:
		return 0, false
	}
}

func teamAllowed(state session.AuthState, requested int64) bool {
	if len(state.TeamIDs) == 0 {
		return state.DefaultTeamID == requested
	}
	for _, id := range state.TeamIDs {
		if id == requested {
			return true
		}
	}
	return false
}
