package sse

import (
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
	"github.com/observability/observability-backend-go/internal/platform/auth"
	"github.com/observability/observability-backend-go/internal/platform/utils"
)

// Publisher is the interface satisfied by both *Broker and *RedisBroker,
// allowing the handler to work with either SSE implementation.
type Publisher interface {
	Subscribe(teamID int64) chan Event
	Unsubscribe(teamID int64, ch chan Event)
	Publish(teamID int64, eventType string, data any)
}

// Handler serves the SSE stream endpoint.
type Handler struct {
	broker     Publisher
	getTenant  func(*gin.Context) types.TenantContext
	jwtManager auth.JWTManager
}

// NewHandler creates a new SSE handler backed by the given broker.
// jwtManager is used to validate tokens passed as query params (EventSource
// does not support custom HTTP headers).
func NewHandler(broker Publisher, getTenant func(*gin.Context) types.TenantContext, jwtManager auth.JWTManager) *Handler {
	return &Handler{
		broker:     broker,
		getTenant:  getTenant,
		jwtManager: jwtManager,
	}
}

// Stream handles GET /api/events/stream (and /api/v1/events/stream).
// It keeps the connection open, streaming SSE events for the authenticated
// user's team until the client disconnects.
//
// Because the browser's EventSource API cannot set custom HTTP headers, the
// client passes the JWT token and team ID as query parameters:
//
//	/api/events/stream?token=<jwt>&teamId=<id>
//
// If the normal TenantMiddleware already populated the tenant context (e.g.
// when a reverse proxy forwards the Authorization header), that takes
// precedence.
func (h *Handler) Stream(c *gin.Context) {
	tenant := h.getTenant(c)

	// Fallback: if TenantMiddleware didn't populate (no Authorization header),
	// try query-param-based auth for EventSource compatibility.
	if tenant.TeamID == 0 {
		if token := c.Query("token"); token != "" {
			claims, err := h.jwtManager.Parse(token)
			if err == nil {
				teamID := claims.TeamID
				if qTeam := c.Query("teamId"); qTeam != "" {
					if parsed, e := strconv.ParseInt(qTeam, 10, 64); e == nil && parsed > 0 {
						teamID = parsed
					}
				}
				tenant = types.TenantContext{
					TeamID:    teamID,
					UserID:    utils.ToInt64(claims.Subject, 0),
					UserEmail: claims.Email,
					UserRole:  claims.Role,
				}
			}
		}
	}

	if tenant.TeamID == 0 {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
		return
	}

	// Set SSE headers.
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering

	// Flush headers immediately so the client sees the 200 status.
	c.Writer.WriteHeaderNow()
	c.Writer.Flush()

	ch := h.broker.Subscribe(tenant.TeamID)
	defer h.broker.Unsubscribe(tenant.TeamID, ch)

	// Send an initial "connected" event so the client knows the stream is live.
	initial := fmt.Sprintf("event: connected\ndata: {\"teamId\":%d}\n\n", tenant.TeamID)
	if _, err := c.Writer.WriteString(initial); err != nil {
		return
	}
	c.Writer.Flush()

	ctx := c.Request.Context()

	for {
		select {
		case <-ctx.Done():
			// Client disconnected.
			log.Printf("sse: client disconnected (team=%d)", tenant.TeamID)
			return

		case evt, ok := <-ch:
			if !ok {
				// Channel was closed (broker shut down or unsubscribed elsewhere).
				return
			}
			if _, err := c.Writer.Write(evt.Format()); err != nil {
				// Write failed — client likely disconnected.
				return
			}
			c.Writer.Flush()
		}
	}
}
