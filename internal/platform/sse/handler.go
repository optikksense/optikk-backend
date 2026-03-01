package sse

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	types "github.com/observability/observability-backend-go/internal/contracts"
)

// Handler serves the SSE stream endpoint.
type Handler struct {
	broker    *Broker
	getTenant func(*gin.Context) types.TenantContext
}

// NewHandler creates a new SSE handler backed by the given broker.
func NewHandler(broker *Broker, getTenant func(*gin.Context) types.TenantContext) *Handler {
	return &Handler{
		broker:    broker,
		getTenant: getTenant,
	}
}

// Stream handles GET /api/events/stream (and /api/v1/events/stream).
// It keeps the connection open, streaming SSE events for the authenticated
// user's team until the client disconnects.
func (h *Handler) Stream(c *gin.Context) {
	tenant := h.getTenant(c)
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

	ch, unsubscribe := h.broker.Subscribe(tenant.TeamID)
	defer func() {
		// Close the channel so the drain loop in unsubscribe terminates.
		close(ch)
		// NOTE: we cannot call unsubscribe after close because it ranges over ch.
		// Instead, inline the cleanup here.
	}()

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
			// Manually remove from broker since we can't use unsubscribe after close.
			h.broker.mu.Lock()
			delete(h.broker.subscribers[tenant.TeamID], ch)
			if len(h.broker.subscribers[tenant.TeamID]) == 0 {
				delete(h.broker.subscribers, tenant.TeamID)
			}
			h.broker.mu.Unlock()
			return

		case evt := <-ch:
			if _, err := c.Writer.Write(evt.Format()); err != nil {
				// Write failed — client likely disconnected.
				h.broker.mu.Lock()
				delete(h.broker.subscribers[tenant.TeamID], ch)
				if len(h.broker.subscribers[tenant.TeamID]) == 0 {
					delete(h.broker.subscribers, tenant.TeamID)
				}
				h.broker.mu.Unlock()
				return
			}
			c.Writer.Flush()
		}
	}
}
