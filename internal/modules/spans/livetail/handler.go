package livetail

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/common"
)

const (
	pollInterval   = 2 * time.Second
	maxSessionTime = 5 * time.Minute
)

type Handler struct {
	getTenant common.GetTenantFunc
	service   *Service
}

func NewHandler(getTenant common.GetTenantFunc, service *Service) *Handler {
	return &Handler{getTenant: getTenant, service: service}
}

// GetLiveTail handles GET /v1/spans/live-tail as a Server-Sent Events stream.
// Query params: services (comma-sep), status, spanKind
func (h *Handler) GetLiveTail(c *gin.Context) {
	teamID := h.getTenant(c).TeamID

	filters := LiveTailFilters{
		Status:     c.Query("status"),
		SpanKind:   c.Query("spanKind"),
		SearchText: c.Query("search"),
		Operation:  c.Query("operationName"),
		HTTPMethod: c.Query("httpMethod"),
	}
	if svc := c.Query("services"); svc != "" {
		filters.Services = strings.Split(svc, ",")
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	since := time.Now().Add(-pollInterval)
	deadline := time.Now().Add(maxSessionTime)

	c.Stream(func(w io.Writer) bool {
		if time.Now().After(deadline) {
			_, _ = fmt.Fprintf(w, "event: done\ndata: {\"reason\":\"session_timeout\"}\n\n")
			return false
		}

		select {
		case <-c.Request.Context().Done():
			return false
		case <-time.After(pollInterval):
		}

		spans, err := h.service.Poll(teamID, since, filters)
		if err != nil {
			_, _ = fmt.Fprintf(w, "event: error\ndata: {\"message\":\"poll error\"}\n\n")
			return true
		}

		if len(spans) > 0 {
			since = spans[0].Timestamp
			data, _ := json.Marshal(spans)
			_, _ = fmt.Fprintf(w, "event: spans\ndata: %s\n\n", data)
		}

		return true
	})
}
