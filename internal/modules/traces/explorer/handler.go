package explorer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	spanlivetail "github.com/Optikk-Org/optikk-backend/internal/modules/traces/livetail"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

const (
	traceStreamPollInterval      = 2 * time.Second
	traceStreamHeartbeatInterval = 12 * time.Second
	traceStreamMaxSession        = 5 * time.Minute
)

type Handler struct {
	modulecommon.DBTenant
	Service         *Service
	LiveTailService *spanlivetail.Service
}

func NewHandler(getTenant modulecommon.GetTenantFunc, service *Service, liveTailService *spanlivetail.Service) *Handler {
	return &Handler{
		DBTenant:        modulecommon.DBTenant{GetTenant: getTenant},
		Service:         service,
		LiveTailService: liveTailService,
	}
}

func (h *Handler) Query(c *gin.Context) {
	var req QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid request body")
		return
	}
	if req.StartTime <= 0 || req.EndTime <= 0 || req.StartTime >= req.EndTime {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Valid startTime and endTime are required")
		return
	}

	resp, err := h.Service.Query(c.Request.Context(), req, h.GetTenant(c).TeamID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query traces explorer", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) Stream(c *gin.Context) {
	tenant := h.GetTenant(c)
	filters := spanlivetail.LiveTailFilters{
		Status:     c.Query("status"),
		SpanKind:   c.Query("spanKind"),
		SearchText: strings.TrimSpace(c.Query("search")),
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

	ctx := c.Request.Context()
	flusher, canFlush := c.Writer.(http.Flusher)
	since := time.Now().Add(-traceStreamPollInterval)
	deadline := time.Now().Add(traceStreamMaxSession)

	_, _ = fmt.Fprintf(c.Writer, "event: state\ndata: {\"status\":\"connected\"}\n\n")
	if canFlush {
		flusher.Flush()
	}

	ticker := time.NewTicker(traceStreamPollInterval)
	heartbeat := time.NewTicker(traceStreamHeartbeatInterval)
	defer ticker.Stop()
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			_, _ = fmt.Fprintf(c.Writer, "event: heartbeat\ndata: {\"status\":\"alive\"}\n\n")
			if canFlush {
				flusher.Flush()
			}
		case <-ticker.C:
			if time.Now().After(deadline) {
				_, _ = fmt.Fprintf(c.Writer, "event: done\ndata: {\"reason\":\"session_timeout\"}\n\n")
				if canFlush {
					flusher.Flush()
				}
				return
			}

			spans, err := h.LiveTailService.Poll(tenant.TeamID, since, filters)
			if err != nil {
				_, _ = fmt.Fprintf(c.Writer, "event: error\ndata: {\"message\":\"poll failed\"}\n\n")
				if canFlush {
					flusher.Flush()
				}
				continue
			}

			for _, span := range spans {
				payload, err := json.Marshal(StreamItem{
					Item:         span,
					LagMs:        maxTraceLag(time.Since(span.Timestamp).Milliseconds()),
					DroppedCount: 0,
				})
				if err != nil {
					continue
				}
				_, _ = fmt.Fprintf(c.Writer, "event: item\ndata: %s\n\n", payload)
			}
			if len(spans) > 0 {
				since = spans[0].Timestamp
			}
			if canFlush && len(spans) > 0 {
				flusher.Flush()
			}
		}
	}
}

func maxTraceLag(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}
