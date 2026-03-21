package search

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	shared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetLogs(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}

	limit := common.ParseIntParam(c, "limit", 100)
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	direction := strings.ToLower(c.DefaultQuery("direction", "desc"))
	if direction != "asc" {
		direction = "desc"
	}

	var cursor shared.LogCursor
	if raw := strings.TrimSpace(c.Query("cursor")); raw != "" {
		parsed, ok := shared.ParseLogCursor(raw)
		if !ok {
			common.RespondError(c, http.StatusBadRequest, errorcode.Validation, "Invalid cursor")
			return
		}
		cursor = parsed
	}

	resp, err := h.Service.GetLogs(c.Request.Context(), filters, limit, direction, cursor)
	if err != nil {
		common.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query logs", err)
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) StreamLogs(c *gin.Context) {
	filters, ok := shared.EnrichFilters(c, h.GetTenant(c).TeamID)
	if !ok {
		return
	}

	const pollInterval = 2 * time.Second
	const heartbeatInterval = 15 * time.Second
	const maxLogsPerPoll = 50

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	ctx := c.Request.Context()
	flusher, canFlush := c.Writer.(http.Flusher)
	latestNs := uint64(filters.EndMs) * 1_000_000

	ticker := time.NewTicker(pollInterval)
	heartbeat := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			fmt.Fprintf(c.Writer, "event: heartbeat\ndata: {}\n\n")
			if canFlush {
				flusher.Flush()
			}
		case <-ticker.C:
			nowMs := time.Now().UnixMilli()
			pollFilters := filters
			pollFilters.StartMs = int64(latestNs/1_000_000) + 1
			pollFilters.EndMs = nowMs

			resp, err := h.Service.GetLogs(ctx, pollFilters, maxLogsPerPoll, "asc", shared.LogCursor{})
			if err != nil {
				log.Printf("WARN [StreamLogs] poll error: %v", err)
				continue
			}

			for _, entry := range resp.Logs {
				b, err := json.Marshal(entry)
				if err != nil {
					log.Printf("WARN [StreamLogs] marshal error: %v", err)
					continue
				}
				fmt.Fprintf(c.Writer, "data: %s\n\n", b)
				if entry.Timestamp > latestNs {
					latestNs = entry.Timestamp
				}
			}
			if canFlush && len(resp.Logs) > 0 {
				flusher.Flush()
			}
		}
	}
}
