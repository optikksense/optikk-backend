package explorer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	logshared "github.com/observability/observability-backend-go/internal/modules/log/internal/shared"
	logsearch "github.com/observability/observability-backend-go/internal/modules/log/search"
)

const (
	streamPollInterval      = 2 * time.Second
	streamHeartbeatInterval = 12 * time.Second
	streamMaxLogsPerPoll    = 75
)

type Handler struct {
	modulecommon.DBTenant
	Service       *Service
	SearchService *logsearch.Service
}

func NewHandler(getTenant modulecommon.GetTenantFunc, service *Service, searchService *logsearch.Service) *Handler {
	return &Handler{
		DBTenant:      modulecommon.DBTenant{GetTenant: getTenant},
		Service:       service,
		SearchService: searchService,
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
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query logs explorer", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *Handler) Stream(c *gin.Context) {
	tenant := h.GetTenant(c)
	filters, ok := logshared.EnrichFilters(c, tenant.TeamID)
	if !ok {
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	ctx := c.Request.Context()
	flusher, canFlush := c.Writer.(http.Flusher)
	latestNs := uint64(time.Now().Add(-streamPollInterval).UnixMilli()) * 1_000_000 //nolint:gosec // G115 - domain-constrained value

	ticker := time.NewTicker(streamPollInterval)
	heartbeat := time.NewTicker(streamHeartbeatInterval)
	defer ticker.Stop()
	defer heartbeat.Stop()

	_, _ = fmt.Fprintf(c.Writer, "event: state\ndata: {\"status\":\"connected\"}\n\n")
	if canFlush {
		flusher.Flush()
	}

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
			nowMs := time.Now().UnixMilli()
			pollFilters := filters
			pollFilters.StartMs = int64(latestNs/1_000_000) + 1 //nolint:gosec // G115
			pollFilters.EndMs = nowMs

			resp, err := h.SearchService.GetLogs(ctx, pollFilters, streamMaxLogsPerPoll, "asc", logshared.LogCursor{})
			if err != nil {
				_, _ = fmt.Fprintf(c.Writer, "event: error\ndata: {\"message\":\"poll failed\"}\n\n")
				if canFlush {
					flusher.Flush()
				}
				continue
			}

			for _, log := range resp.Logs {
				payload, err := json.Marshal(gin.H{
					"item":         log,
					"lagMs":        maxInt64(0, nowMs-int64(log.Timestamp/1_000_000)), //nolint:gosec // G115
					"droppedCount": 0,
				})
				if err != nil {
					continue
				}
				_, _ = fmt.Fprintf(c.Writer, "event: item\ndata: %s\n\n", payload)
				if log.Timestamp > latestNs {
					latestNs = log.Timestamp
				}
			}
			if canFlush && len(resp.Logs) > 0 {
				flusher.Flush()
			}
		}
	}
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}
