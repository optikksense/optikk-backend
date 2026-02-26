package logs

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/logs/model"
	"github.com/observability/observability-backend-go/internal/modules/logs/service"
	"github.com/observability/observability-backend-go/internal/platform/handlers"
)

type LogHandler struct {
	getTenant handlers.GetTenantFunc
	service   service.Service
}

func NewHandler(getTenant handlers.GetTenantFunc, svc service.Service) *LogHandler {
	return &LogHandler{
		getTenant: getTenant,
		service:   svc,
	}
}

func (h *LogHandler) parseFilters(c *gin.Context) model.LogFilters {
	return model.LogFilters{
		Levels:          handlers.ParseListParam(c, "levels"),
		Services:        handlers.ParseListParam(c, "services"),
		Hosts:           handlers.ParseListParam(c, "hosts"),
		Pods:            handlers.ParseListParam(c, "pods"),
		Containers:      handlers.ParseListParam(c, "containers"),
		Loggers:         handlers.ParseListParam(c, "loggers"),
		TraceID:         c.Query("traceId"),
		SpanID:          c.Query("spanId"),
		Search:          c.Query("search"),
		ExcludeLevels:   handlers.ParseListParam(c, "excludeLevels"),
		ExcludeServices: handlers.ParseListParam(c, "excludeServices"),
		ExcludeHosts:    handlers.ParseListParam(c, "excludeHosts"),
	}
}

func (h *LogHandler) GetLogs(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)
	limit := handlers.ParseIntParam(c, "limit", 100)
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	direction := strings.ToLower(c.DefaultQuery("direction", "desc"))
	if direction != "asc" {
		direction = "desc"
	}

	var cursor model.LogCursor
	if rawCursor := strings.TrimSpace(c.Query("cursor")); rawCursor != "" {
		parsedCursor, ok := model.ParseLogCursor(rawCursor)
		if !ok {
			handlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid cursor")
			return
		}
		cursor = parsedCursor
	}

	resp, err := h.service.GetLogs(c.Request.Context(), teamUUID, startMs, endMs, limit, direction, cursor, h.parseFilters(c))
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query logs")
		return
	}
	handlers.RespondOK(c, resp)
}

func (h *LogHandler) GetLogHistogram(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)
	step := c.Query("step")

	resp, err := h.service.GetLogHistogram(c.Request.Context(), teamUUID, startMs, endMs, step, h.parseFilters(c))
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log histogram")
		return
	}
	handlers.RespondOK(c, resp)
}

func (h *LogHandler) GetLogVolume(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)
	step := c.Query("step")

	resp, err := h.service.GetLogVolume(c.Request.Context(), teamUUID, startMs, endMs, step, h.parseFilters(c))
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log volume")
		return
	}
	handlers.RespondOK(c, resp)
}

func (h *LogHandler) GetLogStats(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)

	resp, err := h.service.GetLogStats(c.Request.Context(), teamUUID, startMs, endMs, h.parseFilters(c))
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log stats")
		return
	}
	handlers.RespondOK(c, resp)
}

func (h *LogHandler) GetLogFields(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)
	field := c.Query("field")

	resp, err := h.service.GetLogFields(c.Request.Context(), teamUUID, startMs, endMs, field, h.parseFilters(c))
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query field values")
		return
	}
	handlers.RespondOK(c, map[string]any{"field": field, "values": resp})
}

func (h *LogHandler) GetLogSurrounding(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	logID := handlers.ParseInt64Param(c, "id", 0)
	before := handlers.ParseIntParam(c, "before", 10)
	after := handlers.ParseIntParam(c, "after", 10)

	resp, err := h.service.GetLogSurrounding(c.Request.Context(), teamUUID, logID, before, after)
	if err != nil {
		handlers.RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "Log entry not found")
		return
	}
	handlers.RespondOK(c, resp)
}

func (h *LogHandler) GetLogDetail(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	traceID := c.Query("traceId")
	spanID := c.Query("spanId")
	timestamp := handlers.ParseInt64Param(c, "timestamp", 0)
	window := handlers.ParseIntParam(c, "contextWindow", 30)

	resp, err := h.service.GetLogDetail(c.Request.Context(), teamUUID, traceID, spanID, timestamp, window)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log detail")
		return
	}
	handlers.RespondOK(c, resp)
}

func (h *LogHandler) GetTraceLogs(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	traceID := c.Param("traceId")

	resp, err := h.service.GetTraceLogs(c.Request.Context(), teamUUID, traceID)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query trace logs")
		return
	}
	handlers.RespondOK(c, resp)
}
