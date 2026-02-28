package logs

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/platform/handlers"
)

type LogHandler struct {
	getTenant handlers.GetTenantFunc
	repo      *ClickHouseRepository
}

func NewHandler(getTenant handlers.GetTenantFunc, repo *ClickHouseRepository) *LogHandler {
	return &LogHandler{
		getTenant: getTenant,
		repo:      repo,
	}
}

func (h *LogHandler) parseFilters(c *gin.Context) LogFilters {
	return LogFilters{
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

func (h *LogHandler) enrichFilters(c *gin.Context, defaultRangeMs int64) LogFilters {
	f := h.parseFilters(c)
	f.TeamUUID = h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, defaultRangeMs)
	f.StartMs = startMs
	f.EndMs = endMs
	return f
}

func (h *LogHandler) GetLogs(c *gin.Context) {
	f := h.enrichFilters(c, 60*60*1000)
	limit := handlers.ParseIntParam(c, "limit", 100)
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	direction := strings.ToLower(c.DefaultQuery("direction", "desc"))
	if direction != "asc" {
		direction = "desc"
	}

	var cursor LogCursor
	if rawCursor := strings.TrimSpace(c.Query("cursor")); rawCursor != "" {
		parsedCursor, ok := ParseLogCursor(rawCursor)
		if !ok {
			handlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid cursor")
			return
		}
		cursor = parsedCursor
	}

	logs, total, err := h.repo.GetLogs(c.Request.Context(), f, limit, direction, cursor)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query logs")
		return
	}

	currentOffset := cursor.Offset
	if currentOffset < 0 {
		currentOffset = 0
	}
	nextOffset := currentOffset + len(logs)
	hasMore := int64(nextOffset) < total

	var nextCursor string
	if hasMore {
		nextCursor = LogCursor{Offset: nextOffset}.Encode()
	}

	handlers.RespondOK(c, LogSearchResponse{
		Logs:       logs,
		HasMore:    hasMore,
		NextCursor: nextCursor,
		Limit:      limit,
		Total:      total,
	})
}

func (h *LogHandler) GetLogFacets(c *gin.Context) {
	f := h.enrichFilters(c, 60*60*1000)

	resp, err := h.repo.GetLogFacets(c.Request.Context(), f)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log facets")
		return
	}
	handlers.RespondOK(c, resp)
}

func (h *LogHandler) GetLogHistogram(c *gin.Context) {
	f := h.enrichFilters(c, 60*60*1000)
	step := c.Query("step")

	buckets, err := h.repo.GetLogHistogram(c.Request.Context(), f, step)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log histogram")
		return
	}

	if step == "" {
		step = autoStep(f.StartMs, f.EndMs)
	}
	handlers.RespondOK(c, LogHistogramData{Buckets: buckets, Step: step})
}

func (h *LogHandler) GetLogVolume(c *gin.Context) {
	f := h.enrichFilters(c, 60*60*1000)
	step := c.Query("step")

	buckets, err := h.repo.GetLogVolume(c.Request.Context(), f, step)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log volume")
		return
	}

	if step == "" {
		step = autoStep(f.StartMs, f.EndMs)
	}
	handlers.RespondOK(c, LogVolumeData{Buckets: buckets, Step: step})
}

func (h *LogHandler) GetLogStats(c *gin.Context) {
	f := h.enrichFilters(c, 60*60*1000)

	resp, err := h.repo.GetLogStats(c.Request.Context(), f)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log stats")
		return
	}
	handlers.RespondOK(c, resp)
}

func (h *LogHandler) GetLogFields(c *gin.Context) {
	f := h.enrichFilters(c, 60*60*1000)
	field := c.Query("field")

	allowed := map[string]string{
		"level": "level", "service_name": "service_name", "host": "host",
		"pod": "pod", "container": "container", "logger": "logger",
	}
	col, ok := allowed[field]
	if !ok {
		handlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "field is required and must be one of: level, service_name, host, pod, container, logger")
		return
	}

	facets, err := h.repo.GetLogFields(c.Request.Context(), f, col)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query field values")
		return
	}
	handlers.RespondOK(c, map[string]any{"field": field, "values": facets})
}

func (h *LogHandler) GetLogSurrounding(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	logID := handlers.ParseInt64Param(c, "id", 0)
	if logID == 0 {
		handlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "id is required")
		return
	}
	before := handlers.ParseIntParam(c, "before", 10)
	after := handlers.ParseIntParam(c, "after", 10)
	if before > 100 {
		before = 100
	}
	if after > 100 {
		after = 100
	}

	anchor, beforeLogs, afterLogs, err := h.repo.GetLogSurrounding(c.Request.Context(), teamUUID, logID, before, after)
	if err != nil {
		handlers.RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "Log entry not found")
		return
	}
	handlers.RespondOK(c, LogSurroundingResponse{
		Anchor: anchor,
		Before: beforeLogs,
		After:  afterLogs,
	})
}

func (h *LogHandler) GetLogDetail(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	traceID := c.Query("traceId")
	spanID := c.Query("spanId")
	timestamp := handlers.ParseInt64Param(c, "timestamp", 0)
	window := handlers.ParseIntParam(c, "contextWindow", 30)

	if traceID == "" || spanID == "" {
		handlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "traceId and spanId are required")
		return
	}
	if timestamp == 0 {
		handlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "timestamp is required")
		return
	}

	center := time.UnixMilli(timestamp).UTC()
	from := center.Add(-time.Duration(window) * time.Second)
	to := center.Add(time.Duration(window) * time.Second)

	log, contextLogs, err := h.repo.GetLogDetail(c.Request.Context(), teamUUID, traceID, spanID, center, from, to)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log detail")
		return
	}
	handlers.RespondOK(c, LogDetailResponse{
		Log:         log,
		ContextLogs: contextLogs,
	})
}

func (h *LogHandler) GetTraceLogs(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	traceID := c.Param("traceId")

	if traceID == "" {
		handlers.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "traceId is required")
		return
	}

	resp, err := h.repo.GetTraceLogs(c.Request.Context(), teamUUID, traceID)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query trace logs")
		return
	}
	// Return the logs array directly to preserve the API contract.
	handlers.RespondOK(c, resp.Logs)
}
