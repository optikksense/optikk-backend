package logs

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/common"
)

type LogHandler struct {
	getTenant common.GetTenantFunc
	repo      *ClickHouseRepository
}

func NewHandler(getTenant common.GetTenantFunc, repo *ClickHouseRepository) *LogHandler {
	return &LogHandler{
		getTenant: getTenant,
		repo:      repo,
	}
}

func (h *LogHandler) parseFilters(c *gin.Context) LogFilters {
	return LogFilters{
		Severities:        common.ParseListParam(c, "severities"),
		Services:          common.ParseListParam(c, "services"),
		Hosts:             common.ParseListParam(c, "hosts"),
		Pods:              common.ParseListParam(c, "pods"),
		Containers:        common.ParseListParam(c, "containers"),
		Environments:      common.ParseListParam(c, "environments"),
		TraceID:           c.Query("traceId"),
		SpanID:            c.Query("spanId"),
		Search:            c.Query("search"),
		ExcludeSeverities: common.ParseListParam(c, "excludeSeverities"),
		ExcludeServices:   common.ParseListParam(c, "excludeServices"),
		ExcludeHosts:      common.ParseListParam(c, "excludeHosts"),
	}
}

func (h *LogHandler) enrichFilters(c *gin.Context, defaultRangeMs int64) LogFilters {
	f := h.parseFilters(c)
	f.TeamUUID = h.getTenant(c).TeamUUID()
	startMs, endMs := common.ParseRange(c, defaultRangeMs)
	f.StartMs = startMs
	f.EndMs = endMs
	return f
}

func (h *LogHandler) GetLogs(c *gin.Context) {
	f := h.enrichFilters(c, 60*60*1000)
	limit := common.ParseIntParam(c, "limit", 100)
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
			common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "Invalid cursor")
			return
		}
		cursor = parsedCursor
	}

	logs, total, err := h.repo.GetLogs(c.Request.Context(), f, limit, direction, cursor)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query logs")
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

	common.RespondOK(c, LogSearchResponse{
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
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log facets")
		return
	}
	common.RespondOK(c, resp)
}

func (h *LogHandler) GetLogHistogram(c *gin.Context) {
	f := h.enrichFilters(c, 60*60*1000)
	step := c.Query("step")

	buckets, err := h.repo.GetLogHistogram(c.Request.Context(), f, step)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log histogram")
		return
	}

	common.RespondOK(c, LogHistogramData{Buckets: buckets, Step: step})
}

func (h *LogHandler) GetLogVolume(c *gin.Context) {
	f := h.enrichFilters(c, 60*60*1000)
	step := c.Query("step")

	buckets, err := h.repo.GetLogVolume(c.Request.Context(), f, step)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log volume")
		return
	}

	common.RespondOK(c, LogVolumeData{Buckets: buckets, Step: step})
}

func (h *LogHandler) GetLogStats(c *gin.Context) {
	f := h.enrichFilters(c, 60*60*1000)

	resp, err := h.repo.GetLogStats(c.Request.Context(), f)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log stats")
		return
	}
	common.RespondOK(c, resp)
}

func (h *LogHandler) GetLogFields(c *gin.Context) {
	f := h.enrichFilters(c, 60*60*1000)
	field := c.Query("field")

	allowed := map[string]string{
		"severity_text": "severity_text", "service": "service", "host": "host",
		"pod": "pod", "container": "container", "scope_name": "scope_name",
		"environment": "environment",
	}
	col, ok := allowed[field]
	if !ok {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "field is required and must be one of: severity_text, service, host, pod, container, scope_name, environment")
		return
	}

	facets, err := h.repo.GetLogFields(c.Request.Context(), f, col)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query field values")
		return
	}
	common.RespondOK(c, map[string]any{"field": field, "values": facets})
}

func (h *LogHandler) GetLogSurrounding(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	logID := strings.TrimSpace(c.Query("id"))
	if logID == "" {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "id is required")
		return
	}
	before := common.ParseIntParam(c, "before", 10)
	after := common.ParseIntParam(c, "after", 10)
	if before > 100 {
		before = 100
	}
	if after > 100 {
		after = 100
	}

	anchor, beforeLogs, afterLogs, err := h.repo.GetLogSurrounding(c.Request.Context(), teamUUID, logID, before, after)
	if err != nil {
		common.RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "Log entry not found")
		return
	}
	common.RespondOK(c, LogSurroundingResponse{
		Anchor: anchor,
		Before: beforeLogs,
		After:  afterLogs,
	})
}

func (h *LogHandler) GetLogDetail(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	traceID := c.Query("traceId")
	spanID := c.Query("spanId")
	timestampMs := common.ParseInt64Param(c, "timestamp", 0)
	window := common.ParseIntParam(c, "contextWindow", 30)

	if traceID == "" || spanID == "" {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "traceId and spanId are required")
		return
	}
	if timestampMs == 0 {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "timestamp is required")
		return
	}

	// Convert ms to nanoseconds.
	centerNs := uint64(timestampMs) * 1_000_000
	windowNs := uint64(window) * 1_000_000_000
	fromNs := centerNs - windowNs
	toNs := centerNs + windowNs

	log, contextLogs, err := h.repo.GetLogDetail(c.Request.Context(), teamUUID, traceID, spanID, centerNs, fromNs, toNs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log detail")
		return
	}
	common.RespondOK(c, LogDetailResponse{
		Log:         log,
		ContextLogs: contextLogs,
	})
}

func (h *LogHandler) GetTraceLogs(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	traceID := c.Param("traceId")

	if traceID == "" {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "traceId is required")
		return
	}

	resp, err := h.repo.GetTraceLogs(c.Request.Context(), teamUUID, traceID)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query trace logs")
		return
	}
	common.RespondOK(c, resp.Logs)
}
