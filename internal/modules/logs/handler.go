package logs

import (
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// LogHandler handles all log-related API endpoints.
type LogHandler struct {
	modulecommon.DBTenant
	Repo *Repository
}

func parseFilters(c *gin.Context, teamUUID string, startMs, endMs int64) LogFilters {
	return LogFilters{
		TeamUUID:        teamUUID,
		StartMs:         startMs,
		EndMs:           endMs,
		Levels:          ParseListParam(c, "levels"),
		Services:        ParseListParam(c, "services"),
		Hosts:           ParseListParam(c, "hosts"),
		Pods:            ParseListParam(c, "pods"),
		Containers:      ParseListParam(c, "containers"),
		Loggers:         ParseListParam(c, "loggers"),
		TraceID:         c.Query("traceId"),
		SpanID:          c.Query("spanId"),
		Search:          c.Query("search"),
		ExcludeLevels:   ParseListParam(c, "excludeLevels"),
		ExcludeServices: ParseListParam(c, "excludeServices"),
		ExcludeHosts:    ParseListParam(c, "excludeHosts"),
	}
}

func logBucketExpr(step string) string {
	switch step {
	case "5m":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL MINUTE(timestamp) MOD 5 MINUTE), '%Y-%m-%d %H:%i:00')`
	case "10m":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL MINUTE(timestamp) MOD 10 MINUTE), '%Y-%m-%d %H:%i:00')`
	case "15m":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL MINUTE(timestamp) MOD 15 MINUTE), '%Y-%m-%d %H:%i:00')`
	case "30m":
		return `DATE_FORMAT(DATE_SUB(timestamp, INTERVAL MINUTE(timestamp) MOD 30 MINUTE), '%Y-%m-%d %H:%i:00')`
	case "1h":
		return `DATE_FORMAT(timestamp, '%Y-%m-%d %H:00:00')`
	default: // "1m"
		return `DATE_FORMAT(timestamp, '%Y-%m-%d %H:%i:00')`
	}
}

func autoStep(startMs, endMs int64) string {
	ms := endMs - startMs
	switch {
	case ms <= 30*60*1000:
		return "1m"
	case ms <= 3*60*60*1000:
		return "5m"
	case ms <= 24*60*60*1000:
		return "30m"
	default:
		return "1h"
	}
}

// GetLogs — primary log search (cursor-based pagination).
func (h *LogHandler) GetLogs(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	limit := ParseIntParam(c, "limit", 100)
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	direction := c.DefaultQuery("direction", "desc")
	if direction != "asc" {
		direction = "desc"
	}
	cursor := ParseInt64Param(c, "cursor", 0)

	f := parseFilters(c, teamUUID, startMs, endMs)
	logs, total, levelFacets, serviceFacets, hostFacets, err := h.Repo.GetLogs(f, limit, direction, cursor)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query logs")
		return
	}

	var nextCursor int64
	if len(logs) == limit {
		nextCursor = Int64FromAny(logs[len(logs)-1]["id"])
	}

	RespondOK(c, map[string]any{
		"logs":       NormalizeRows(logs),
		"hasMore":    len(logs) == limit,
		"nextCursor": nextCursor,
		"limit":      limit,
		"total":      total,
		"facets": map[string]any{
			"levels":   NormalizeRows(levelFacets),
			"services": NormalizeRows(serviceFacets),
			"hosts":    NormalizeRows(hostFacets),
		},
	})
}

// GetLogHistogram — time-bucketed log counts split by level.
func (h *LogHandler) GetLogHistogram(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	step := c.Query("step")
	if step == "" {
		step = autoStep(startMs, endMs)
	}
	bucket := logBucketExpr(step)
	f := parseFilters(c, teamUUID, startMs, endMs)

	rows, err := h.Repo.GetLogHistogram(f, bucket)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log histogram")
		return
	}
	RespondOK(c, map[string]any{"buckets": NormalizeRows(rows), "step": step})
}

// GetLogVolume — per-bucket totals with per-level breakdown columns.
func (h *LogHandler) GetLogVolume(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	step := c.Query("step")
	if step == "" {
		step = autoStep(startMs, endMs)
	}
	bucket := logBucketExpr(step)
	f := parseFilters(c, teamUUID, startMs, endMs)

	rows, err := h.Repo.GetLogVolume(f, bucket)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log volume")
		return
	}
	RespondOK(c, map[string]any{"buckets": NormalizeRows(rows), "step": step})
}

// GetLogStats — field distribution counts for the current filter window.
func (h *LogHandler) GetLogStats(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	f := parseFilters(c, teamUUID, startMs, endMs)

	total, levelRows, serviceRows, hostRows, podRows, loggerRows, err := h.Repo.GetLogStats(f)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log stats")
		return
	}

	RespondOK(c, map[string]any{
		"total": total,
		"fields": map[string]any{
			"level":        NormalizeRows(levelRows),
			"service_name": NormalizeRows(serviceRows),
			"host":         NormalizeRows(hostRows),
			"pod":          NormalizeRows(podRows),
			"logger":       NormalizeRows(loggerRows),
		},
	})
}

// GetLogFields — cardinality for a single field (Explore label browser).
func (h *LogHandler) GetLogFields(c *gin.Context) {
	field := c.Query("field")
	allowed := map[string]string{
		"level":        "level",
		"service_name": "service_name",
		"host":         "host",
		"pod":          "pod",
		"container":    "container",
		"logger":       "logger",
	}
	col, ok := allowed[field]
	if !ok {
		RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR",
			"unsupported field; allowed: level, service_name, host, pod, container, logger")
		return
	}

	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	f := parseFilters(c, teamUUID, startMs, endMs)

	rows, err := h.Repo.GetLogFields(f, col)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query field values")
		return
	}
	RespondOK(c, map[string]any{"field": field, "values": NormalizeRows(rows)})
}

// GetLogSurrounding — N lines before/after a specific log entry.
func (h *LogHandler) GetLogSurrounding(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	logID := ParseInt64Param(c, "id", 0)
	before := ParseIntParam(c, "before", 10)
	after := ParseIntParam(c, "after", 10)
	if before > 100 {
		before = 100
	}
	if after > 100 {
		after = 100
	}

	anchor, beforeRows, afterRows, err := h.Repo.GetLogSurrounding(teamUUID, logID, before, after)
	if err != nil || anchor == nil {
		RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "Log entry not found")
		return
	}

	RespondOK(c, map[string]any{
		"anchor": anchor,
		"before": NormalizeRows(beforeRows),
		"after":  NormalizeRows(afterRows),
	})
}

// GetLogDetail — single log + surrounding service context within a time window.
func (h *LogHandler) GetLogDetail(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	traceID := c.Query("traceId")
	spanID := c.Query("spanId")
	timestamp := ParseInt64Param(c, "timestamp", time.Now().UnixMilli())
	window := ParseIntParam(c, "contextWindow", 30)
	center := time.UnixMilli(timestamp).UTC() // Use UTC explicitly
	from := center.Add(-time.Duration(window) * time.Second)
	to := center.Add(time.Duration(window) * time.Second)

	logRow, contextLogs, err := h.Repo.GetLogDetail(teamUUID, traceID, spanID, center, from, to)
	if err != nil || logRow == nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log detail")
		return
	}

	RespondOK(c, map[string]any{
		"log":         logRow,
		"contextLogs": NormalizeRows(contextLogs),
	})
}

// GetTraceLogs — all logs correlated with a trace id.
func (h *LogHandler) GetTraceLogs(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	traceID := c.Param("traceId")
	
	rows, err := h.Repo.GetTraceLogs(teamUUID, traceID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query trace logs")
		return
	}
	RespondOK(c, NormalizeRows(rows))
}
