package logs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

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
	f := LogFilters{
		Severities:        common.ParseListParam(c, "severities"),
		Services:          common.ParseListParam(c, "services"),
		Hosts:             common.ParseListParam(c, "hosts"),
		Pods:              common.ParseListParam(c, "pods"),
		Containers:        common.ParseListParam(c, "containers"),
		Environments:      common.ParseListParam(c, "environments"),
		TraceID:           c.Query("traceId"),
		SpanID:            c.Query("spanId"),
		Search:            c.Query("search"),
		SearchMode:        c.Query("searchMode"), // "exact" or "" (ngram default)
		ExcludeSeverities: common.ParseListParam(c, "excludeSeverities"),
		ExcludeServices:   common.ParseListParam(c, "excludeServices"),
		ExcludeHosts:      common.ParseListParam(c, "excludeHosts"),
	}

	// Parse structured attribute filters from query params.
	// Format: attr[key]=value or attr[key][op]=value
	// Simple form: ?attr.user_id=123&attr.env=prod
	for key, vals := range c.Request.URL.Query() {
		if after, ok := strings.CutPrefix(key, "attr."); ok && len(vals) > 0 {
			f.AttributeFilters = append(f.AttributeFilters, LogAttributeFilter{
				Key:   after,
				Value: vals[0],
				Op:    "eq",
			})
		}
		if after, ok := strings.CutPrefix(key, "attr_neq."); ok && len(vals) > 0 {
			f.AttributeFilters = append(f.AttributeFilters, LogAttributeFilter{
				Key:   after,
				Value: vals[0],
				Op:    "neq",
			})
		}
		if after, ok := strings.CutPrefix(key, "attr_contains."); ok && len(vals) > 0 {
			f.AttributeFilters = append(f.AttributeFilters, LogAttributeFilter{
				Key:   after,
				Value: vals[0],
				Op:    "contains",
			})
		}
		if after, ok := strings.CutPrefix(key, "attr_regex."); ok && len(vals) > 0 {
			f.AttributeFilters = append(f.AttributeFilters, LogAttributeFilter{
				Key:   after,
				Value: vals[0],
				Op:    "regex",
			})
		}
	}
	return f
}

func (h *LogHandler) enrichFilters(c *gin.Context) (LogFilters, bool) {
	f := h.parseFilters(c)
	f.TeamID = h.getTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return LogFilters{}, false
	}
	f.StartMs = startMs
	f.EndMs = endMs
	return f, true
}

func (h *LogHandler) GetLogs(c *gin.Context) {
	f, ok := h.enrichFilters(c)
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

func (h *LogHandler) GetLogHistogram(c *gin.Context) {
	f, ok := h.enrichFilters(c)
	if !ok {
		return
	}
	step := c.Query("step")

	buckets, err := h.repo.GetLogHistogram(c.Request.Context(), f, step)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log histogram")
		return
	}

	common.RespondOK(c, LogHistogramData{Buckets: buckets, Step: step})
}

func (h *LogHandler) GetLogVolume(c *gin.Context) {
	f, ok := h.enrichFilters(c)
	if !ok {
		return
	}
	step := c.Query("step")

	buckets, err := h.repo.GetLogVolume(c.Request.Context(), f, step)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log volume")
		return
	}

	common.RespondOK(c, LogVolumeData{Buckets: buckets, Step: step})
}

func (h *LogHandler) GetLogStats(c *gin.Context) {
	f, ok := h.enrichFilters(c)
	if !ok {
		return
	}

	resp, err := h.repo.GetLogStats(c.Request.Context(), f)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query log stats")
		return
	}
	common.RespondOK(c, resp)
}

func (h *LogHandler) GetLogFields(c *gin.Context) {
	f, ok := h.enrichFilters(c)
	if !ok {
		return
	}
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
	teamID := h.getTenant(c).TeamID
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

	anchor, beforeLogs, afterLogs, err := h.repo.GetLogSurrounding(c.Request.Context(), teamID, logID, before, after)
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
	teamID := h.getTenant(c).TeamID
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

	log, contextLogs, err := h.repo.GetLogDetail(c.Request.Context(), teamID, traceID, spanID, centerNs, fromNs, toNs)
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
	teamID := h.getTenant(c).TeamID
	traceID := c.Param("traceId")

	if traceID == "" {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", "traceId is required")
		return
	}

	resp, err := h.repo.GetTraceLogs(c.Request.Context(), teamID, traceID)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query trace logs")
		return
	}
	common.RespondOK(c, resp.Logs)
}

func (h *LogHandler) GetLogAggregate(c *gin.Context) {
	f, ok := h.enrichFilters(c)
	if !ok {
		return
	}

	var req LogAggregateRequest
	_ = c.ShouldBindQuery(&req)
	if req.GroupBy == "" {
		req.GroupBy = "service"
	}
	if req.Step == "" {
		req.Step = "5m"
	}
	if req.TopN <= 0 {
		req.TopN = 20
	}
	if req.Metric == "" {
		req.Metric = "count"
	}

	rows, err := h.repo.GetLogAggregate(c.Request.Context(), f, req)
	if err != nil {
		common.RespondError(c, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
		return
	}
	common.RespondOK(c, map[string]any{
		"group_by": req.GroupBy,
		"step":     req.Step,
		"metric":   req.Metric,
		"rows":     rows,
	})
}

func (h *LogHandler) StreamLogs(c *gin.Context) {
	f, ok := h.enrichFilters(c)
	if !ok {
		return
	}

	const pollInterval = 2 * time.Second
	const heartbeatInterval = 15 * time.Second
	const maxLogsPerPoll = 50

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no") // disable nginx buffering

	ctx := c.Request.Context()
	flusher, canFlush := c.Writer.(http.Flusher)

	// Track the latest timestamp we have already sent (nanoseconds).
	// Start from EndMs so we only stream logs newer than the request.
	latestNs := uint64(f.EndMs) * 1_000_000

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
			// Slide the filter window: [latestNs, now].
			nowMs := time.Now().UnixMilli()
			poll := f
			poll.StartMs = int64(latestNs/1_000_000) + 1 // exclusive lower bound
			poll.EndMs = nowMs

			// Use cursor-free, ascending, small-limit fetch.
			logs, _, err := h.repo.GetLogs(ctx, poll, maxLogsPerPoll, "asc", LogCursor{})
			if err != nil {
				// Don't close stream on transient error; retry next tick.
				continue
			}

			for _, l := range logs {
				b, err := json.Marshal(l)
				if err != nil {
					continue
				}
				fmt.Fprintf(c.Writer, "data: %s\n\n", b)
				if l.Timestamp > latestNs {
					latestNs = l.Timestamp
				}
			}
			if canFlush && len(logs) > 0 {
				flusher.Flush()
			}
		}
	}
}
