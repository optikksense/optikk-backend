package traces

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/modules/common"
)

// parseAttributeFilters reads ?attr.key=value / ?attr_neq.key=value / etc. from the query string.
func parseAttributeFilters(c *gin.Context) []SpanAttributeFilter {
	var filters []SpanAttributeFilter
	for key, vals := range c.Request.URL.Query() {
		if len(vals) == 0 {
			continue
		}
		if after, ok := strings.CutPrefix(key, "attr."); ok {
			filters = append(filters, SpanAttributeFilter{Key: after, Value: vals[0], Op: "eq"})
		} else if after, ok := strings.CutPrefix(key, "attr_neq."); ok {
			filters = append(filters, SpanAttributeFilter{Key: after, Value: vals[0], Op: "neq"})
		} else if after, ok := strings.CutPrefix(key, "attr_contains."); ok {
			filters = append(filters, SpanAttributeFilter{Key: after, Value: vals[0], Op: "contains"})
		} else if after, ok := strings.CutPrefix(key, "attr_regex."); ok {
			filters = append(filters, SpanAttributeFilter{Key: after, Value: vals[0], Op: "regex"})
		}
	}
	return filters
}

// encodeCursor base64-encodes a TraceCursor so it's URL-safe.
func encodeCursor(cur TraceCursor) string {
	b, _ := json.Marshal(cur)
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeCursor parses a base64-encoded TraceCursor; returns zero value on error.
func decodeCursor(raw string) TraceCursor {
	if raw == "" {
		return TraceCursor{}
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return TraceCursor{}
	}
	var cur TraceCursor
	_ = json.Unmarshal(b, &cur)
	return cur
}

type TraceHandler struct {
	getTenant common.GetTenantFunc
	repo      *ClickHouseRepository
}

func NewHandler(getTenant common.GetTenantFunc, repo *ClickHouseRepository) *TraceHandler {
	return &TraceHandler{
		getTenant: getTenant,
		repo:      repo,
	}
}

func (h *TraceHandler) buildFilters(c *gin.Context, teamID int64, startMs, endMs int64) TraceFilters {
	services := common.ParseListParam(c, "services")
	if len(services) == 0 {
		if singleService := c.Query("service"); singleService != "" {
			services = []string{singleService}
		}
	}
	operation := c.Query("operationName")
	if operation == "" {
		operation = c.Query("operation")
	}
	httpStatus := c.Query("httpStatusCode")
	if httpStatus == "" {
		httpStatus = c.Query("http.status_code")
	}
	return TraceFilters{
		TeamID:         teamID,
		StartMs:          startMs,
		EndMs:            endMs,
		Services:         services,
		Status:           c.Query("status"),
		MinDuration:      c.Query("minDuration"),
		MaxDuration:      c.Query("maxDuration"),
		TraceID:          c.Query("traceId"),
		Operation:        operation,
		HTTPMethod:       c.Query("httpMethod"),
		HTTPStatus:       httpStatus,
		AttributeFilters: parseAttributeFilters(c),
	}
}

func (h *TraceHandler) GetTraces(c *gin.Context) {
	teamID := h.getTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	limit := common.ParseIntParam(c, "limit", 100)
	offset := common.ParseIntParam(c, "offset", 0)
	filters := h.buildFilters(c, teamID, startMs, endMs)

	traces, total, summary, err := h.repo.GetTraces(c.Request.Context(), filters, limit, offset)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query traces")
		return
	}
	common.RespondOK(c, TraceSearchResponse{
		Traces:  traces,
		HasMore: len(traces) >= limit,
		Offset:  offset,
		Limit:   limit,
		Total:   total,
		Summary: summary,
	})
}

// GetTracesKeyset is an alternative to GetTraces using cursor-based pagination.
// Clients pass ?cursor=<opaque> from the previous response's nextCursor field.
// GET /traces/keyset
func (h *TraceHandler) GetTracesKeyset(c *gin.Context) {
	teamID := h.getTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	limit := common.ParseIntParam(c, "limit", 100)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	filters := h.buildFilters(c, teamID, startMs, endMs)
	cursor := decodeCursor(c.Query("cursor"))

	traces, summary, hasMore, err := h.repo.GetTracesKeyset(c.Request.Context(), filters, limit, cursor)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query traces")
		return
	}

	var nextCursor string
	if hasMore && len(traces) > 0 {
		last := traces[len(traces)-1]
		nextCursor = encodeCursor(TraceCursor{Timestamp: last.StartTime, SpanID: last.SpanID})
	}

	common.RespondOK(c, map[string]any{
		"traces":     traces,
		"hasMore":    hasMore,
		"nextCursor": nextCursor,
		"limit":      limit,
		"summary":    summary,
	})
}

// GetOperationAggregation returns per-operation RED metrics (aggregated traces view).
// GET /traces/operations
func (h *TraceHandler) GetOperationAggregation(c *gin.Context) {
	teamID := h.getTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	limit := common.ParseIntParam(c, "limit", 200)
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	filters := h.buildFilters(c, teamID, startMs, endMs)

	rows, err := h.repo.GetOperationAggregation(c.Request.Context(), filters, limit)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query operation aggregation")
		return
	}
	common.RespondOK(c, rows)
}

// ensure time is used (TraceCursor embeds time.Time)
var _ = time.Time{}

func (h *TraceHandler) GetTraceSpans(c *gin.Context) {
	teamID := h.getTenant(c).TeamID
	traceID := c.Param("traceId")

	spans, err := h.repo.GetTraceSpans(c.Request.Context(), teamID, traceID)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query trace spans")
		return
	}
	common.RespondOK(c, spans)
}

// GetSpanTree resolves the trace_id for the given spanId and returns all spans
// in that trace, ordered by start_time. This allows the waterfall to be driven
// by a root span_id rather than a trace_id.
func (h *TraceHandler) GetSpanTree(c *gin.Context) {
	teamID := h.getTenant(c).TeamID
	spanID := c.Param("spanId")

	spans, err := h.repo.GetSpanTree(c.Request.Context(), teamID, spanID)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query span tree")
		return
	}
	common.RespondOK(c, spans)
}

func (h *TraceHandler) GetServiceDependencies(c *gin.Context) {
	teamID := h.getTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}

	deps, err := h.repo.GetServiceDependencies(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query dependencies")
		return
	}
	common.RespondOK(c, deps)
}

func (h *TraceHandler) GetErrorGroups(c *gin.Context) {
	serviceName := c.Query("serviceName")
	limit := common.ParseIntParam(c, "limit", 100)
	h.getErrorGroupsInternal(c, serviceName, limit)
}

func (h *TraceHandler) GetServiceErrors(c *gin.Context) {
	serviceName := c.Param("serviceName")
	h.getErrorGroupsInternal(c, serviceName, 50)
}

func (h *TraceHandler) getErrorGroupsInternal(c *gin.Context, serviceName string, limit int) {
	teamID := h.getTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}

	groups, err := h.repo.GetErrorGroups(c.Request.Context(), teamID, startMs, endMs, serviceName, limit)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query errors")
		return
	}
	common.RespondOK(c, groups)
}

func (h *TraceHandler) GetErrorTimeSeries(c *gin.Context) {
	teamID := h.getTenant(c).TeamID
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.repo.GetErrorTimeSeries(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error timeseries")
		return
	}
	common.RespondOK(c, points)
}

func (h *TraceHandler) GetLatencyHistogram(c *gin.Context) {
	teamID := h.getTenant(c).TeamID
	serviceName := c.Query("serviceName")
	operationName := c.Query("operationName")
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}

	buckets, err := h.repo.GetLatencyHistogram(c.Request.Context(), teamID, startMs, endMs, serviceName, operationName)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query latency histogram")
		return
	}
	common.RespondOK(c, buckets)
}

func (h *TraceHandler) GetLatencyHeatmap(c *gin.Context) {
	teamID := h.getTenant(c).TeamID
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.repo.GetLatencyHeatmap(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query latency heatmap")
		return
	}
	common.RespondOK(c, points)
}
