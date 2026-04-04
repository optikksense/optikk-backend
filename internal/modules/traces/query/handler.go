package query

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

const maxAttributeFilters = 10

type TraceService interface {
	SearchTraces(ctx context.Context, filters TraceFilters, limit int, cursorRaw string, offset int) (TraceSearchResult, error)
	GetTraceSpans(ctx context.Context, teamID int64, traceID string) ([]Span, error)
	GetSpanTree(ctx context.Context, teamID int64, spanID string) ([]Span, error)
	GetServiceDependencies(ctx context.Context, teamID int64, startMs, endMs int64) ([]ServiceDependency, error)
	GetErrorGroups(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string, limit int) ([]ErrorGroup, error)
	GetErrorTimeSeries(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]ErrorTimeSeries, error)
	GetLatencyHistogram(ctx context.Context, teamID int64, startMs, endMs int64, serviceName, operationName string) ([]LatencyHistogramBucket, error)
	GetLatencyHeatmap(ctx context.Context, teamID int64, startMs, endMs int64, serviceName string) ([]LatencyHeatmapPoint, error)
}

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
		if len(filters) >= maxAttributeFilters {
			break
		}
	}
	return filters
}

type TraceHandler struct {
	modulecommon.DBTenant
	Service TraceService
}

func NewHandler(getTenant modulecommon.GetTenantFunc, service TraceService) *TraceHandler {
	return &TraceHandler{
		DBTenant: modulecommon.DBTenant{GetTenant: getTenant},
		Service:  service,
	}
}

func (h *TraceHandler) buildFilters(c *gin.Context, teamID int64, startMs, endMs int64) TraceFilters {
	services := modulecommon.ParseListParam(c, "services")
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
		TeamID:           teamID,
		StartMs:          startMs,
		EndMs:            endMs,
		Services:         services,
		Status:           c.Query("status"),
		SearchText:       strings.TrimSpace(c.Query("search")),
		MinDuration:      c.Query("minDuration"),
		MaxDuration:      c.Query("maxDuration"),
		TraceID:          c.Query("traceId"),
		Operation:        operation,
		HTTPMethod:       c.Query("httpMethod"),
		HTTPStatus:       httpStatus,
		SearchMode:       c.DefaultQuery("mode", SearchModeAll),
		SpanKind:         c.Query("spanKind"),
		SpanName:         c.Query("spanName"),
		AttributeFilters: parseAttributeFilters(c),
	}
}

// --- Trace search handlers ---

func (h *TraceHandler) GetTraces(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	limit := modulecommon.ParseIntParam(c, "limit", 100)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	filters := h.buildFilters(c, teamID, startMs, endMs)
	result, err := h.Service.SearchTraces(c.Request.Context(), filters, limit, c.Query("cursor"), modulecommon.ParseIntParam(c, "offset", 0))
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query traces", err)
		return
	}
	if result.UsesKeyset {
		modulecommon.RespondOK(c, TraceCursorResponse{
			Traces:     result.Traces,
			HasMore:    result.HasMore,
			NextCursor: result.NextCursor,
			Limit:      result.Limit,
			Total:      result.Total,
			Summary:    result.Summary,
		})
		return
	}
	modulecommon.RespondOK(c, TraceSearchResponse{
		Traces:  result.Traces,
		HasMore: result.HasMore,
		Offset:  result.Offset,
		Limit:   result.Limit,
		Total:   result.Total,
		Summary: result.Summary,
	})
}

func (h *TraceHandler) GetTracesKeyset(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	limit := modulecommon.ParseIntParam(c, "limit", 100)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	filters := h.buildFilters(c, teamID, startMs, endMs)
	result, err := h.Service.SearchTraces(c.Request.Context(), filters, limit, c.Query("cursor"), 0)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query traces", err)
		return
	}
	modulecommon.RespondOK(c, TraceCursorResponse{
		Traces:     result.Traces,
		HasMore:    result.HasMore,
		NextCursor: result.NextCursor,
		Limit:      result.Limit,
		Summary:    result.Summary,
	})
}

// GetSpanSearch is the span-level search endpoint.
// It forces SearchMode="all" so all spans (not just root) are searched.
func (h *TraceHandler) GetSpanSearch(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	limit := modulecommon.ParseIntParam(c, "limit", 100)
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	filters := h.buildFilters(c, teamID, startMs, endMs)
	filters.SearchMode = SearchModeAll
	result, err := h.Service.SearchTraces(c.Request.Context(), filters, limit, c.Query("cursor"), 0)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query spans", err)
		return
	}
	modulecommon.RespondOK(c, SpanSearchResponse{
		Spans:      result.Traces,
		HasMore:    result.HasMore,
		NextCursor: result.NextCursor,
		Limit:      result.Limit,
		Summary:    result.Summary,
	})
}

// --- Trace detail handlers ---

func (h *TraceHandler) GetTraceSpans(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	traceID := c.Param("traceId")

	spans, err := h.Service.GetTraceSpans(c.Request.Context(), teamID, traceID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query trace spans", err)
		return
	}
	modulecommon.RespondOK(c, spans)
}

func (h *TraceHandler) GetSpanTree(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	spanID := c.Param("spanId")

	spans, err := h.Service.GetSpanTree(c.Request.Context(), teamID, spanID)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query span tree", err)
		return
	}
	modulecommon.RespondOK(c, spans)
}

func (h *TraceHandler) GetServiceDependencies(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	deps, err := h.Service.GetServiceDependencies(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query dependencies", err)
		return
	}
	modulecommon.RespondOK(c, deps)
}

// --- Error handlers ---

func (h *TraceHandler) GetErrorGroups(c *gin.Context) {
	serviceName := c.Query("serviceName")
	limit := modulecommon.ParseIntParam(c, "limit", 100)
	h.getErrorGroupsInternal(c, serviceName, limit)
}

func (h *TraceHandler) GetServiceErrors(c *gin.Context) {
	serviceName := c.Param("serviceName")
	h.getErrorGroupsInternal(c, serviceName, 50)
}

func (h *TraceHandler) getErrorGroupsInternal(c *gin.Context, serviceName string, limit int) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	groups, err := h.Service.GetErrorGroups(c.Request.Context(), teamID, startMs, endMs, serviceName, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query errors", err)
		return
	}
	modulecommon.RespondOK(c, groups)
}

func (h *TraceHandler) GetErrorTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetErrorTimeSeries(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query error timeseries", err)
		return
	}
	modulecommon.RespondOK(c, points)
}

func (h *TraceHandler) GetServiceErrorTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Param("serviceName")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetErrorTimeSeries(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query service error timeseries", err)
		return
	}
	modulecommon.RespondOK(c, points)
}

// --- Latency handlers ---

func (h *TraceHandler) GetLatencyHistogram(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	operationName := c.Query("operationName")
	if operationName == "" {
		operationName = c.Query("operation")
	}
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	buckets, err := h.Service.GetLatencyHistogram(c.Request.Context(), teamID, startMs, endMs, serviceName, operationName)
	if err != nil {
		slog.Error("latency histogram query failed", slog.Any("error", err))
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency histogram", err)
		return
	}
	modulecommon.RespondOK(c, buckets)
}

func (h *TraceHandler) GetLatencyHeatmap(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		serviceName = c.Query("service")
	}
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetLatencyHeatmap(c.Request.Context(), teamID, startMs, endMs, serviceName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query latency heatmap", err)
		return
	}
	modulecommon.RespondOK(c, points)
}
