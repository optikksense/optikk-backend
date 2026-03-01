package traces

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/platform/handlers"
)

type TraceHandler struct {
	getTenant handlers.GetTenantFunc
	repo      *ClickHouseRepository
}

func NewHandler(getTenant handlers.GetTenantFunc, repo *ClickHouseRepository) *TraceHandler {
	return &TraceHandler{
		getTenant: getTenant,
		repo:      repo,
	}
}

func (h *TraceHandler) GetTraces(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)
	limit := handlers.ParseIntParam(c, "limit", 100)
	offset := handlers.ParseIntParam(c, "offset", 0)

	services := handlers.ParseListParam(c, "services")
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

	filters := TraceFilters{
		TeamUUID:    teamUUID,
		StartMs:     startMs,
		EndMs:       endMs,
		Services:    services,
		Status:      c.Query("status"),
		MinDuration: c.Query("minDuration"),
		MaxDuration: c.Query("maxDuration"),
		TraceID:     c.Query("traceId"),
		Operation:   operation,
		HTTPMethod:  c.Query("httpMethod"),
		HTTPStatus:  httpStatus,
	}

	traces, total, summary, err := h.repo.GetTraces(c.Request.Context(), filters, limit, offset)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query traces")
		return
	}
	handlers.RespondOK(c, TraceSearchResponse{
		Traces:  traces,
		HasMore: len(traces) >= limit,
		Offset:  offset,
		Limit:   limit,
		Total:   total,
		Summary: summary,
	})
}

func (h *TraceHandler) GetTraceSpans(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	traceID := c.Param("traceId")

	spans, err := h.repo.GetTraceSpans(c.Request.Context(), teamUUID, traceID)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query trace spans")
		return
	}
	handlers.RespondOK(c, spans)
}

// GetSpanTree resolves the trace_id for the given spanId and returns all spans
// in that trace, ordered by start_time. This allows the waterfall to be driven
// by a root span_id rather than a trace_id.
func (h *TraceHandler) GetSpanTree(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	spanID := c.Param("spanId")

	spans, err := h.repo.GetSpanTree(c.Request.Context(), teamUUID, spanID)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query span tree")
		return
	}
	handlers.RespondOK(c, spans)
}

func (h *TraceHandler) GetServiceDependencies(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)

	deps, err := h.repo.GetServiceDependencies(c.Request.Context(), teamUUID, startMs, endMs)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query dependencies")
		return
	}
	handlers.RespondOK(c, deps)
}

func (h *TraceHandler) GetErrorGroups(c *gin.Context) {
	serviceName := c.Query("serviceName")
	limit := handlers.ParseIntParam(c, "limit", 100)
	h.getErrorGroupsInternal(c, serviceName, limit)
}

func (h *TraceHandler) GetServiceErrors(c *gin.Context) {
	serviceName := c.Param("serviceName")
	h.getErrorGroupsInternal(c, serviceName, 50)
}

func (h *TraceHandler) getErrorGroupsInternal(c *gin.Context, serviceName string, limit int) {
	teamUUID := h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)

	groups, err := h.repo.GetErrorGroups(c.Request.Context(), teamUUID, startMs, endMs, serviceName, limit)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query errors")
		return
	}
	handlers.RespondOK(c, groups)
}

func (h *TraceHandler) GetErrorTimeSeries(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)

	points, err := h.repo.GetErrorTimeSeries(c.Request.Context(), teamUUID, startMs, endMs, serviceName)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error timeseries")
		return
	}
	handlers.RespondOK(c, points)
}

func (h *TraceHandler) GetLatencyHistogram(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	operationName := c.Query("operationName")
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)

	buckets, err := h.repo.GetLatencyHistogram(c.Request.Context(), teamUUID, startMs, endMs, serviceName, operationName)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query latency histogram")
		return
	}
	handlers.RespondOK(c, buckets)
}

func (h *TraceHandler) GetLatencyHeatmap(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)

	points, err := h.repo.GetLatencyHeatmap(c.Request.Context(), teamUUID, startMs, endMs, serviceName)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query latency heatmap")
		return
	}
	handlers.RespondOK(c, points)
}
