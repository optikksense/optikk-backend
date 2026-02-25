package traces

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"
)

// TraceHandler handles distributed-trace and span API endpoints.
type TraceHandler struct {
	modulecommon.DBTenant
	Repo *Repository
}

// GetTraces — list root spans (traces) with summary stats.
func (h *TraceHandler) GetTraces(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	limit := ParseIntParam(c, "limit", 100)
	offset := ParseIntParam(c, "offset", 0)
	services := ParseListParam(c, "services")
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

	f := TraceFilters{
		TeamUUID:    teamUUID,
		StartMs:     startMs,
		EndMs:       endMs,
		Services:    services,
		Status:      c.Query("status"),
		MinDuration: c.Query("minDuration"),
		MaxDuration: c.Query("maxDuration"),
		TraceID:     c.Query("traceId"),
		Operation:   operation,
		HTTPStatus:  httpStatus,
	}

	traces, total, summary, err := h.Repo.GetTraces(f, limit, offset)
	if err != nil {
		log.Printf("GetTraces error: %v", err)
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query traces")
		return
	}

	RespondOK(c, map[string]any{
		"traces":  NormalizeRows(traces),
		"hasMore": len(traces) >= limit,
		"offset":  offset,
		"limit":   limit,
		"total":   total,
		"summary": summary,
	})
}

// GetTraceSpans — all spans belonging to a single trace.
func (h *TraceHandler) GetTraceSpans(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	traceID := c.Param("traceId")

	rows, err := h.Repo.GetTraceSpans(teamUUID, traceID)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query trace spans")
		return
	}
	RespondOK(c, NormalizeRows(rows))
}

// GetServiceDependencies — upstream/downstream call graph derived from parent spans.
func (h *TraceHandler) GetServiceDependencies(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Repo.GetServiceDependencies(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query dependencies")
		return
	}
	RespondOK(c, NormalizeRows(rows))
}

// GetErrorGroups — aggregated error groups across all services.
func (h *TraceHandler) GetErrorGroups(c *gin.Context) {
	serviceName := c.Query("serviceName")
	limit := ParseIntParam(c, "limit", 100)
	h.getErrorGroupsInternal(c, serviceName, limit)
}

// GetServiceErrors — error groups scoped to a single service.
func (h *TraceHandler) GetServiceErrors(c *gin.Context) {
	serviceName := c.Param("serviceName")
	h.getErrorGroupsInternal(c, serviceName, 50)
}

func (h *TraceHandler) getErrorGroupsInternal(c *gin.Context, serviceName string, limit int) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Repo.GetErrorGroups(teamUUID, startMs, endMs, serviceName, limit)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query errors")
		return
	}
	RespondOK(c, NormalizeRows(rows))
}

// GetErrorTimeSeries — per-minute error rate per service.
func (h *TraceHandler) GetErrorTimeSeries(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Repo.GetErrorTimeSeries(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query error timeseries")
		return
	}
	RespondOK(c, NormalizeRows(rows))
}

// GetLatencyHistogram — distribution of span durations in buckets.
func (h *TraceHandler) GetLatencyHistogram(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	operationName := c.Query("operationName")
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Repo.GetLatencyHistogram(teamUUID, startMs, endMs, serviceName, operationName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query latency histogram")
		return
	}
	RespondOK(c, NormalizeRows(rows))
}

// GetLatencyHeatmap — 2-D heatmap of time bucket × latency bucket.
func (h *TraceHandler) GetLatencyHeatmap(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Repo.GetLatencyHeatmap(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query latency heatmap")
		return
	}
	RespondOK(c, NormalizeRows(rows))
}
