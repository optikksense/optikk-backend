package metrics

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/internal/platform/handlers"
)

// MetricHandler handles HTTP requests for metrics.
type MetricHandler struct {
	getTenant handlers.GetTenantFunc
	repo      *ClickHouseRepository
}

// NewHandler creates a new metrics handler.
func NewHandler(getTenant handlers.GetTenantFunc, repo *ClickHouseRepository) *MetricHandler {
	return &MetricHandler{
		getTenant: getTenant,
		repo:      repo,
	}
}

// parseFilters extracts common MetricFilters from the request.
func (h *MetricHandler) parseFilters(c *gin.Context) MetricFilters {
	teamUUID := h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)
	return MetricFilters{
		TeamUUID:    teamUUID,
		Start:       time.UnixMilli(startMs),
		End:         time.UnixMilli(endMs),
		ServiceName: c.Query("serviceName"),
	}
}

func (h *MetricHandler) GetServiceMetrics(c *gin.Context) {
	f := h.parseFilters(c)
	metrics, err := h.repo.GetServiceMetrics(c.Request.Context(), f)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query service metrics")
		return
	}
	handlers.RespondOK(c, metrics)
}

func (h *MetricHandler) GetServiceEndpointBreakdown(c *gin.Context) {
	f := h.parseFilters(c)
	f.ServiceName = c.Param("serviceName")
	metrics, err := h.repo.GetEndpointMetrics(c.Request.Context(), f)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query endpoint breakdown")
		return
	}
	handlers.RespondOK(c, metrics)
}

func (h *MetricHandler) GetEndpointMetrics(c *gin.Context) {
	f := h.parseFilters(c)
	metrics, err := h.repo.GetEndpointMetrics(c.Request.Context(), f)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query endpoint metrics")
		return
	}
	handlers.RespondOK(c, metrics)
}

func (h *MetricHandler) GetMetricsTimeSeries(c *gin.Context) {
	f := h.parseFilters(c)
	points, err := h.repo.GetServiceTimeSeries(c.Request.Context(), f)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query metrics timeseries")
		return
	}
	handlers.RespondOK(c, points)
}

func (h *MetricHandler) GetMetricsSummary(c *gin.Context) {
	f := h.parseFilters(c)
	summary, err := h.repo.GetMetricsSummary(c.Request.Context(), f)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query metrics summary")
		return
	}
	handlers.RespondOK(c, summary)
}

func (h *MetricHandler) GetEndpointTimeSeries(c *gin.Context) {
	f := h.parseFilters(c)
	points, err := h.repo.GetEndpointTimeSeries(c.Request.Context(), f)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query endpoint timeseries")
		return
	}
	handlers.RespondOK(c, points)
}

func (h *MetricHandler) GetServiceTopology(c *gin.Context) {
	f := h.parseFilters(c)
	topology, err := h.repo.GetTopology(c.Request.Context(), f)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query topology")
		return
	}
	handlers.RespondOK(c, topology)
}

func (h *MetricHandler) GetSystemStatus(c *gin.Context) {
	handlers.RespondOK(c, map[string]any{
		"version": "v2-go",
		"tables":  []string{"spans", "logs", "incidents", "metrics", "deployments", "health_check_results"},
	})
}
