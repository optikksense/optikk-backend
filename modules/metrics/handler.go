package metrics

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/observability/observability-backend-go/modules/metrics/service"
	"github.com/observability/observability-backend-go/internal/platform/handlers"
)

type MetricHandler struct {
	getTenant handlers.GetTenantFunc
	service   service.Service
}

func NewHandler(getTenant handlers.GetTenantFunc, svc service.Service) *MetricHandler {
	return &MetricHandler{
		getTenant: getTenant,
		service:   svc,
	}
}

func (h *MetricHandler) GetDashboardOverview(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	overview, err := h.service.GetDashboardOverview(c.Request.Context(), teamUUID)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query dashboard overview")
		return
	}
	handlers.RespondOK(c, overview)
}

func (h *MetricHandler) GetDashboardServices(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	services, err := h.service.GetDashboardServices(c.Request.Context(), teamUUID)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query services")
		return
	}
	handlers.RespondOK(c, services)
}

func (h *MetricHandler) GetDashboardServiceDetail(c *gin.Context) {
	serviceName := c.Param("serviceName")
	teamUUID := h.getTenant(c).TeamUUID()
	detail, err := h.service.GetDashboardServiceDetail(c.Request.Context(), teamUUID, serviceName)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query service detail")
		return
	}
	if detail.Name == "" {
		handlers.RespondError(c, http.StatusNotFound, "RESOURCE_NOT_FOUND", "Service not found")
		return
	}
	handlers.RespondOK(c, detail)
}

func (h *MetricHandler) GetServiceMetrics(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)
	metrics, err := h.service.GetServiceMetrics(c.Request.Context(), teamUUID, time.UnixMilli(startMs), time.UnixMilli(endMs))
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query service metrics")
		return
	}
	handlers.RespondOK(c, metrics)
}

func (h *MetricHandler) GetEndpointMetrics(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)
	metrics, err := h.service.GetEndpointMetrics(c.Request.Context(), teamUUID, time.UnixMilli(startMs), time.UnixMilli(endMs), serviceName)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query endpoint metrics")
		return
	}
	handlers.RespondOK(c, metrics)
}

func (h *MetricHandler) GetServiceEndpointBreakdown(c *gin.Context) {
	serviceName := c.Param("serviceName")
	teamUUID := h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)
	metrics, err := h.service.GetEndpointMetrics(c.Request.Context(), teamUUID, time.UnixMilli(startMs), time.UnixMilli(endMs), serviceName)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query endpoint breakdown")
		return
	}
	handlers.RespondOK(c, metrics)
}

func (h *MetricHandler) GetMetricsTimeSeries(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)
	points, err := h.service.GetMetricsTimeSeries(c.Request.Context(), teamUUID, time.UnixMilli(startMs), time.UnixMilli(endMs), serviceName)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query metrics timeseries")
		return
	}
	handlers.RespondOK(c, points)
}

func (h *MetricHandler) GetMetricsSummary(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)
	summary, err := h.service.GetMetricsSummary(c.Request.Context(), teamUUID, time.UnixMilli(startMs), time.UnixMilli(endMs))
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query metrics summary")
		return
	}
	handlers.RespondOK(c, summary)
}

func (h *MetricHandler) GetServiceTimeSeries(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)
	points, err := h.service.GetServiceTimeSeries(c.Request.Context(), teamUUID, time.UnixMilli(startMs), time.UnixMilli(endMs))
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query service timeseries")
		return
	}
	handlers.RespondOK(c, points)
}

func (h *MetricHandler) GetEndpointTimeSeries(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	serviceName := c.Query("serviceName")
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)
	points, err := h.service.GetEndpointTimeSeries(c.Request.Context(), teamUUID, time.UnixMilli(startMs), time.UnixMilli(endMs), serviceName)
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query endpoint timeseries")
		return
	}
	handlers.RespondOK(c, points)
}

func (h *MetricHandler) GetServiceTopology(c *gin.Context) {
	teamUUID := h.getTenant(c).TeamUUID()
	startMs, endMs := handlers.ParseRange(c, 60*60*1000)
	topology, err := h.service.GetServiceTopology(c.Request.Context(), teamUUID, time.UnixMilli(startMs), time.UnixMilli(endMs))
	if err != nil {
		handlers.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query topology")
		return
	}
	handlers.RespondOK(c, topology)
}

func (h *MetricHandler) GetSystemStatus(c *gin.Context) {
	status := h.service.GetSystemStatus(c.Request.Context())
	handlers.RespondOK(c, status)
}
