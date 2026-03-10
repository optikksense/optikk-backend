package servicepage

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// ServiceHandler handles services overview page endpoints.
type ServiceHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetTotalServices returns the total number of services in range.
func (h *ServiceHandler) GetTotalServices(c *gin.Context) {
	h.respondWithCount(c, h.Service.GetTotalServices, "Failed to query total services")
}

// GetHealthyServices returns the healthy services count.
func (h *ServiceHandler) GetHealthyServices(c *gin.Context) {
	h.respondWithCount(c, h.Service.GetHealthyServices, "Failed to query healthy services")
}

// GetDegradedServices returns the degraded services count.
func (h *ServiceHandler) GetDegradedServices(c *gin.Context) {
	h.respondWithCount(c, h.Service.GetDegradedServices, "Failed to query degraded services")
}

// GetUnhealthyServices returns the unhealthy services count.
func (h *ServiceHandler) GetUnhealthyServices(c *gin.Context) {
	h.respondWithCount(c, h.Service.GetUnhealthyServices, "Failed to query unhealthy services")
}

// GetServiceMetrics returns service-level aggregates for the services page table.
func (h *ServiceHandler) GetServiceMetrics(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetServiceMetrics(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query service metrics")
		return
	}

	RespondOK(c, rows)
}

// GetServiceTimeSeries returns service-level time series for services overview charts.
func (h *ServiceHandler) GetServiceTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	points, err := h.Service.GetServiceTimeSeries(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query service timeseries")
		return
	}

	RespondOK(c, points)
}

// GetServiceEndpoints returns endpoint-level metrics for a specific service.
func (h *ServiceHandler) GetServiceEndpoints(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Param("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	endpoints, err := h.Service.GetServiceEndpoints(teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query endpoint breakdown")
		return
	}

	RespondOK(c, endpoints)
}

func (h *ServiceHandler) respondWithCount(c *gin.Context, fn func(int64, int64, int64) (int64, error), message string) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	count, err := fn(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", message)
		return
	}

	RespondOK(c, map[string]any{"count": count})
}
