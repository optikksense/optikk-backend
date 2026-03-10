package overview

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// OverviewHandler handles overview page endpoints.
type OverviewHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetRequestRate returns per-service request-rate buckets for the summary tab.
func (h *OverviewHandler) GetRequestRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")

	points, err := h.Service.GetRequestRate(teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview request rate")
		return
	}

	RespondOK(c, points)
}

// GetErrorRate returns per-service error-rate buckets for the summary tab.
func (h *OverviewHandler) GetErrorRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")

	points, err := h.Service.GetErrorRate(teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview error rate")
		return
	}

	RespondOK(c, points)
}

// GetP95Latency returns per-service p95 latency buckets for the summary tab.
func (h *OverviewHandler) GetP95Latency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")

	points, err := h.Service.GetP95Latency(teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview p95 latency")
		return
	}

	RespondOK(c, points)
}

// GetServices returns service-level metrics for overview-derived pages.
func (h *OverviewHandler) GetServices(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetServices(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview services")
		return
	}

	RespondOK(c, rows)
}

// GetTopEndpoints returns endpoint aggregates for the overview page.
func (h *OverviewHandler) GetTopEndpoints(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")

	rows, err := h.Service.GetTopEndpoints(teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview top endpoints")
		return
	}

	RespondOK(c, rows)
}

// GetEndpointTimeSeries returns endpoint-level time-series buckets for charts.
func (h *OverviewHandler) GetEndpointTimeSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")

	rows, err := h.Service.GetEndpointTimeSeries(teamID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview endpoint time series")
		return
	}

	RespondOK(c, rows)
}
