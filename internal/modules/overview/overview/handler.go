package overview

import (
	"net/http"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/overview/overview/service"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"
)

// OverviewHandler handles overview page endpoints.
type OverviewHandler struct {
	modulecommon.DBTenant
	Service service.Service
}

// GetSummary returns the KPI summary for the overview page.
func (h *OverviewHandler) GetSummary(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	summary, err := h.Service.GetSummary(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview summary")
		return
	}

	RespondOK(c, summary)
}

// GetTimeSeries returns overview time-series buckets.
func (h *OverviewHandler) GetTimeSeries(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	serviceName := c.Query("serviceName")

	points, err := h.Service.GetTimeSeries(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview time series")
		return
	}

	RespondOK(c, points)
}

// GetServices returns service-level metrics for overview-derived pages.
func (h *OverviewHandler) GetServices(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Service.GetServices(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview services")
		return
	}

	RespondOK(c, rows)
}

// GetEndpointMetrics returns endpoint aggregates for the overview page.
func (h *OverviewHandler) GetEndpointMetrics(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	serviceName := c.Query("serviceName")

	rows, err := h.Service.GetEndpointMetrics(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview endpoint metrics")
		return
	}

	RespondOK(c, rows)
}

// GetEndpointTimeSeries returns endpoint-level time-series buckets for charts.
func (h *OverviewHandler) GetEndpointTimeSeries(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)
	serviceName := c.Query("serviceName")

	rows, err := h.Service.GetEndpointTimeSeries(teamUUID, startMs, endMs, serviceName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query overview endpoint time series")
		return
	}

	RespondOK(c, rows)
}
