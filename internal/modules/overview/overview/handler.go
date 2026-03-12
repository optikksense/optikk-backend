package overview

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type OverviewHandler struct {
	modulecommon.DBTenant
	Service Service
}

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
