package servicepage

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type ServiceHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *ServiceHandler) GetTotalServices(c *gin.Context) {
	h.respondWithCount(c, h.Service.GetTotalServices, "Failed to query total services")
}

func (h *ServiceHandler) GetHealthyServices(c *gin.Context) {
	h.respondWithCount(c, h.Service.GetHealthyServices, "Failed to query healthy services")
}

func (h *ServiceHandler) GetDegradedServices(c *gin.Context) {
	h.respondWithCount(c, h.Service.GetDegradedServices, "Failed to query degraded services")
}

func (h *ServiceHandler) GetUnhealthyServices(c *gin.Context) {
	h.respondWithCount(c, h.Service.GetUnhealthyServices, "Failed to query unhealthy services")
}

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
