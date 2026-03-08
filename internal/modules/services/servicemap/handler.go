package servicemap

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// ServiceMapHandler handles service map endpoints.
type ServiceMapHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetUpstreamDownstream returns all upstream and downstream dependencies for a service.
func (h *ServiceMapHandler) GetUpstreamDownstream(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	serviceName := c.Param("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	deps, err := h.Service.GetUpstreamDownstream(teamUUID, serviceName, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query upstream/downstream dependencies")
		return
	}
	RespondOK(c, deps)
}

// GetExternalDependencies returns calls to hosts outside the known service mesh.
func (h *ServiceMapHandler) GetExternalDependencies(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	deps, err := h.Service.GetExternalDependencies(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query external dependencies")
		return
	}
	RespondOK(c, deps)
}

// GetClientServerLatency returns dual time-series of client vs server p95 latency.
func (h *ServiceMapHandler) GetClientServerLatency(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	operationName := c.Query("operationName")

	points, err := h.Service.GetClientServerLatency(teamUUID, startMs, endMs, operationName)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query client/server latency")
		return
	}
	RespondOK(c, points)
}
