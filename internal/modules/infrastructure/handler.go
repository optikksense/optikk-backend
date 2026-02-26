package infrastructure

import (
	"net/http"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/modules/infrastructure/service"
	. "github.com/observability/observability-backend-go/internal/platform/handlers"
)

// InfrastructureHandler handles infrastructure page endpoints.
type InfrastructureHandler struct {
	modulecommon.DBTenant
	Service service.Service
}

// GetInfrastructure returns host/pod/container level resource summary.
func (h *InfrastructureHandler) GetInfrastructure(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Service.GetInfrastructure(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query infrastructure metrics")
		return
	}

	RespondOK(c, rows)
}

// GetInfrastructureNodes returns host-level aggregation for the nodes view.
func (h *InfrastructureHandler) GetInfrastructureNodes(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Service.GetInfrastructureNodes(teamUUID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query node health")
		return
	}

	RespondOK(c, rows)
}

// GetInfrastructureNodeServices returns services running on a specific host.
func (h *InfrastructureHandler) GetInfrastructureNodeServices(c *gin.Context) {
	teamUUID := h.GetTenant(c).TeamUUID()
	host := c.Param("host")
	startMs, endMs := ParseRange(c, 60*60*1000)

	rows, err := h.Service.GetInfrastructureNodeServices(teamUUID, host, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query node services")
		return
	}

	RespondOK(c, rows)
}
