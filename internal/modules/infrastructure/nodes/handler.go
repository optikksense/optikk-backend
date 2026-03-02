package nodes

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

// NodeHandler handles node page endpoints.
type NodeHandler struct {
	modulecommon.DBTenant
	Service Service
}

// GetInfrastructureNodes returns host-level aggregation for the nodes view.
func (h *NodeHandler) GetInfrastructureNodes(c *gin.Context) {
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
func (h *NodeHandler) GetInfrastructureNodeServices(c *gin.Context) {
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
