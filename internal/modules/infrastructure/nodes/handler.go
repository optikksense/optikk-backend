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
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetInfrastructureNodes(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query node health")
		return
	}

	RespondOK(c, rows)
}

// GetInfrastructureNodeSummary returns aggregate counts for node dashboard stat cards.
func (h *NodeHandler) GetInfrastructureNodeSummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	summary, err := h.Service.GetInfrastructureNodeSummary(teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query node summary")
		return
	}

	RespondOK(c, summary)
}

// GetInfrastructureNodeServices returns services running on a specific host.
func (h *NodeHandler) GetInfrastructureNodeServices(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	host := c.Param("host")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetInfrastructureNodeServices(teamID, host, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query node services")
		return
	}

	RespondOK(c, rows)
}
