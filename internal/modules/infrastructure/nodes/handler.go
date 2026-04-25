package nodes

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type NodeHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *NodeHandler) GetInfrastructureNodes(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetInfrastructureNodes(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query node health", err)
		return
	}

	modulecommon.RespondOK(c, rows)
}

func (h *NodeHandler) GetInfrastructureNodeSummary(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	summary, err := h.Service.GetInfrastructureNodeSummary(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query node summary", err)
		return
	}

	modulecommon.RespondOK(c, summary)
}

func (h *NodeHandler) GetInfrastructureNodeServices(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	host := c.Param("host")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	rows, err := h.Service.GetInfrastructureNodeServices(c.Request.Context(), teamID, host, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query node services", err)
		return
	}

	modulecommon.RespondOK(c, rows)
}
