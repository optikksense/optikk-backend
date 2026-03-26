package servicemap

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type ServiceMapHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *ServiceMapHandler) GetTopology(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	resp, err := h.Service.GetTopology(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query topology", err)
		return
	}

	modulecommon.RespondOK(c, resp)
}

func (h *ServiceMapHandler) GetUpstreamDownstream(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Param("serviceName")
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	deps, err := h.Service.GetUpstreamDownstream(teamID, serviceName, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query upstream/downstream dependencies", err)
		return
	}
	modulecommon.RespondOK(c, deps)
}

func (h *ServiceMapHandler) GetExternalDependencies(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}

	deps, err := h.Service.GetExternalDependencies(teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query external dependencies", err)
		return
	}
	modulecommon.RespondOK(c, deps)
}

func (h *ServiceMapHandler) GetClientServerLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	operationName := c.Query("operationName")
	if operationName == "" {
		operationName = c.Query("operation")
	}

	points, err := h.Service.GetClientServerLatency(teamID, startMs, endMs, operationName)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query client/server latency", err)
		return
	}
	modulecommon.RespondOK(c, points)
}
