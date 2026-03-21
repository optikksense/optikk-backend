package servicemap

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type ServiceMapHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *ServiceMapHandler) GetUpstreamDownstream(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	serviceName := c.Param("serviceName")
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	deps, err := h.Service.GetUpstreamDownstream(teamID, serviceName, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query upstream/downstream dependencies", err)
		return
	}
	RespondOK(c, deps)
}

func (h *ServiceMapHandler) GetExternalDependencies(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}

	deps, err := h.Service.GetExternalDependencies(teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query external dependencies", err)
		return
	}
	RespondOK(c, deps)
}

func (h *ServiceMapHandler) GetClientServerLatency(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	operationName := c.Query("operationName")
	if operationName == "" {
		operationName = c.Query("operation")
	}

	points, err := h.Service.GetClientServerLatency(teamID, startMs, endMs, operationName)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query client/server latency", err)
		return
	}
	RespondOK(c, points)
}
