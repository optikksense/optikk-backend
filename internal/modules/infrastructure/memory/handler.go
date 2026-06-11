package memory

import (
	"context"
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type MemoryHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *MemoryHandler) GetAvgMemory(c *gin.Context) {
	modulecommon.HandleRangeQuery(c, h.GetTenant, "Failed to query avg memory", func(ctx context.Context, teamID, startMs, endMs int64) (any, error) {
		return h.Service.GetAvgMemory(ctx, teamID, startMs, endMs)
	})
}

func (h *MemoryHandler) GetMemoryByInstance(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	host := c.Query("host")
	pod := c.Query("pod")
	container := c.Query("container")
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "serviceName is required")
		return
	}
	resp, err := h.Service.GetMemoryByInstance(c.Request.Context(), teamID, host, pod, container, serviceName, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query memory by instance", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
