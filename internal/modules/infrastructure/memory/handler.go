package memory

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type MemoryHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *MemoryHandler) GetMemoryUsage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetMemoryUsage(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query memory usage")
		return
	}
	RespondOK(c, resp)
}

func (h *MemoryHandler) GetMemoryUsagePercentage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetMemoryUsagePercentage(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query memory usage percentage")
		return
	}
	RespondOK(c, resp)
}

func (h *MemoryHandler) GetSwapUsage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSwapUsage(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query swap usage")
		return
	}
	RespondOK(c, resp)
}
