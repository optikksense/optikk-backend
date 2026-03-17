package cpu

import (
	"net/http"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type CPUHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *CPUHandler) GetCPUTime(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCPUTime(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query CPU time")
		return
	}
	RespondOK(c, resp)
}

func (h *CPUHandler) GetCPUUsagePercentage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCPUUsagePercentage(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query CPU usage percentage")
		return
	}
	RespondOK(c, resp)
}

func (h *CPUHandler) GetLoadAverage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLoadAverage(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query load average")
		return
	}
	RespondOK(c, resp)
}

func (h *CPUHandler) GetProcessCount(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetProcessCount(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query process count")
		return
	}
	RespondOK(c, resp)
}
