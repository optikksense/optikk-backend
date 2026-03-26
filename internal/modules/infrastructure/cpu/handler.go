package cpu

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type CPUHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *CPUHandler) GetCPUTime(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCPUTime(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query CPU time", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *CPUHandler) GetCPUUsagePercentage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCPUUsagePercentage(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query CPU usage percentage", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *CPUHandler) GetLoadAverage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetLoadAverage(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query load average", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *CPUHandler) GetProcessCount(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetProcessCount(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query process count", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
