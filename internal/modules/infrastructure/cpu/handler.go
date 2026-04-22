package cpu

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type CPUHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *CPUHandler) GetAvgCPU(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetAvgCPU(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg CPU", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *CPUHandler) GetCPUByInstance(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetCPUByInstance(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query CPU by instance", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *CPUHandler) GetCPUTopHosts(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	limit := modulecommon.ParseIntParam(c, "limit", defaultTopHosts)
	if limit <= 0 || limit > maxTopHosts {
		limit = defaultTopHosts
	}
	resp, err := h.Service.GetCPUTopHosts(c.Request.Context(), teamID, startMs, endMs, limit)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query top CPU hosts", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
