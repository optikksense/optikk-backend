package connections

import (
	"net/http"

	"github.com/gin-gonic/gin"
	common "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
)

type Handler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *Handler) GetConnectionCountSeries(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionCountSeries(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection count series")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetConnectionUtilization(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionUtilization(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection utilization")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetConnectionLimits(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionLimits(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection limits")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetPendingRequests(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetPendingRequests(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query pending connection requests")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetConnectionTimeoutRate(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionTimeoutRate(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection timeout rate")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetConnectionWaitTime(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionWaitTime(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection wait time")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetConnectionCreateTime(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionCreateTime(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection create time")
		return
	}
	common.RespondOK(c, resp)
}

func (h *Handler) GetConnectionUseTime(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := common.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetConnectionUseTime(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		common.RespondError(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to query connection use time")
		return
	}
	common.RespondOK(c, resp)
}
