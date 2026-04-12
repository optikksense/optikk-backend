package resourceutil //nolint:misspell

import (
	"log/slog"
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type ResourceUtilisationHandler struct {
	modulecommon.DBTenant
	Service Service
}

func (h *ResourceUtilisationHandler) GetAvgCPU(c *gin.Context) {
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

func (h *ResourceUtilisationHandler) GetAvgMemory(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetAvgMemory(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg Memory", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetAvgNetwork(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetAvgNetwork(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg Network", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetAvgConnPool(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetAvgConnPool(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg Conn Pool", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetCPUUsagePercentage(c *gin.Context) {
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

func (h *ResourceUtilisationHandler) GetMemoryUsagePercentage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetMemoryUsagePercentage(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query Memory usage percentage", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetByService(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetResourceUsageByService(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query resource usage by service", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *ResourceUtilisationHandler) GetByInstance(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetResourceUsageByInstance(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		slog.Error("resource usage by instance query failed", slog.Any("error", err))
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query resource usage by instance", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
