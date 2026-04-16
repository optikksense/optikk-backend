package memory

import (
	"net/http"

	errorcode "github.com/Optikk-Org/optikk-backend/internal/shared/contracts"

	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
)

type MemoryHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *MemoryHandler) GetMemoryUsage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetMemoryUsage(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query memory usage", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *MemoryHandler) GetMemoryUsagePercentage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetMemoryUsagePercentage(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query memory usage percentage", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *MemoryHandler) GetSwapUsage(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetSwapUsage(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query swap usage", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *MemoryHandler) GetAvgMemory(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetAvgMemory(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query avg memory", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *MemoryHandler) GetMemoryByService(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	serviceName := c.Query("serviceName")
	if serviceName == "" {
		modulecommon.RespondError(c, http.StatusBadRequest, errorcode.BadRequest, "serviceName is required")
		return
	}
	resp, err := h.Service.GetMemoryByService(c.Request.Context(), teamID, serviceName, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query memory by service", err)
		return
	}
	modulecommon.RespondOK(c, resp)
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
