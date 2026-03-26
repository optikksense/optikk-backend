package jvm

import (
	"net/http"

	"github.com/Optikk-Org/optikk-backend/internal/shared/contracts/errorcode"

	"github.com/Optikk-Org/optikk-backend/internal/infra/logger"
	modulecommon "github.com/Optikk-Org/optikk-backend/internal/shared/httputil"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type JVMHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *JVMHandler) GetJVMMemory(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMMemory(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query JVM memory", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *JVMHandler) GetJVMGCDuration(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMGCDuration(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query JVM GC duration", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *JVMHandler) GetJVMGCCollections(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMGCCollections(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query JVM GC collections", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *JVMHandler) GetJVMThreadCount(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMThreadCount(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query JVM thread count", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *JVMHandler) GetJVMClasses(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMClasses(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		logger.L().Error("jvm classes query failed", zap.Error(err))
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query JVM classes", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *JVMHandler) GetJVMCPU(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMCPU(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query JVM CPU", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}

func (h *JVMHandler) GetJVMBuffers(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := modulecommon.ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMBuffers(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		modulecommon.RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query JVM buffers", err)
		return
	}
	modulecommon.RespondOK(c, resp)
}
