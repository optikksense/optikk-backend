package jvm

import (
	"net/http"

	"github.com/observability/observability-backend-go/internal/contracts/errorcode"

	"github.com/gin-gonic/gin"
	. "github.com/observability/observability-backend-go/internal/modules/common"
	modulecommon "github.com/observability/observability-backend-go/internal/modules/common"
	"github.com/observability/observability-backend-go/internal/platform/logger"
	"go.uber.org/zap"
)

type JVMHandler struct {
	modulecommon.DBTenant
	Service *Service
}

func (h *JVMHandler) GetJVMMemory(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMMemory(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query JVM memory", err)
		return
	}
	RespondOK(c, resp)
}

func (h *JVMHandler) GetJVMGCDuration(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMGCDuration(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query JVM GC duration", err)
		return
	}
	RespondOK(c, resp)
}

func (h *JVMHandler) GetJVMGCCollections(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMGCCollections(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query JVM GC collections", err)
		return
	}
	RespondOK(c, resp)
}

func (h *JVMHandler) GetJVMThreadCount(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMThreadCount(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query JVM thread count", err)
		return
	}
	RespondOK(c, resp)
}

func (h *JVMHandler) GetJVMClasses(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMClasses(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		logger.L().Error("jvm classes query failed", zap.Error(err))
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query JVM classes", err)
		return
	}
	RespondOK(c, resp)
}

func (h *JVMHandler) GetJVMCPU(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMCPU(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query JVM CPU", err)
		return
	}
	RespondOK(c, resp)
}

func (h *JVMHandler) GetJVMBuffers(c *gin.Context) {
	teamID := h.GetTenant(c).TeamID
	startMs, endMs, ok := ParseRequiredRange(c)
	if !ok {
		return
	}
	resp, err := h.Service.GetJVMBuffers(c.Request.Context(), teamID, startMs, endMs)
	if err != nil {
		RespondErrorWithCause(c, http.StatusInternalServerError, errorcode.Internal, "Failed to query JVM buffers", err)
		return
	}
	RespondOK(c, resp)
}
